---

# S-2000 (Scrapper-2000) - Discord Chat Ops Tool

S-2000 is a powerful, Go-based command-line tool designed to automate the process of exporting Discord chat history using [DiscordChatExporter (DCE)](https://github.com/Tyrrrz/DiscordChatExporter) and then scraping specific data from the generated JSON files. It features multiple modes of operation for flexible data extraction.

The tool supports chunking large channel exports by date ranges for parallel processing and aggregates all scraped data into clean, usable CSV files.

## Features

*   **Modular Subcommand System:**
    *   `run-all`: A complete pipeline to first run DCE to export JSON, and then scrape user/role data.
    *   `scrape-roles`: A dedicated mode to scrape user ID, username, display name, and role information from existing JSON files.
    *   `scrape-messages`: A dedicated mode to scrape just display names and message content from existing JSON files.
*   **DCE Orchestration:**
    *   Concurrently runs multiple instances of DiscordChatExporter CLI.
    *   Supports exporting entire channels or chunking exports by specified date ranges (manual or auto-generated).
    *   Manages a configurable number of concurrent DCE processes to respect system resources and Discord API rate limits.
*   **High-Performance Scraping:**
    *   Efficiently streams and parses potentially very large JSON files.
    *   Leverages Go's concurrency for both DCE execution and JSON parsing.
*   **Configuration Driven:** Uses a central `config.yaml` file for all settings.
*   **Organized Output:** Creates intermediate JSON files in structured directories and produces separate, clearly-named final CSV reports for different data types.

## Prerequisites

*   [Go](https://golang.org/dl/) (version 1.18 or higher recommended).
*   [DiscordChatExporter (DCE) CLI](https://github.com/Tyrrrz/DiscordChatExporter/releases) executable accessible by the S-2000 program.

## Installation & Setup

1.  **Download or Clone S-2000:**
    ```bash
    # If you have it in a git repo:
    # git clone <your-s2000-repo-url>
    # cd <s2000-repo-directory>

    # Otherwise, just ensure s2000.go (or main.go) is in your current directory
    ```

2.  **Install DiscordChatExporter CLI:**
    *   Download the appropriate release for your OS from the [DCE releases page](https://github.com/Tyrrrz/DiscordChatExporter/releases).
    *   Make sure the DCE executable is either in your system's PATH or you provide its full path in the `config.yaml` (`dce_execpath`).

3.  **Build S-2000 Executable (Recommended):**
    ```bash
    go build -o s2000 .
    ```
    This creates an executable named `s2000` (or `s2000.exe` on Windows).

4.  **Prepare `config.yaml`:**
    *   Create a `config.yaml` file in the same directory as your S-2000 executable (or specify its path using the `-config` flag).
    *   See the "Configuration" section below for details on its structure.

## Configuration (`config.yaml`)

All settings are managed via `config.yaml`. Here's an example:

```yaml
config:
  token: "YOUR_DISCORD_BOT_TOKEN"           # REQUIRED: Your Discord Bot Token
  dce_execpath: "DiscordChatExporter.Cli"   # REQUIRED: Path to DCE CLI executable
                                            # e.g., "./DiscordChatExporter.Cli" or "/usr/local/bin/DiscordChatExporter.Cli" (Linux/macOS)
                                            # e.g., "DiscordChatExporter.Cli.exe" or "C:\\DCE\\DiscordChatExporter.Cli.exe" (Windows)
  intermediate_export_directory: "./s2000_json_exports" # Directory for DCE's JSON output chunks

  # --- Output Paths for Different Modes ---
  # For 'run-all' and 'scrape-roles' modes
  final_csv_output_path: "./s2000_aggregated_user_role_data.csv"
  # For 'scrape-messages' mode
  messages_csv_output_path: "./s2000_scraped_messages.csv"

  dce_export_format: "Json"                 # Should always be "Json" for S-2000
  dce_max_concurrent: 4                     # Max number of DCE instances to run in parallel (e.g., 2-16)
  channels:
    - id: "YOUR_CHANNEL_ID_1"               # REQUIRED: Numerical ID of the Discord channel
      name: "my-large-channel"              # Optional: Friendly name for directory/file naming
      autochunk:                            # Optional: Settings for automatic date chunking
        startdate: "2019-05-23"             # REQUIRED if autochunk is used
        enddate: "2024-07-21"               # REQUIRED if autochunk is used - e.g., today's date
        chunkdurationmonths: 1              # REQUIRED if autochunk is used (e.g., 1, 3, 6)
```

**Key Configuration Fields:**

*   `token`: Your Discord bot token.
*   `dce_execpath`: Path to the DCE executable.
*   `intermediate_export_directory`: Folder where DCE will save JSON chunk files.
*   `final_csv_output_path`: Path for the CSV report from the `scrape-roles` mode.
*   `messages_csv_output_path`: Path for the CSV report from the `scrape-messages` mode.
*   `dce_max_concurrent`: The maximum number of DCE processes S-2000 will run at the same time.
*   `channels`: A list of channel configurations, each with an `id`, optional `name`, and optional `dateranges` or `autochunk` settings.

## Usage

S-2000 uses subcommands to determine its mode of operation. You must provide a command and a path to the config file.

**Syntax:**

```bash
./s2000 <command> -config <path/to/config.yaml>
```

---

### Commands

#### 1. `run-all`
This command executes the full pipeline: it runs DCE to export JSON files based on your configuration and then immediately scrapes them for user and role data.

```bash
# Run the full export and role scraping process
./s2000 run-all -config config.yaml
```

**Output:**
*   JSON files will be created in the `intermediate_export_directory`.
*   A CSV file will be created at the path specified by `final_csv_output_path`.

---

#### 2. `scrape-roles`
This command skips the DCE export phase and directly scrapes user/role data from any existing JSON files found in the `intermediate_export_directory`. Useful for re-running analysis without re-downloading.

```bash
# Scrape roles from already exported JSON files
./s2000 scrape-roles -config config.yaml
```

**Output:**
*   A CSV file will be created at the path specified by `final_csv_output_path`.

**CSV Format (Roles):**
*   `UserID`, `Username`, `DisplayName`, `RoleID`, `RoleName`

---

#### 3. `scrape-messages`
This command skips the DCE export phase and directly scrapes display names and message content from existing JSON files in the `intermediate_export_directory`.

```bash
# Scrape messages from already exported JSON files
./s2000 scrape-messages -config config.yaml
```

**Output:**
*   A CSV file will be created at the path specified by `messages_csv_output_path`.

**CSV Format (Messages):**
*   `DisplayName`, `MessageContent`

---

## Troubleshooting & Notes

*   **Discord Rate Limits:** DCE respects Discord's API rate limits. High concurrency or large exports will be slowed down by Discord. Monitor the console output for rate limit messages.
*   **Permissions:** Ensure the bot token used has `View Channel` and `Read Message History` permissions for all configured channels.
*   **Paths:** Double-check all paths in your `config.yaml` (`dce_execpath`, output directories).
*   **Disk Space:** Exporting large channels can consume significant disk space.

## Development

S-2000 is written in Go. Key components:
*   `flag` package for subcommand routing.
*   `gopkg.in/yaml.v2` for configuration.
*   `os/exec` for running DiscordChatExporter CLI.
*   Goroutines and semaphores for managing concurrency.
*   `encoding/json.NewDecoder` for memory-efficient JSON streaming.

## License

```
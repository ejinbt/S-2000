

# S-2000 (Scrapper-2000) - Discord Chat Operations Tool

S-2000 is a powerful, Go-based command-line tool designed to fully automate the process of exporting and analyzing Discord chat history using [DiscordChatExporter (DCE)](https://github.com/Tyrrrz/DiscordChatExporter).

It features multiple modes of operation, from simple message scraping to a comprehensive, server-wide analysis that gathers message content, reactions, and detailed sender metadata (including account creation and server join dates).

## Features

*   **Modular Subcommand System:**
    *   `scrape-extended`: A fully automated, one-command solution to scrape all text channels in a server for a recent period, generating a detailed report.
    *   `run-all`: A pipeline to export specific channels (chunked by date) and then scrape aggregated user/role data.
    *   `scrape-roles`: A dedicated mode to scrape user ID, display names, and a complete list of all their roles from existing JSON files.
    *   `scrape-messages`: A simple mode to scrape just display names and message content from existing JSON files.
*   **Automated Data Gathering:** The `scrape-extended` mode automatically discovers all server channels, fetches member data for join dates, and orchestrates the entire export process.
*   **High-Performance Operations:**
    *   Concurrently runs multiple instances of DiscordChatExporter to speed up exports.
    *   Leverages Go's concurrency for ultra-fast scraping of the generated JSON files.
    *   Uses efficient JSON streaming to handle very large files without consuming excessive memory.
*   **Rich Data Extraction:** Capable of calculating user account creation dates from their ID and correlating messages with server join dates.
*   **Configuration Driven:** A single `config.yaml` file controls all settings for all modes.
*   **Organized Output:** Creates intermediate JSON files in structured directories and produces clean, clearly-named final CSV reports.

## Prerequisites

*   [Go](https://golang.org/dl/) (version 1.18 or higher recommended).
*   [DiscordChatExporter (DCE) CLI](https://github.com/Tyrrrz/DiscordChatExporter/releases) executable accessible by the S-2000 program.

## Installation & Setup

1.  **Download S-2000:**
    *   Clone or download the source code into a directory.

2.  **Install DiscordChatExporter CLI:**
    *   Download the appropriate release for your OS from the [DCE releases page](https://github.com/Tyrrrz/DiscordChatExporter/releases).
    *   Place the executable where S-2000 can find it (e.g., in the same directory, or a location in your system's PATH).

3.  **Build S-2000 Executable:**
    *   Open a terminal in the S-2000 source directory.
    *   Run: `go build -o s2000 .`
    *   This creates an executable named `s2000` (or `s2000.exe` on Windows).

4.  **Prepare `config.yaml`:**
    *   Create a `config.yaml` file in the same directory as your S-2000 executable.
    *   See the "Configuration" section below for details.

## Configuration (`config.yaml`)

All settings are managed via `config.yaml`.

```yaml
config:
  token: "YOUR_DISCORD_BOT_TOKEN"           # REQUIRED: Your Discord Bot Token
  dce_execpath: "DiscordChatExporter.Cli"   # REQUIRED: Path to DCE CLI executable
  
  # --- Settings for 'scrape-extended' Mode ---
  server_id_to_export: "YOUR_SERVER_ID_HERE"  # REQUIRED for scrape-extended
  export_duration_months: 3                   # How many months of history to fetch

  # --- General Settings ---
  intermediate_export_directory: "./s2000_exports" # Directory for all intermediate JSON files
  dce_export_format: "Json"
  dce_max_concurrent: 8                       # Max parallel DCE processes (adjust based on CPU)

  # --- Output Paths for Different Modes ---
  # For 'run-all' and 'scrape-roles'
  final_csv_output_path: "./s2000_user_roles.csv"
  # For 'scrape-messages'
  messages_csv_output_path: "./s2000_messages.csv"
  # For 'scrape-extended'
  extended_scrape_csv_output_path: "./s2000_extended_report.csv"

  # --- Settings for 'run-all' and 'scrape-roles' Modes ---
  # This section is used if you are NOT running scrape-extended
  channels:
    - id: "MANUAL_CHANNEL_ID_1"
      name: "manual-export-channel"
      autochunk:
        startdate: "2023-01-01"
        enddate: "2023-12-31"
        chunkdurationmonths: 1
```

## Usage

S-2000 uses subcommands to determine its mode of operation.

**Syntax:**
`./s2000 <command> -config <path/to/config.yaml>`

---

### **Primary Command: `scrape-extended`**
This is the most powerful, fully automated command. It discovers all channels in a server, fetches member data, exports recent messages, and generates a detailed report.

```bash
# Run the full, automated server analysis
./s2000 scrape-extended -config config.yaml
```

**Output:**
*   A CSV file at the path specified by `extended_scrape_csv_output_path`.

**CSV Format (`scrape-extended`):**
*   `MessageID`, `TimestampUTC`, `AuthorID`, `AuthorName`, `DisplayName`, `AccountCreationDateUTC`, `ServerJoinDateUTC`, `MessageContent`, `Reactions` (e.g., `emoji1:count|emoji2:count`)

---

### Other Commands

#### `run-all`
Runs the DCE export pipeline for the channels manually specified in the `channels` section of the config, then scrapes them for aggregated user roles.

```bash
./s2000 run-all -config config.yaml
```

#### `scrape-roles`
Skips DCE exporting. Scrapes aggregated user and role data from existing JSON files in the `intermediate_export_directory`.

```bash
./s2000 scrape-roles -config config.yaml
```
**CSV Format (`scrape-roles`):**
*   `UserID`, `Username`, `DisplayName`, `RoleIDs` (e.g., `id1|id2`), `RoleNames` (e.g., `Role1|Role2`)

#### `scrape-messages`
Skips DCE exporting. Scrapes display names and message content from existing JSON files.

```bash
./s2000 scrape-messages -config config.yaml
```
**CSV Format (`scrape-messages`):**
*   `DisplayName`, `MessageContent`

---

## Troubleshooting & Notes

*   **Discord Rate Limits:** DCE respects Discord's API rate limits. High concurrency or large exports will be slowed down by Discord. Monitor the console output for rate limit messages.
*   **Permissions:** Ensure the bot token used has the `Server Members Intent` enabled in the Discord Developer Portal, as well as `View Channel` and `Read Message History` permissions for all channels.
*   **Paths:** Double-check all paths in your `config.yaml`, especially `dce_execpath`.
*   **Disk Space:** Exporting a full server, even for 3 months, can consume significant disk space.



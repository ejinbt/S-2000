# S-2000 (Scrapper-2000) - Discord Chat Operations Tool

S-2000 is a powerful, Go-based command-line tool designed to automate the process of exporting and analyzing Discord chat history using [DiscordChatExporter (DCE)](https://github.com/Tyrrrz/DiscordChatExporter).

It features multiple modes of operation, from simple message scraping to comprehensive, server-wide analysis and targeted user-activity lookups.

## Features

*   **Modular Subcommand System:**
    *   `scrape-extended`: Fully automates a server-wide scrape of recent messages for a detailed report.
    *   `find-first-message`: For a given list of users, exports the full history of specified channels to find their absolute first message.
    *   `run-all`: Exports specific channels (chunked by date) and scrapes aggregated user/role data.
    *   `scrape-roles`: Scrapes user IDs, display names, and a complete list of all their roles from existing JSON files.
    *   `scrape-messages`: Scrapes display names and message content from existing JSON files.
*   **Automated & Targeted Exporting:** Can export entire servers, specific channels with date filters, or the full history of channels for deep analysis.
*   **High-Performance Operations:** Leverages Go's concurrency for both DCE execution and ultra-fast JSON scraping, complete with progress bars for all long-running tasks.
*   **Rich Data Extraction:** Can calculate user account creation dates and find a user's first-ever message in a given set of channels.
*   **Configuration Driven:** A single `config.yaml` file controls all settings for all modes.

## Prerequisites

*   [Go](https://golang.org/dl/) (version 1.18 or higher recommended).
*   [DiscordChatExporter (DCE) CLI](https://github.com/Tyrrrz/DiscordChatExporter/releases) (Latest version recommended).

## Installation & Setup

1.  **Download S-2000:** Clone or download the source code.
2.  **Install DCE CLI:** Download the release for your OS and place the executable where S-2000 can find it (e.g., in the same directory, or a location in your system's PATH).
3.  **Build S-2000:** Open a terminal in the S-2000 source directory and run `go build -o s2000 .` (or `s2000.exe` on Windows).
4.  **Prepare `config.yaml`:** Create a `config.yaml` file. See the configuration section below.

## Configuration (`config.yaml`)

All settings are managed via `config.yaml`.

```yaml
# S-2000 Configuration File

config:
  token: "YOUR_DISCORD_TOKEN_HERE"          # REQUIRED: Your Discord User or Bot Token
  dce_execpath: "DiscordChatExporter.Cli"   # REQUIRED: Path to DCE CLI executable

  # --- Settings for 'scrape-extended' Mode ---
  server_id_to_export: "YOUR_SERVER_ID_HERE"
  export_duration_months: 3

  # --- Settings for 'find-first-message' Mode ---
  # CSV file with UserIDs in the first column (must have a header)
  input_user_csv_path: "./input_users.csv"
  # Output file for the first message report
  first_message_output_path: "./s2000_first_messages_report.csv"

  # --- General Settings ---
  intermediate_export_directory: "./s2000_exports"
  dce_max_concurrent: 8

  # --- Output Paths for Other Modes ---
  final_csv_output_path: "./s2000_user_roles.csv"
  messages_csv_output_path: "./s2000_messages.csv"
  extended_scrape_csv_output_path: "./s2000_extended_report.csv"
  
  # --- Channels for 'run-all' and 'find-first-message' Modes ---
  # For 'find-first-message', list ALL channels you want to search through for the true first message.
  # For 'run-all', this list is used for targeted, date-chunked exports.
  channels:
    - id: "111111111111111111"
      name: "general"
    - id: "222222222222222222"
      name: "introductions"
```

## Usage

**Syntax:** `./s2000 <command> -config <path/to/config.yaml>`

---

### **Primary Commands**

#### `find-first-message`
For a list of users (provided in a CSV), this command exports the **entire history** of the channels specified in the `channels` list and finds the absolute earliest message for each user.

1.  Create a CSV file specified in `input_user_csv_path` with a `UserID` column.
2.  List all channels to search in the `channels` section of the config.
3.  Run the command:
    ```bash
    ./s2000 find-first-message -config config.yaml
    ```
**Output:** A detailed CSV report about each user's first message at `first_message_output_path`.

---

#### `scrape-extended`
Performs a fully automated scrape of an entire server for recent messages.

1.  Set `server_id_to_export` and `export_duration_months` in the config.
2.  Run the command:
    ```bash
    ./s2000 scrape-extended -config config.yaml
    ```
**Output:** A detailed CSV report of user activity over the specified period at `extended_scrape_csv_output_path`.

---

### **Other Commands**

*   **`run-all`**: Exports specific channels from the `channels` list (supports date-chunking) and then runs the role scraper.
*   **`scrape-roles`**: Scrapes aggregated user and role data from existing JSON files in the `intermediate_export_directory`.
*   **`scrape-messages`**: Scrapes just display names and message content from existing JSON files.

## Troubleshooting & Notes

*   **Discord Rate Limits:** DCE respects Discord's API rate limits. Exporting large amounts of data will take time.
*   **User Tokens:** When using a user token (not a bot), you are acting as your own user. Be mindful of Discord's ToS.
*   **Permissions:** Ensure you have permission to view the channels you are trying to export.

## License

RAPIDFREELANCIN COPYRIGHTS

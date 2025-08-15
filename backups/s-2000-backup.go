package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort" // Import for sorting roles for consistent output
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/schollz/progressbar/v3"
	"gopkg.in/yaml.v2"
)

const discordEpoch = 1420070400000

// --- Configuration Structures (Unchanged) ---
type DateRangeStr string
type AutoChunkConfig struct {
	StartDate           string `yaml:"startdate"`
	EndDate             string `yaml:"enddate"`
	ChunkDurationMonths int    `yaml:"chunkdurationmonths"`
}
type ChannelConfig struct {
	ID         string           `yaml:"id"`
	Name       string           `yaml:"name"`
	DateRanges []DateRangeStr   `yaml:"dateranges"`
	AutoChunk  *AutoChunkConfig `yaml:"autochunk"`
}
type S2000AppConfig struct {
	Token                       string          `yaml:"token"`
	DceExecPath                 string          `yaml:"dce_execpath"`
	IntermediateExportDirectory string          `yaml:"intermediate_export_directory"`
	FinalCsvOutputPath          string          `yaml:"final_csv_output_path"`
	MessagesCsvOutputPath       string          `yaml:"messages_csv_output_path"`
	DceExportFormat             string          `yaml:"dce_export_format"`
	DceMaxConcurrent            int             `yaml:"dce_max_concurrent"`
	Channels                    []ChannelConfig `yaml:"channels"`
}
type RootConfig struct {
	Config S2000AppConfig `yaml:"config"`
}

// For parsing channels.json
type DiscoveredChannel struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// For parsing members.json
type MemberInfo struct {
	User struct {
		ID string `json:"id"`
	} `json:"user"`
	JoinedAt time.Time `json:"joined_at"`
}

// --- JSON Scraping Structures (Updated for Role Aggregation) ---
type ScraperMessageRoles struct {
	Author ScraperAuthorRoles `json:"author"`
}
type ScraperAuthorRoles struct {
	ID       string        `json:"id"`
	Name     string        `json:"name"`
	Nickname string        `json:"nickname"`
	Roles    []ScraperRole `json:"roles"`
}
type ScraperRole struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// NEW data structure for aggregating roles per user
type AggregatedUserEntry struct {
	UserID      string
	Username    string
	DisplayName string
	// Use a map for roles to automatically handle duplicates: map[RoleID]RoleName
	Roles map[string]string
}

// For message content scraping
type ScraperMessageContent struct {
	Content string               `json:"content"`
	Author  ScraperAuthorContent `json:"author"`
}
type ScraperAuthorContent struct {
	Name     string `json:"name"`
	Nickname string `json:"nickname"`
}

// getCreationTimeFromID calculates the creation time of a Discord entity from its Snowflake ID.
func getCreationTimeFromID(id string) (time.Time, error) {
	idInt, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	timestamp := (idInt >> 22) + discordEpoch
	return time.Unix(0, timestamp*int64(time.Millisecond)), nil
}

// --- Helper Functions (Unchanged) ---
func sanitizeFilename(name string) string {
	replacer := strings.NewReplacer(" ", "_", "/", "_", "\\", "_", ":", "_", "*", "_", "?", "_", "\"", "_", "<", "_", ">", "_", "|", "_")
	return replacer.Replace(name)
}

func limitString(s string, length int) string {
	if len(s) <= length {
		return s
	}
	return s[:length] + "..."
}

// --- Phase 1: DCE Orchestration (Unchanged) ---
// ... (generateDateRanges, CommandAndFileOutput, createDceCommands, runDceCommand, executeDcePhase are unchanged) ...
// NOTE: I am including them in this complete file for copy-paste convenience.

func generateDateRanges(autoChunk AutoChunkConfig) ([]DateRangeStr, error) {
	var ranges []DateRangeStr
	layout := "2006-01-02"
	startDate, err := time.Parse(layout, autoChunk.StartDate)
	if err != nil {
		return nil, fmt.Errorf("error parsing autochunk start date '%s': %v", autoChunk.StartDate, err)
	}
	endDate, err := time.Parse(layout, autoChunk.EndDate)
	if err != nil {
		return nil, fmt.Errorf("error parsing autochunk end date '%s': %v", autoChunk.EndDate, err)
	}
	if startDate.After(endDate) {
		return nil, fmt.Errorf("autochunk start date '%s' is after end date '%s'", autoChunk.StartDate, autoChunk.EndDate)
	}
	currentStartDate := startDate
	for !currentStartDate.After(endDate) {
		currentEndDate := currentStartDate.AddDate(0, autoChunk.ChunkDurationMonths, -1)
		if currentEndDate.After(endDate) {
			currentEndDate = endDate
		}
		ranges = append(ranges, DateRangeStr(fmt.Sprintf("%s;%s", currentStartDate.Format(layout), currentEndDate.Format(layout))))
		currentStartDate = currentEndDate.AddDate(0, 0, 1)
	}
	return ranges, nil
}

type CommandAndFileOutput struct {
	CommandArgs    []string
	OutputJSONPath string
}

func createDceCommands(appConfig S2000AppConfig) ([]CommandAndFileOutput, error) {
	var commandsAndOutputs []CommandAndFileOutput
	err := os.MkdirAll(appConfig.IntermediateExportDirectory, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("error creating intermediate export directory '%s': %v", appConfig.IntermediateExportDirectory, err)
	}
	for _, channel := range appConfig.Channels {
		channelSubDir := channel.ID
		if channel.Name != "" {
			channelSubDir = sanitizeFilename(channel.Name) + "_" + channel.ID
		}
		channelOutputDir := filepath.Join(appConfig.IntermediateExportDirectory, channelSubDir)
		_ = os.MkdirAll(channelOutputDir, os.ModePerm)
		var effectiveDateRanges []DateRangeStr
		if channel.AutoChunk != nil {
			generatedRanges, errGen := generateDateRanges(*channel.AutoChunk)
			if errGen != nil {
				log.Printf("Error generating date ranges for channel %s (%s): %v. Skipping autochunk.", channel.ID, channel.Name, errGen)
			} else {
				effectiveDateRanges = generatedRanges
			}
		} else {
			effectiveDateRanges = channel.DateRanges
		}
		exportFormat := appConfig.DceExportFormat
		if len(effectiveDateRanges) == 0 {
			filenameSuffix := fmt.Sprintf("%s_all.%s", channel.ID, strings.ToLower(exportFormat))
			if channel.Name != "" {
				filenameSuffix = fmt.Sprintf("%s_%s_all.%s", sanitizeFilename(channel.Name), channel.ID, strings.ToLower(exportFormat))
			}
			outputJSONPath := filepath.Join(channelOutputDir, filenameSuffix)
			args := []string{appConfig.DceExecPath, "export", "-t", appConfig.Token, "-c", channel.ID, "-f", exportFormat, "-o", outputJSONPath}
			commandsAndOutputs = append(commandsAndOutputs, CommandAndFileOutput{CommandArgs: args, OutputJSONPath: outputJSONPath})
		} else {
			for _, drStr := range effectiveDateRanges {
				parts := strings.Split(string(drStr), ";")
				if len(parts) != 2 {
					log.Printf("Warning: Invalid date range '%s' for channel %s. Skipping.", drStr, channel.ID)
					continue
				}
				startDateStr, endDateStr := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
				filenameSuffix := fmt.Sprintf("%s_%s_from_%s_to_%s.%s", sanitizeFilename(channel.Name), channel.ID, strings.ReplaceAll(startDateStr, "-", ""), strings.ReplaceAll(endDateStr, "-", ""), strings.ToLower(exportFormat))
				outputJSONPath := filepath.Join(channelOutputDir, filenameSuffix)
				args := []string{appConfig.DceExecPath, "export", "-t", appConfig.Token, "-c", channel.ID, "-f", exportFormat, "--after", startDateStr, "--before", endDateStr, "-o", outputJSONPath}
				commandsAndOutputs = append(commandsAndOutputs, CommandAndFileOutput{CommandArgs: args, OutputJSONPath: outputJSONPath})
			}
		}
	}
	return commandsAndOutputs, nil
}

func createDynamicDceCommands(appConfig S2000AppConfig, discoveredChannels []DiscoveredChannel) []CommandAndFileOutput {
	var commandsAndOutputs []CommandAndFileOutput

	// Calculate the "--after" date based on the config
	afterDate := time.Now().AddDate(0, -appConfig.ExportDurationMonths, 0)
	afterDateStr := afterDate.Format("2006-01-02")

	for _, channel := range discoveredChannels {
		// IMPORTANT: We only want to scrape text-based channels.
		// Adjust this list if needed based on DCE's output for channel types.
		switch channel.Type {
		case "GuildTextChat", "GuildAnnouncement", "GuildForum":
			// It's a text channel, so we'll export it.
		default:
			// It's a voice channel, category, etc. Skip it.
			log.Printf("Skipping non-text channel: %s (Type: %s)", channel.Name, channel.Type)
			continue
		}

		channelSubDir := sanitizeFilename(channel.Name) + "_" + channel.ID
		channelOutputDir := filepath.Join(appConfig.IntermediateExportDirectory, channelSubDir)
		_ = os.MkdirAll(channelOutputDir, os.ModePerm)

		outputJSONPath := filepath.Join(channelOutputDir, fmt.Sprintf("%s_last_%d_months.json", channel.ID, appConfig.ExportDurationMonths))

		args := []string{
			appConfig.DceExecPath, "export", "-t", appConfig.Token,
			"-c", channel.ID, "-f", "Json",
			"--after", afterDateStr,
			"-o", outputJSONPath,
		}
		commandsAndOutputs = append(commandsAndOutputs, CommandAndFileOutput{CommandArgs: args, OutputJSONPath: outputJSONPath})
	}
	return commandsAndOutputs
}
func runDceCommand(cmdAndOutput CommandAndFileOutput, wg *sync.WaitGroup, sem chan struct{}, bar *progressbar.ProgressBar) {
	defer wg.Done()
	defer bar.Add(1)
	sem <- struct{}{}
	defer func() { <-sem }()
	executable := cmdAndOutput.CommandArgs[0]
	argsOnly := cmdAndOutput.CommandArgs[1:]
	cmd := exec.Command(executable, argsOnly...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("\nDCE ERROR: %s %s\nError: %v\nOutput (last 500 chars):\n%s\n", executable, strings.Join(argsOnly, " "), err, limitString(string(output), 500))
	}
}
func executeDcePhase(dceCommands []CommandAndFileOutput, maxConcurrent int) {
	if len(dceCommands) == 0 {
		log.Println("No DCE export tasks to run.")
		return
	}
	bar := progressbar.NewOptions(len(dceCommands),
		progressbar.OptionSetDescription("Exporting Chat Logs (DCE)"),
		progressbar.OptionSetTheme(progressbar.Theme{Saucer: "[green]=[reset]", SaucerHead: "[green]>[reset]", BarStart: "[", BarEnd: "]"}),
		progressbar.OptionShowCount(),
		progressbar.OptionOnCompletion(func() { fmt.Fprint(os.Stderr, "\n") }),
	)
	var wgDce sync.WaitGroup
	semDce := make(chan struct{}, maxConcurrent)
	log.Printf("Launching %d DCE export tasks (Concurrency: %d)...", len(dceCommands), maxConcurrent)
	for _, cmdAndOut := range dceCommands {
		wgDce.Add(1)
		go runDceCommand(cmdAndOut, &wgDce, semDce, bar)
	}
	wgDce.Wait()
	log.Println("--- All message export tasks completed. ---")
}

// --- Phase 2: JSON Scraping Functions (Updated Logic for Role Aggregation) ---
func processJSONFileForAggregatedRoles(filePath string, aggregatedUsers map[string]*AggregatedUserEntry, muData *sync.Mutex, wg *sync.WaitGroup, sem chan struct{}, bar *progressbar.ProgressBar) {
	defer wg.Done()
	defer bar.Add(1)
	sem <- struct{}{}
	defer func() { <-sem }()

	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("\nRoleScrape ERROR opening file %s: %v", filePath, err)
		return
	}
	defer file.Close()

	decoder := json.NewDecoder(bufio.NewReader(file))
	_, _ = decoder.Token() // '{'
	for decoder.More() {
		token, _ := decoder.Token()
		key, ok := token.(string)
		if !ok || key != "messages" {
			var dummy interface{}
			_ = decoder.Decode(&dummy)
			continue
		}
		_, _ = decoder.Token() // '['
		for decoder.More() {
			var msg ScraperMessageRoles
			if err := decoder.Decode(&msg); err != nil {
				var skipDummy interface{}
				_ = decoder.Decode(&skipDummy)
				continue
			}
			if msg.Author.ID == "" {
				continue
			}

			// --- NEW AGGREGATION LOGIC ---
			muData.Lock()
			// Check if we have seen this user before
			entry, exists := aggregatedUsers[msg.Author.ID]
			if !exists {
				// First time seeing this user, create a new entry
				displayName := msg.Author.Nickname
				if displayName == "" {
					displayName = msg.Author.Name
				}
				entry = &AggregatedUserEntry{
					UserID:      msg.Author.ID,
					Username:    msg.Author.Name,
					DisplayName: displayName,
					Roles:       make(map[string]string), // Initialize the roles map
				}
				aggregatedUsers[msg.Author.ID] = entry
			}

			// Add all roles from this message to the user's role map.
			// Duplicates will be handled automatically by the map.
			for _, role := range msg.Author.Roles {
				if role.ID != "" { // Only add roles that have an ID
					entry.Roles[role.ID] = role.Name
				}
			}
			muData.Unlock()
			// --- END NEW AGGREGATION LOGIC ---
		}
		break
	}
}
func processJSONFileForExtendedScrape(filePath string, writer *csv.Writer, muWriter *sync.Mutex, memberJoinDateMap map[string]time.Time, wg *sync.WaitGroup, sem chan struct{}, bar *progressbar.ProgressBar) {
	defer wg.Done()
	defer bar.Add(1)
	sem <- struct{}{}
	defer func() { <-sem }()

	file, err := os.Open(filePath)
	if err != nil {
		return
	}
	defer file.Close()

	decoder := json.NewDecoder(bufio.NewReader(file))
	_, _ = decoder.Token() // '{'
	for decoder.More() {
		token, _ := decoder.Token()
		key, ok := token.(string)
		if !ok || key != "messages" {
			var dummy interface{}
			_ = decoder.Decode(&dummy)
			continue
		}

		_, _ = decoder.Token() // '['
		for decoder.More() {
			var msg ScraperMessageExtended
			if err := decoder.Decode(&msg); err != nil {
				var skipDummy interface{}
				_ = decoder.Decode(&skipDummy)
				continue
			}

			if msg.Author.ID == "" {
				continue
			}

			// --- Data Calculation and Lookup ---
			accountCreationDate, err := getCreationTimeFromID(msg.Author.ID)
			if err != nil {
				// Handle error if ID is invalid, though unlikely
				accountCreationDate = time.Time{} // Zero time
			}

			serverJoinDate := memberJoinDateMap[msg.Author.ID] // Will be zero time if not found

			displayName := msg.Author.Nickname
			if displayName == "" {
				displayName = msg.Author.Name
			}

			var reactionParts []string
			for _, reaction := range msg.Reactions {
				reactionParts = append(reactionParts, fmt.Sprintf("%s:%d", reaction.Emoji.Name, reaction.Count))
			}
			reactionsStr := strings.Join(reactionParts, "|")

			// Prepare CSV record
			record := []string{
				msg.ID,
				msg.Timestamp.UTC().Format(time.RFC3339),
				msg.Author.ID,
				msg.Author.Name,
				displayName,
				accountCreationDate.Format(time.RFC3339),
				serverJoinDate.Format(time.RFC3339),
				msg.Content,
				reactionsStr,
			}

			// Write record to CSV, protected by a mutex
			muWriter.Lock()
			_ = writer.Write(record)
			muWriter.Unlock()
		}
		break // Done with messages
	}
}
func runExtendedScrape(appConfig S2000AppConfig, startTime time.Time) {
	log.Println("--- Mode: Extended Server Scrape ---")

	// --- Phase 0: Discovery & Prerequisite Gathering ---
	log.Println("--- Phase 0: Discovering members and channels ---")
	err := os.MkdirAll(appConfig.IntermediateExportDirectory, os.ModePerm)
	if err != nil {
		log.Fatalf("FATAL: Could not create intermediate directory: %v", err)
	}

	membersJSONPath := filepath.Join(appConfig.IntermediateExportDirectory, "members.json")
	channelsJSONPath := filepath.Join(appConfig.IntermediateExportDirectory, "channels.json")

	// Step 0a: Get Server Members
	log.Println("Executing DCE to get server members...")
	membersCmd := exec.Command(appConfig.DceExecPath, "get-members", "-t", appConfig.Token, "-g", appConfig.ServerIdToExport, "-o", membersJSONPath)
	if output, err := membersCmd.CombinedOutput(); err != nil {
		log.Fatalf("FATAL: Failed to get server members with DCE: %v\nOutput:\n%s", err, string(output))
	}
	log.Printf("Successfully exported member data to %s", membersJSONPath)

	// Step 0b: Get Server Channels
	log.Println("Executing DCE to get server channels...")
	channelsCmd := exec.Command(appConfig.DceExecPath, "get-channels", "-t", appConfig.Token, "-g", appConfig.ServerIdToExport, "-o", channelsJSONPath)
	if output, err := channelsCmd.CombinedOutput(); err != nil {
		log.Fatalf("FATAL: Failed to get server channels with DCE: %v\nOutput:\n%s", err, string(output))
	}
	log.Printf("Successfully discovered channel data and saved to %s", channelsJSONPath)

	// --- Phase 1: Export Channel Messages ---
	log.Println("--- Phase 1: Orchestrating message exports for all discovered channels ---")
	channelsFile, err := os.ReadFile(channelsJSONPath)
	if err != nil {
		log.Fatalf("FATAL: Could not read discovered channels file: %v", err)
	}
	var discoveredChannels []DiscoveredChannel
	if err := json.Unmarshal(channelsFile, &discoveredChannels); err != nil {
		log.Fatalf("FATAL: Could not parse discovered channels file: %v", err)
	}

	dceCommands := createDynamicDceCommands(appConfig, discoveredChannels)

	// Execute the DCE export phase
	if len(dceCommands) > 0 {
		executeDcePhase(dceCommands, appConfig.DceMaxConcurrent)
	} else {
		log.Println("No text channels found to export.")
	}

	// --- Phase 2: Scrape & Aggregate Data ---
	log.Println("--- Phase 2: Starting extended scrape of all exported data ---")
	membersFile, err := os.ReadFile(membersJSONPath)
	if err != nil {
		log.Fatalf("FATAL: Could not read members data file: %v", err)
	}
	var memberList []MemberInfo
	if err := json.Unmarshal(membersFile, &memberList); err != nil {
		log.Fatalf("FATAL: Could not parse members data file: %v", err)
	}
	memberJoinDateMap := make(map[string]time.Time)
	for _, member := range memberList {
		memberJoinDateMap[member.User.ID] = member.JoinedAt.UTC() // Store as UTC
	}
	log.Printf("Loaded join dates for %d members.", len(memberJoinDateMap))

	// Find all the message JSON files we created (excluding members.json and channels.json)
	allJsonFiles, err := getJSONFilesFromExportDir(appConfig.IntermediateExportDirectory)
	if err != nil {
		log.Fatalf("FATAL: Could not read JSON files from export directory: %v", err)
	}

	var exportedMessageFiles []string
	for _, f := range allJsonFiles {
		base := filepath.Base(f)
		if base != "members.json" && base != "channels.json" {
			exportedMessageFiles = append(exportedMessageFiles, f)
		}
	}

	if len(exportedMessageFiles) == 0 {
		log.Println("No message JSON files found to scrape. Exiting.")
		log.Printf("S-2000 (scrape-extended) finished in %s.", time.Since(startTime))
		return
	}

	// Setup CSV writer
	csvFile, err := os.Create(appConfig.ExtendedScrapeCsvOutputPath)
	if err != nil {
		log.Fatalf("FATAL: Error creating extended scrape CSV file %s: %v", appConfig.ExtendedScrapeCsvOutputPath, err)
	}
	defer csvFile.Close()
	csvWriter := csv.NewWriter(bufio.NewWriter(csvFile))
	defer csvWriter.Flush()

	headers := []string{"MessageID", "TimestampUTC", "AuthorID", "AuthorName", "DisplayName", "AccountCreationDateUTC", "ServerJoinDateUTC", "MessageContent", "Reactions"}
	_ = csvWriter.Write(headers)

	// Setup progress bar and concurrency for scraping
	bar := progressbar.NewOptions(len(exportedMessageFiles),
		progressbar.OptionSetDescription("Scraping Extended Data  "),
		progressbar.OptionSetTheme(progressbar.Theme{Saucer: "[blue]=[reset]", SaucerHead: "[blue]>[reset]", BarStart: "[", BarEnd: "]"}),
		progressbar.OptionSetWidth(30),
		progressbar.OptionShowCount(),
		progressbar.OptionOnCompletion(func() { fmt.Fprint(os.Stderr, "\n") }),
	)

	var wg sync.WaitGroup
	var muWriter sync.Mutex
	scraperConcurrency := runtime.NumCPU() * 2
	sem := make(chan struct{}, scraperConcurrency)

	log.Printf("Scraping extended data from %d message files (Concurrency: %d)...", len(exportedMessageFiles), scraperConcurrency)
	for _, jsonPath := range exportedMessageFiles {
		wg.Add(1)
		go processJSONFileForExtendedScrape(jsonPath, csvWriter, &muWriter, memberJoinDateMap, &wg, sem, bar)
	}
	wg.Wait()

	if err := csvWriter.Error(); err != nil {
		log.Fatalf("FATAL: Error flushing CSV writer for extended scrape: %v", err)
	}
	log.Printf("Successfully wrote extended scrape data to %s", appConfig.ExtendedScrapeCsvOutputPath)
	log.Printf("S-2000 (scrape-extended) finished successfully in %s.", time.Since(startTime))
}

func scrapeMessagesFromJSON(filePath string, writer *csv.Writer, muWriter *sync.Mutex, wg *sync.WaitGroup, sem chan struct{}, bar *progressbar.ProgressBar) {
	defer wg.Done()
	defer bar.Add(1)
	sem <- struct{}{}
	defer func() { <-sem }()
	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("\nMessageScrape ERROR opening file %s: %v", filePath, err)
		return
	}
	defer file.Close()
	decoder := json.NewDecoder(bufio.NewReader(file))
	_, _ = decoder.Token() // '{'
	for decoder.More() {
		token, _ := decoder.Token()
		key, ok := token.(string)
		if !ok || key != "messages" {
			var dummy interface{}
			_ = decoder.Decode(&dummy)
			continue
		}
		_, _ = decoder.Token() // '['
		for decoder.More() {
			var msg ScraperMessageContent
			if err := decoder.Decode(&msg); err != nil {
				var skipDummy interface{}
				_ = decoder.Decode(&skipDummy)
				continue
			}
			if msg.Content == "" {
				continue
			}
			displayName := msg.Author.Nickname
			if displayName == "" {
				displayName = msg.Author.Name
			}
			muWriter.Lock()
			_ = writer.Write([]string{displayName, msg.Content})
			muWriter.Unlock()
		}
		break
	}
}

// --- S-2000 Main Program & Subcommands ---
func main() {
	startTime := time.Now()
	log.Println("S-2000 (Scrapper-2000) Initializing...")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s <command> -config <path/to/config.yaml>\n", os.Args[0])
		fmt.Fprintln(os.Stderr, "Commands:")
		fmt.Fprintln(os.Stderr, "  run-all         : Runs DCE export, then scrapes for user/role data.")
		fmt.Fprintln(os.Stderr, "  scrape-roles    : Scrapes user/role data from existing JSON files.")
		fmt.Fprintln(os.Stderr, "  scrape-messages : Scrapes usernames and messages from existing JSON files.")
	}
	if len(os.Args) < 2 {
		flag.Usage()
		os.Exit(1)
	}
	var configFile string
	switch os.Args[1] {
	case "run-all", "scrape-roles", "scrape-messages":
		cmdFlags := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
		cmdFlags.StringVar(&configFile, "config", "config.yaml", "Path to the configuration file.")
		_ = cmdFlags.Parse(os.Args[2:])
	default:
		log.Printf("Error: Unknown command '%s'", os.Args[1])
		flag.Usage()
		os.Exit(1)
	}
	appConfig, err := loadConfig(configFile)
	if err != nil {
		log.Fatal(err)
	}
	switch os.Args[1] {
	case "run-all":
		runFullOrchestrationAndScrape(appConfig, startTime)
	case "scrape-roles":
		runScrapeRolesOnly(appConfig, startTime)
	case "scrape-messages":
		runMessageScrapeOnly(appConfig, startTime)
	}
}

func loadConfig(configFile string) (S2000AppConfig, error) {
	log.Printf("Using config file: %s", configFile)
	data, err := os.ReadFile(configFile)
	if err != nil {
		return S2000AppConfig{}, fmt.Errorf("FATAL: Error reading config file '%s': %w", configFile, err)
	}
	var rootCfg RootConfig
	err = yaml.Unmarshal(data, &rootCfg)
	if err != nil {
		return S2000AppConfig{}, fmt.Errorf("FATAL: Error parsing YAML from '%s': %w", configFile, err)
	}
	appConfig := rootCfg.Config
	if appConfig.DceMaxConcurrent <= 0 {
		appConfig.DceMaxConcurrent = 4
	}
	if appConfig.DceExportFormat == "" || strings.ToLower(appConfig.DceExportFormat) != "json" {
		appConfig.DceExportFormat = "Json"
	}
	return appConfig, nil
}

func getJSONFilesFromExportDir(dir string) ([]string, error) {
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(strings.ToLower(info.Name()), ".json") {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

func runFullOrchestrationAndScrape(appConfig S2000AppConfig, startTime time.Time) {
	log.Println("--- Mode: Full Orchestration & Role Scrape ---")
	executeDcePhase(appConfig)
	jsonFiles, err := getJSONFilesFromExportDir(appConfig.IntermediateExportDirectory)
	if err != nil {
		log.Fatalf("FATAL: Could not read JSON files after export: %v", err)
	}
	if len(jsonFiles) == 0 {
		log.Println("No JSON files were exported. Cannot proceed with scraping.")
	} else {
		runScrapeRoles(appConfig, jsonFiles)
	}
	log.Printf("S-2000 (run-all) finished successfully in %s.", time.Since(startTime))
}

func runScrapeRolesOnly(appConfig S2000AppConfig, startTime time.Time) {
	log.Println("--- Mode: Scrape Roles Only ---")
	jsonFiles, err := getJSONFilesFromExportDir(appConfig.IntermediateExportDirectory)
	if err != nil {
		log.Fatalf("FATAL: Could not read JSON files from export directory: %v", err)
	}
	if len(jsonFiles) == 0 {
		log.Fatalf("No JSON files found in %s to scrape.", appConfig.IntermediateExportDirectory)
	}
	runScrapeRoles(appConfig, jsonFiles)
	log.Printf("S-2000 (scrape-roles) finished in %s.", time.Since(startTime))
}

func runScrapeRoles(appConfig S2000AppConfig, jsonFiles []string) {
	log.Println("--- Starting Aggregated Role Scrape ---")
	bar := progressbar.NewOptions(len(jsonFiles), progressbar.OptionSetDescription("Scraping User/Role Data "), progressbar.OptionSetTheme(progressbar.Theme{Saucer: "[cyan]=[reset]", SaucerHead: "[cyan]>[reset]", BarStart: "[", BarEnd: "]"}), progressbar.OptionSetWidth(30), progressbar.OptionShowCount(), progressbar.OptionOnCompletion(func() { fmt.Fprint(os.Stderr, "\n") }))

	// Use the new data structure
	aggregatedUsers := make(map[string]*AggregatedUserEntry)
	var muData sync.Mutex
	var wgScrape sync.WaitGroup
	scraperConcurrency := runtime.NumCPU() * 2
	if scraperConcurrency < 4 {
		scraperConcurrency = 4
	}
	if scraperConcurrency > 16 {
		scraperConcurrency = 16
	}
	semScrape := make(chan struct{}, scraperConcurrency)

	log.Printf("Scraping roles from %d JSON files (Concurrency: %d)...", len(jsonFiles), scraperConcurrency)
	for _, jsonPath := range jsonFiles {
		wgScrape.Add(1)
		go processJSONFileForAggregatedRoles(jsonPath, aggregatedUsers, &muData, &wgScrape, semScrape, bar)
	}
	wgScrape.Wait()
	log.Println("--- Role scraping tasks completed. ---")

	// --- NEW CSV Writing Logic ---
	log.Printf("Writing %d unique users to CSV: %s", len(aggregatedUsers), appConfig.FinalCsvOutputPath)
	csvFile, err := os.Create(appConfig.FinalCsvOutputPath)
	if err != nil {
		log.Fatalf("FATAL: Error creating role CSV file %s: %v", appConfig.FinalCsvOutputPath, err)
	}
	defer csvFile.Close()
	csvWriter := csv.NewWriter(bufio.NewWriter(csvFile))
	defer csvWriter.Flush()

	// New Headers
	_ = csvWriter.Write([]string{"UserID", "Username", "DisplayName", "RoleIDs", "RoleNames"})

	for _, entry := range aggregatedUsers {
		var roleIDs, roleNames []string
		// Collect IDs and Names from the roles map
		for id, name := range entry.Roles {
			roleIDs = append(roleIDs, id)
			roleNames = append(roleNames, name)
		}
		// Sort them for consistent output order in the CSV
		sort.Strings(roleIDs)
		sort.Strings(roleNames)

		record := []string{
			entry.UserID,
			entry.Username,
			entry.DisplayName,
			strings.Join(roleIDs, "|"),   // Join with a pipe separator
			strings.Join(roleNames, "|"), // Join with a pipe separator
		}
		_ = csvWriter.Write(record)
	}
	if err := csvWriter.Error(); err != nil {
		log.Fatalf("FATAL: Error flushing CSV writer for roles: %v", err)
	}
	log.Printf("Successfully wrote aggregated role data to %s", appConfig.FinalCsvOutputPath)
}

func runMessageScrapeOnly(appConfig S2000AppConfig, startTime time.Time) {
	log.Println("--- Mode: Scrape Messages & Usernames Only ---")
	jsonFiles, err := getJSONFilesFromExportDir(appConfig.IntermediateExportDirectory)
	if err != nil {
		log.Fatalf("FATAL: Could not read JSON files from export directory: %v", err)
	}
	if len(jsonFiles) == 0 {
		log.Fatalf("No JSON files found in %s to scrape.", appConfig.IntermediateExportDirectory)
	}
	bar := progressbar.NewOptions(len(jsonFiles), progressbar.OptionSetDescription("Scraping Message Content  "), progressbar.OptionSetTheme(progressbar.Theme{Saucer: "[magenta]=[reset]", SaucerHead: "[magenta]>[reset]", BarStart: "[", BarEnd: "]"}), progressbar.OptionSetWidth(30), progressbar.OptionShowCount(), progressbar.OptionOnCompletion(func() { fmt.Fprint(os.Stderr, "\n") }))
	csvFile, err := os.Create(appConfig.MessagesCsvOutputPath)
	if err != nil {
		log.Fatalf("FATAL: Error creating message CSV file %s: %v", appConfig.MessagesCsvOutputPath, err)
	}
	defer csvFile.Close()
	csvWriter := csv.NewWriter(bufio.NewWriter(csvFile))
	defer csvWriter.Flush()
	_ = csvWriter.Write([]string{"DisplayName", "MessageContent"})
	var wg sync.WaitGroup
	var muWriter sync.Mutex
	scraperConcurrency := runtime.NumCPU() * 2
	if scraperConcurrency < 4 {
		scraperConcurrency = 4
	}
	if scraperConcurrency > 16 {
		scraperConcurrency = 16
	}
	sem := make(chan struct{}, scraperConcurrency)
	log.Printf("Scraping messages from %d JSON files (Concurrency: %d)...", len(jsonFiles), scraperConcurrency)
	for _, jsonPath := range jsonFiles {
		wg.Add(1)
		go scrapeMessagesFromJSON(jsonPath, csvWriter, &muWriter, &wg, sem, bar)
	}
	wg.Wait()
	if err := csvWriter.Error(); err != nil {
		log.Fatalf("FATAL: Error flushing CSV writer for messages: %v", err)
	}
	log.Printf("Successfully scraped messages to %s", appConfig.MessagesCsvOutputPath)
	log.Printf("S-2000 (scrape-messages) finished in %s.", time.Since(startTime))
}

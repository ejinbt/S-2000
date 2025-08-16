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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/schollz/progressbar/v3"
	"gopkg.in/yaml.v2"
)

// --- S-2000 Configuration Structures ---
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
	ServerIdToExport            string          `yaml:"server_id_to_export"`
	ExportDurationMonths        int             `yaml:"export_duration_months"`
	IntermediateExportDirectory string          `yaml:"intermediate_export_directory"`
	FinalCsvOutputPath          string          `yaml:"final_csv_output_path"`
	MessagesCsvOutputPath       string          `yaml:"messages_csv_output_path"`
	ExtendedScrapeCsvOutputPath string          `yaml:"extended_scrape_csv_output_path"`
	DceExportFormat             string          `yaml:"dce_export_format"`
	DceMaxConcurrent            int             `yaml:"dce_max_concurrent"`
	Channels                    []ChannelConfig `yaml:"channels"`
}
type RootConfig struct {
	Config S2000AppConfig `yaml:"config"`
}

// --- JSON Scraping Structures ---
type ScraperMessageRoles struct {
	Author ScraperAuthorRoles `json:"author"`
}
type ScraperAuthorRoles struct {
	ID, Name, Nickname string
	Roles              []ScraperRole `json:"roles"`
}
type ScraperRole struct{ ID, Name string }
type AggregatedUserEntry struct {
	UserID, Username, DisplayName string
	Roles                         map[string]string
}
type ScraperMessageContent struct {
	Content string
	Author  ScraperAuthorContent `json:"author"`
}
type ScraperAuthorContent struct{ Name, Nickname string }
type ScraperMessageExtended struct {
	ID, Content string
	Timestamp   time.Time
	Author      ScraperAuthorRoles
	Reactions   []ScraperReaction `json:"reactions"`
}
type ScraperReaction struct {
	Emoji ScraperEmoji
	Count int
}
type ScraperEmoji struct{ Name string }

// --- Helper Functions ---
const discordEpoch = 1420070400000

func getCreationTimeFromID(id string) (time.Time, error) {
	idInt, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	timestamp := (idInt >> 22) + discordEpoch
	return time.Unix(0, timestamp*int64(time.Millisecond)).UTC(), nil
}
func sanitizeFilename(name string) string {
	return strings.NewReplacer(" ", "_", "/", "_", "\\", "_", ":", "_", "*", "_", "?", "_", "\"", "_", "<", "_", ">", "_", "|", "_").Replace(name)
}
func limitString(s string, length int) string {
	if len(s) <= length {
		return s
	}
	return s[:length] + "..."
}

// --- Phase 1: DCE Orchestration ---
type CommandAndFileOutput struct {
	CommandArgs    []string
	OutputJSONPath string
}

func generateDateRanges(autoChunk AutoChunkConfig) ([]DateRangeStr, error) {
	var ranges []DateRangeStr
	layout := "2006-01-02"
	startDate, err := time.Parse(layout, autoChunk.StartDate)
	if err != nil {
		return nil, fmt.Errorf("error parsing start date: %v", err)
	}
	endDate, err := time.Parse(layout, autoChunk.EndDate)
	if err != nil {
		return nil, fmt.Errorf("error parsing end date: %v", err)
	}
	if startDate.After(endDate) {
		return nil, fmt.Errorf("start date is after end date")
	}
	currentStartDate := startDate
	for !currentStartDate.After(endDate) {
		currentEndDate := currentStartDate.AddDate(0, autoChunk.ChunkDurationMonths, 0).AddDate(0, 0, -1)
		if currentEndDate.After(endDate) {
			currentEndDate = endDate
		}
		ranges = append(ranges, DateRangeStr(fmt.Sprintf("%s;%s", currentStartDate.Format(layout), currentEndDate.Format(layout))))
		currentStartDate = currentEndDate.AddDate(0, 0, 1)
	}
	return ranges, nil
}
func createDceCommands(appConfig S2000AppConfig) ([]CommandAndFileOutput, error) {
	var commandsAndOutputs []CommandAndFileOutput
	err := os.MkdirAll(appConfig.IntermediateExportDirectory, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("error creating dir: %v", err)
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
				log.Printf("Error generating ranges: %v.", errGen)
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
					log.Printf("Warning: Invalid date range '%s'.", drStr)
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
	bar := progressbar.NewOptions(len(dceCommands), progressbar.OptionSetDescription("Exporting Chat Logs (DCE)"), progressbar.OptionSetTheme(progressbar.Theme{Saucer: "[green]=[reset]", SaucerHead: "[green]>[reset]", BarStart: "[", BarEnd: "]"}), progressbar.OptionShowCount(), progressbar.OptionOnCompletion(func() { fmt.Fprint(os.Stderr, "\n") }))
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

// --- Phase 2: JSON Scraping Functions ---
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
			muData.Lock()
			entry, exists := aggregatedUsers[msg.Author.ID]
			if !exists {
				displayName := msg.Author.Nickname
				if displayName == "" {
					displayName = msg.Author.Name
				}
				entry = &AggregatedUserEntry{UserID: msg.Author.ID, Username: msg.Author.Name, DisplayName: displayName, Roles: make(map[string]string)}
				aggregatedUsers[msg.Author.ID] = entry
			}
			for _, role := range msg.Author.Roles {
				if role.ID != "" {
					entry.Roles[role.ID] = role.Name
				}
			}
			muData.Unlock()
		}
		break
	}
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
func processJSONFileForExtendedScrape(filePath string, writer *csv.Writer, muWriter *sync.Mutex, wg *sync.WaitGroup, sem chan struct{}, bar *progressbar.ProgressBar) {
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
			accountCreationDate, _ := getCreationTimeFromID(msg.Author.ID)
			displayName := msg.Author.Nickname
			if displayName == "" {
				displayName = msg.Author.Name
			}
			var reactionParts []string
			for _, reaction := range msg.Reactions {
				reactionParts = append(reactionParts, fmt.Sprintf("%s:%d", reaction.Emoji.Name, reaction.Count))
			}
			reactionsStr := strings.Join(reactionParts, "|")
			record := []string{msg.ID, msg.Timestamp.UTC().Format(time.RFC3339), msg.Author.ID, msg.Author.Name, displayName, accountCreationDate.Format(time.RFC3339), msg.Content, reactionsStr}
			muWriter.Lock()
			_ = writer.Write(record)
			muWriter.Unlock()
		}
		break
	}
}

// --- S-2000 Main Program & Subcommands ---
func main() {
	startTime := time.Now()
	log.Println("S-2000 (Scrapper-2000) Initializing...")
	configFile := flag.String("config", "config.yaml", "Path to the configuration file.")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "S-2000 - Discord Chat Operations Tool\n\nUsage: %s <command> [options]\n\n", os.Args[0])
		fmt.Fprintln(os.Stderr, "Commands:")
		fmt.Fprintln(os.Stderr, "  run-all           : Runs DCE export, then scrapes for aggregated user/role data.")
		fmt.Fprintln(os.Stderr, "  scrape-roles      : Scrapes aggregated user/role data from existing JSON files.")
		fmt.Fprintln(os.Stderr, "  scrape-messages   : Scrapes usernames and messages from existing JSON files.")
		fmt.Fprintln(os.Stderr, "  scrape-extended   : Fully automates a server-wide scrape for a detailed report.")
		fmt.Fprintln(os.Stderr, "\nOptions:")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() == 0 {
		log.Println("Error: No command provided.")
		flag.Usage()
		os.Exit(1)
	}
	command := flag.Arg(0)
	appConfig, err := loadConfig(*configFile)
	if err != nil {
		log.Fatal(err)
	}
	switch command {
	case "run-all":
		runFullOrchestrationAndScrape(appConfig, startTime)
	case "scrape-roles":
		runScrapeRolesOnly(appConfig, startTime)
	case "scrape-messages":
		runMessageScrapeOnly(appConfig, startTime)
	case "scrape-extended":
		runExtendedScrape(appConfig, startTime)
	default:
		log.Printf("Error: Unknown command '%s'", command)
		flag.Usage()
		os.Exit(1)
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
	dceCommands, err := createDceCommands(appConfig)
	if err != nil {
		log.Fatalf("FATAL: Error creating DCE commands: %v", err)
	}
	executeDcePhase(dceCommands, appConfig.DceMaxConcurrent)
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
		log.Fatalf("FATAL: Could not read JSON files: %v", err)
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
	log.Printf("Writing %d unique users to CSV: %s", len(aggregatedUsers), appConfig.FinalCsvOutputPath)
	csvFile, err := os.Create(appConfig.FinalCsvOutputPath)
	if err != nil {
		log.Fatalf("FATAL: Error creating role CSV file %s: %v", appConfig.FinalCsvOutputPath, err)
	}
	defer csvFile.Close()
	csvWriter := csv.NewWriter(bufio.NewWriter(csvFile))
	defer csvWriter.Flush()
	_ = csvWriter.Write([]string{"UserID", "Username", "DisplayName", "RoleIDs", "RoleNames"})
	for _, entry := range aggregatedUsers {
		var roleIDs, roleNames []string
		for id, name := range entry.Roles {
			roleIDs = append(roleIDs, id)
			roleNames = append(roleNames, name)
		}
		sort.Strings(roleIDs)
		sort.Strings(roleNames)
		record := []string{entry.UserID, entry.Username, entry.DisplayName, strings.Join(roleIDs, "|"), strings.Join(roleNames, "|")}
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
		log.Fatalf("FATAL: Could not read JSON files: %v", err)
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

func runExtendedScrape(appConfig S2000AppConfig, startTime time.Time) {
	log.Println("--- Mode: Extended Server Scrape ---")
	log.Println("--- Phase 1: Exporting all server channels (this may take a while)... ---")
	err := os.MkdirAll(appConfig.IntermediateExportDirectory, os.ModePerm)
	if err != nil {
		log.Fatalf("FATAL: Could not create intermediate directory: %v", err)
	}

	afterDate := time.Now().AddDate(0, -appConfig.ExportDurationMonths, 0)
	afterDateStr := afterDate.Format("2006-01-02")

	// The output path for exportguild is a directory. DCE will create files inside it.
	// We add a trailing slash to be explicit, which DCE recommends.
	outputDir := appConfig.IntermediateExportDirectory + string(os.PathSeparator)

	log.Printf("Executing DCE 'exportguild' for server %s for messages after %s...", appConfig.ServerIdToExport, afterDateStr)
	guildExportCmd := exec.Command(appConfig.DceExecPath, "exportguild", "-t", appConfig.Token, "-g", appConfig.ServerIdToExport, "-f", "Json", "--after", afterDateStr, "-o", outputDir)

	// This is a single, long-running command, so no progress bar for this part. We just wait for it to finish.
	if output, err := guildExportCmd.CombinedOutput(); err != nil {
		log.Fatalf("FATAL: Failed to export server with DCE 'exportguild': %v\nOutput:\n%s", err, string(output))
	}
	log.Printf("Successfully exported server channels to directory: %s", appConfig.IntermediateExportDirectory)

	log.Println("--- Phase 2: Starting extended scrape of all exported data ---")
	exportedMessageFiles, err := getJSONFilesFromExportDir(appConfig.IntermediateExportDirectory)
	if err != nil || len(exportedMessageFiles) == 0 {
		log.Fatalf("FATAL: No message JSON files found to scrape after export phase. Error: %v", err)
	}

	csvFile, err := os.Create(appConfig.ExtendedScrapeCsvOutputPath)
	if err != nil {
		log.Fatalf("FATAL: Error creating extended scrape CSV file %s: %v", appConfig.ExtendedScrapeCsvOutputPath, err)
	}
	defer csvFile.Close()
	csvWriter := csv.NewWriter(bufio.NewWriter(csvFile))
	defer csvWriter.Flush()

	headers := []string{"MessageID", "TimestampUTC", "AuthorID", "AuthorName", "DisplayName", "AccountCreationDateUTC", "MessageContent", "Reactions"}
	_ = csvWriter.Write(headers)

	bar := progressbar.NewOptions(len(exportedMessageFiles), progressbar.OptionSetDescription("Scraping Extended Data  "), progressbar.OptionSetTheme(progressbar.Theme{Saucer: "[blue]=[reset]", SaucerHead: "[blue]>[reset]", BarStart: "[", BarEnd: "]"}), progressbar.OptionSetWidth(30), progressbar.OptionShowCount(), progressbar.OptionOnCompletion(func() { fmt.Fprint(os.Stderr, "\n") }))

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

	log.Printf("Scraping extended data from %d message files (Concurrency: %d)...", len(exportedMessageFiles), scraperConcurrency)
	for _, jsonPath := range exportedMessageFiles {
		wg.Add(1)
		go processJSONFileForExtendedScrape(jsonPath, csvWriter, &muWriter, &wg, sem, bar)
	}
	wg.Wait()

	if err := csvWriter.Error(); err != nil {
		log.Fatalf("FATAL: Error flushing CSV writer for extended scrape: %v", err)
	}
	log.Printf("Successfully wrote extended scrape data to %s", appConfig.ExtendedScrapeCsvOutputPath)
	log.Printf("S-2000 (scrape-extended) finished successfully in %s.", time.Since(startTime))
}

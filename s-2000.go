package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
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
	StartDate, EndDate  string
	ChunkDurationMonths int
}
type ChannelConfig struct {
	ID, Name   string
	DateRanges []DateRangeStr
	AutoChunk  *AutoChunkConfig
}
type S2000AppConfig struct {
	Token                         string          `yaml:"token"`
	DceExecPath                   string          `yaml:"dce_execpath"`
	ServerIdToExport              string          `yaml:"server_id_to_export"`
	ExportDurationMonths          int             `yaml:"export_duration_months"`
	InputUserCsvPath              string          `yaml:"input_user_csv_path"`
	FirstMessageOutputPath        string          `yaml:"first_message_output_path"`
	IntermediateExportDirectory   string          `yaml:"intermediate_export_directory"` // Correct tag added
	FinalCsvOutputPath            string          `yaml:"final_csv_output_path"`
	MessagesCsvOutputPath         string          `yaml:"messages_csv_output_path"`
	ExtendedScrapeCsvOutputPath   string          `yaml:"extended_scrape_csv_output_path"`
	ExtendedAnalysisJsonDirectory string          `yaml:"extended_analysis_json_directory"` // Correct tag added
	DceExportFormat               string          `yaml:"dce_export_format"`
	DceMaxConcurrent              int             `yaml:"dce_max_concurrent"`
	Channels                      []ChannelConfig `yaml:"channels"`
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
	Channels                      map[string]bool
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
type GuildChannel struct{ ID, Name, Type string }
type GuildExportFile struct {
	Guild    GuildInfo
	Channels []GuildChannel
}
type GuildInfo struct{ Name string }
type ExtendedUserData struct {
	Author                  ScraperAuthorRoles
	FirstMessage            ScraperMessageExtended
	FirstMessageChannelName string
}

// Structs for analyze-to-json output
type ChannelAnalysisOutput struct {
	Channel      ChannelInfo       `json:"channel"`
	AnalyzedAt   time.Time         `json:"analyzedAt"`
	MessageCount int               `json:"messageCount"`
	Messages     []AnalyzedMessage `json:"messages"`
}
type ChannelInfo struct{ ID, Name string }
type AnalyzedMessage struct {
	MessageID    string             `json:"messageId"`
	Content      string             `json:"content"`
	TimestampUTC time.Time          `json:"timestampUTC"`
	Author       AnalyzedAuthor     `json:"author"`
	Reactions    []AnalyzedReaction `json:"reactions"`
}
type AnalyzedAuthor struct {
	UserID                 string    `json:"userId"`
	Username               string    `json:"username"`
	DisplayName            string    `json:"displayName"`
	AccountCreationDateUTC time.Time `json:"accountCreationDateUTC"`
}
type AnalyzedReaction struct {
	EmojiName string `json:"emojiName"`
	Count     int    `json:"count"`
}

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
func createDceCommands(appConfig S2000AppConfig, fullExport bool) ([]CommandAndFileOutput, error) {
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
		if !fullExport && channel.AutoChunk != nil {
			generatedRanges, errGen := generateDateRanges(*channel.AutoChunk)
			if errGen != nil {
				log.Printf("Error generating ranges: %v.", errGen)
			} else {
				effectiveDateRanges = generatedRanges
			}
		} else if !fullExport {
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
func processJSONFileForAggregatedRoles(filePath, channelName string, aggregatedUsers map[string]*AggregatedUserEntry, muData *sync.Mutex, wg *sync.WaitGroup, sem chan struct{}, bar *progressbar.ProgressBar) {
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
				entry = &AggregatedUserEntry{UserID: msg.Author.ID, Username: msg.Author.Name, DisplayName: displayName, Roles: make(map[string]string), Channels: make(map[string]bool)}
				aggregatedUsers[msg.Author.ID] = entry
			}
			entry.Channels[channelName] = true
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
func scrapeMessagesFromJSON(filePath, channelName string, writer *csv.Writer, muWriter *sync.Mutex, wg *sync.WaitGroup, sem chan struct{}, bar *progressbar.ProgressBar) {
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
			_ = writer.Write([]string{channelName, displayName, msg.Content})
			muWriter.Unlock()
		}
		break
	}
}
func findFirstMessagesInFile(filePath string) (map[string]ScraperMessageExtended, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	firstMessages := make(map[string]ScraperMessageExtended)
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
			existingMsg, exists := firstMessages[msg.Author.ID]
			if !exists || msg.Timestamp.Before(existingMsg.Timestamp) {
				firstMessages[msg.Author.ID] = msg
			}
		}
		break
	}
	return firstMessages, nil
}
func aggregateFirstMessageFromJSON(filePath, channelName string, targetUsers map[string]bool, firstMessages map[string]*ExtendedUserData, muData *sync.Mutex, wg *sync.WaitGroup, sem chan struct{}, bar *progressbar.ProgressBar) {
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
			if _, isTarget := targetUsers[msg.Author.ID]; !isTarget {
				continue
			}
			muData.Lock()
			userData, exists := firstMessages[msg.Author.ID]
			if !exists {
				userData = &ExtendedUserData{Author: msg.Author, FirstMessage: msg, FirstMessageChannelName: channelName}
				firstMessages[msg.Author.ID] = userData
			} else {
				if msg.Timestamp.Before(userData.FirstMessage.Timestamp) {
					userData.FirstMessage = msg
					userData.Author = msg.Author
					userData.FirstMessageChannelName = channelName
				}
			}
			muData.Unlock()
		}
		break
	}
}
func processFileForJsonAnalysis(filePath string, outputDir string, wg *sync.WaitGroup, sem chan struct{}, bar *progressbar.ProgressBar) {
	defer wg.Done()
	defer bar.Add(1)
	sem <- struct{}{}
	defer func() { <-sem }()
	file, err := os.Open(filePath)
	if err != nil {
		return
	}
	var channelInfo ChannelInfo
	var messages []AnalyzedMessage
	content, err := io.ReadAll(file)
	if err != nil {
		return
	}
	file.Close()
	var data struct{ Channel struct{ ID, Name string } }
	if err := json.Unmarshal(content, &data); err == nil {
		channelInfo = ChannelInfo{ID: data.Channel.ID, Name: data.Channel.Name}
	} else {
		return
	}
	decoder := json.NewDecoder(strings.NewReader(string(content)))
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
			var analyzedReactions []AnalyzedReaction
			for _, reaction := range msg.Reactions {
				analyzedReactions = append(analyzedReactions, AnalyzedReaction{EmojiName: reaction.Emoji.Name, Count: reaction.Count})
			}
			analyzedMsg := AnalyzedMessage{
				MessageID:    msg.ID,
				Content:      msg.Content,
				TimestampUTC: msg.Timestamp.UTC(),
				Author: AnalyzedAuthor{
					UserID: msg.Author.ID, Username: msg.Author.Name, DisplayName: displayName, AccountCreationDateUTC: accountCreationDate,
				},
				Reactions: analyzedReactions,
			}
			messages = append(messages, analyzedMsg)
		}
		break
	}
	outputData := ChannelAnalysisOutput{
		Channel: channelInfo, AnalyzedAt: time.Now().UTC(), MessageCount: len(messages), Messages: messages,
	}
	outputFilename := filepath.Join(outputDir, fmt.Sprintf("%s_%s_analysis.json", sanitizeFilename(channelInfo.Name), channelInfo.ID))
	outputFile, err := os.Create(outputFilename)
	if err != nil {
		log.Printf("\nERROR creating output file %s: %v", outputFilename, err)
		return
	}
	defer outputFile.Close()
	encoder := json.NewEncoder(outputFile)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(outputData); err != nil {
		log.Printf("\nERROR writing JSON to %s: %v", outputFilename, err)
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
		fmt.Fprintln(os.Stderr, "  run-all              : Runs DCE export, then scrapes for aggregated user/role data.")
		fmt.Fprintln(os.Stderr, "  scrape-roles         : Scrapes aggregated user/role data from existing JSON files.")
		fmt.Fprintln(os.Stderr, "  scrape-messages      : Scrapes usernames and messages from existing JSON files.")
		fmt.Fprintln(os.Stderr, "  scrape-extended      : Fully automates a server-wide scrape for a detailed report.")
		fmt.Fprintln(os.Stderr, "  analyze-extended     : Scrapes a detailed report from a folder of existing JSON files.")
		fmt.Fprintln(os.Stderr, "  analyze-to-json      : Analyzes a folder of JSONs, creating a separate JSON analysis file per channel.")
		fmt.Fprintln(os.Stderr, "  find-first-message   : For a list of users, finds their first ever message in specified channels.")
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
	case "analyze-extended":
		runAnalyzeExtended(appConfig, startTime)
	case "analyze-to-json":
		runAnalyzeToJson(appConfig, startTime)
	case "find-first-message":
		runFindFirstMessage(appConfig, startTime)
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
	dceCommands, err := createDceCommands(appConfig, false)
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
		channelDir := filepath.Base(filepath.Dir(jsonPath))
		parts := strings.Split(channelDir, "_")
		channelName := parts[0]
		wgScrape.Add(1)
		go processJSONFileForAggregatedRoles(jsonPath, channelName, aggregatedUsers, &muData, &wgScrape, semScrape, bar)
	}
	wgScrape.Wait()
	log.Println("--- Role scraping tasks completed. ---")
	log.Printf("Writing %d unique users to CSV: %s", len(aggregatedUsers), appConfig.FinalCsvOutputPath)
	csvFile, err := os.Create(appConfig.FinalCsvOutputPath)
	if err != nil {
		log.Fatalf("FATAL: Error creating role CSV file: %v", err)
	}
	defer csvFile.Close()
	csvWriter := csv.NewWriter(bufio.NewWriter(csvFile))
	defer csvWriter.Flush()
	_ = csvWriter.Write([]string{"UserID", "Username", "DisplayName", "RoleIDs", "RoleNames", "AllChannelsSpokenIn"})
	for _, entry := range aggregatedUsers {
		var roleIDs, roleNames, channelNames []string
		for id, name := range entry.Roles {
			roleIDs = append(roleIDs, id)
			roleNames = append(roleNames, name)
		}
		for chName := range entry.Channels {
			channelNames = append(channelNames, chName)
		}
		sort.Strings(roleIDs)
		sort.Strings(roleNames)
		sort.Strings(channelNames)
		record := []string{entry.UserID, entry.Username, entry.DisplayName, strings.Join(roleIDs, "|"), strings.Join(roleNames, "|"), strings.Join(channelNames, "|")}
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
		log.Fatalf("FATAL: Error creating message CSV file: %v", err)
	}
	defer csvFile.Close()
	csvWriter := csv.NewWriter(bufio.NewWriter(csvFile))
	defer csvWriter.Flush()
	_ = csvWriter.Write([]string{"ChannelName", "DisplayName", "MessageContent"})
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
		channelDir := filepath.Base(filepath.Dir(jsonPath))
		parts := strings.Split(channelDir, "_")
		channelName := parts[0]
		wg.Add(1)
		go scrapeMessagesFromJSON(jsonPath, channelName, csvWriter, &muWriter, &wg, sem, bar)
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
	outputDir := appConfig.IntermediateExportDirectory + string(os.PathSeparator)
	log.Printf("Executing DCE 'exportguild' for server %s for messages after %s...", appConfig.ServerIdToExport, afterDateStr)
	guildExportCmd := exec.Command(appConfig.DceExecPath, "exportguild", "-t", appConfig.Token, "-g", appConfig.ServerIdToExport, "-f", "Json", "--after", afterDateStr, "-o", outputDir)
	if output, err := guildExportCmd.CombinedOutput(); err != nil {
		log.Fatalf("FATAL: Failed to export server with DCE 'exportguild': %v\nOutput:\n%s", err, string(output))
	}
	log.Printf("Successfully exported server channels to directory: %s", appConfig.IntermediateExportDirectory)
	log.Println("--- Phase 2: Starting extended scrape of all exported data ---")
	runAnalyzeExtended(appConfig, startTime)
}

func runAnalyzeExtended(appConfig S2000AppConfig, startTime time.Time) {
	log.Println("--- Mode: Analyze Extended Data from Folder ---")
	log.Println("--- Starting extended analysis of existing data ---")
	if appConfig.IntermediateExportDirectory == "" {
		log.Fatal("FATAL: 'intermediate_export_directory' must be set in config.yaml.")
	}
	exportedMessageFiles, err := getJSONFilesFromExportDir(appConfig.IntermediateExportDirectory)
	if err != nil {
		log.Fatalf("FATAL: Could not read JSON files from '%s': %v", appConfig.IntermediateExportDirectory, err)
	}
	if len(exportedMessageFiles) == 0 {
		log.Fatalf("FATAL: No JSON files found to scrape in '%s'.", appConfig.IntermediateExportDirectory)
	}
	csvFile, err := os.Create(appConfig.ExtendedScrapeCsvOutputPath)
	if err != nil {
		log.Fatalf("FATAL: Error creating extended CSV file %s: %v", appConfig.ExtendedScrapeCsvOutputPath, err)
	}
	defer csvFile.Close()
	csvWriter := csv.NewWriter(bufio.NewWriter(csvFile))
	defer csvWriter.Flush()
	headers := []string{"ChannelID", "ChannelName", "AuthorID", "AuthorName", "DisplayName", "AccountCreationDateUTC", "FirstMessageDateInChannelUTC", "MessageContent", "Reactions"}
	_ = csvWriter.Write(headers)
	bar := progressbar.NewOptions(len(exportedMessageFiles), progressbar.OptionSetDescription("Analyzing Extended Data   "), progressbar.OptionSetTheme(progressbar.Theme{Saucer: "[yellow]=[reset]", SaucerHead: "[yellow]>[reset]", BarStart: "[", BarEnd: "]"}), progressbar.OptionSetWidth(30), progressbar.OptionShowCount(), progressbar.OptionOnCompletion(func() { fmt.Fprint(os.Stderr, "\n") }))
	log.Printf("Analyzing %d JSON files from %s...", len(exportedMessageFiles), appConfig.IntermediateExportDirectory)
	for _, jsonPath := range exportedMessageFiles {
		var data struct{ Channel struct{ ID, Name string } }
		file, err := os.Open(jsonPath)
		if err != nil {
			bar.Add(1)
			continue
		}
		json.NewDecoder(file).Decode(&data)
		file.Close()
		channelID, channelName := data.Channel.ID, data.Channel.Name
		firstMessages, err := findFirstMessagesInFile(jsonPath)
		if err != nil {
			log.Printf("\nWarning: Could not process file %s: %v", jsonPath, err)
			bar.Add(1)
			continue
		}
		var userIDs []string
		for uid := range firstMessages {
			userIDs = append(userIDs, uid)
		}
		sort.Strings(userIDs)
		for _, userID := range userIDs {
			msg := firstMessages[userID]
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
			record := []string{channelID, channelName, msg.Author.ID, msg.Author.Name, displayName, accountCreationDate.Format(time.RFC3339), msg.Timestamp.UTC().Format(time.RFC3339), msg.Content, reactionsStr}
			_ = csvWriter.Write(record)
		}
		bar.Add(1)
	}
	if err := csvWriter.Error(); err != nil {
		log.Fatalf("FATAL: Error flushing CSV writer for extended analysis: %v", err)
	}
	log.Printf("Successfully wrote extended analysis data to %s", appConfig.ExtendedScrapeCsvOutputPath)
	log.Printf("S-2000 (analyze-extended) finished successfully in %s.", time.Since(startTime))
}

func readTargetUsers(filePath string) (map[string]bool, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("could not open user CSV '%s': %w", filePath, err)
	}
	defer file.Close()
	csvReader := csv.NewReader(file)
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("could not parse user CSV: %w", err)
	}
	targetUsers := make(map[string]bool)
	if len(records) > 1 {
		for i := 1; i < len(records); i++ {
			if len(records[i]) > 0 && records[i][0] != "" {
				targetUsers[records[i][0]] = true
			}
		}
	}
	log.Printf("Loaded %d target users from %s", len(targetUsers), filePath)
	return targetUsers, nil
}

func runFindFirstMessage(appConfig S2000AppConfig, startTime time.Time) {
	log.Println("--- Mode: Find First Message for Target Users ---")
	log.Println("--- Phase 0: Loading target users from input CSV ---")
	if appConfig.InputUserCsvPath == "" {
		log.Fatal("FATAL: 'input_user_csv_path' is not set in config.yaml.")
	}
	targetUsers, err := readTargetUsers(appConfig.InputUserCsvPath)
	if err != nil {
		log.Fatalf("FATAL: %v", err)
	}
	if len(targetUsers) == 0 {
		log.Fatalf("FATAL: No target users loaded from CSV. Ensure CSV has a header and UserIDs in the first column.")
	}
	log.Println("--- Phase 1: Exporting FULL history of specified channels ---")
	dceCommands, err := createDceCommands(appConfig, true)
	if err != nil {
		log.Fatalf("FATAL: Error creating DCE commands: %v", err)
	}
	executeDcePhase(dceCommands, appConfig.DceMaxConcurrent)
	log.Println("--- Phase 2: Analyzing exports to find first messages ---")
	jsonFiles, err := getJSONFilesFromExportDir(appConfig.IntermediateExportDirectory)
	if err != nil || len(jsonFiles) == 0 {
		log.Fatalf("FATAL: No JSON files found to scrape. Error: %v", err)
	}
	bar := progressbar.NewOptions(len(jsonFiles), progressbar.OptionSetDescription("Finding First Messages "), progressbar.OptionSetTheme(progressbar.Theme{Saucer: "[purple]=[reset]", SaucerHead: "[purple]>[reset]", BarStart: "[", BarEnd: "]"}), progressbar.OptionSetWidth(30), progressbar.OptionShowCount(), progressbar.OptionOnCompletion(func() { fmt.Fprint(os.Stderr, "\n") }))
	firstMessages := make(map[string]*ExtendedUserData)
	var muData sync.Mutex
	var wg sync.WaitGroup
	scraperConcurrency := runtime.NumCPU() * 2
	if scraperConcurrency < 4 {
		scraperConcurrency = 4
	}
	if scraperConcurrency > 16 {
		scraperConcurrency = 16
	}
	sem := make(chan struct{}, scraperConcurrency)
	for _, jsonPath := range jsonFiles {
		channelDir := filepath.Base(filepath.Dir(jsonPath))
		parts := strings.Split(channelDir, "_")
		channelName := parts[0]
		wg.Add(1)
		go aggregateFirstMessageFromJSON(jsonPath, channelName, targetUsers, firstMessages, &muData, &wg, sem, bar)
	}
	wg.Wait()
	log.Printf("Found first messages for %d out of %d target users.", len(firstMessages), len(targetUsers))
	log.Printf("Writing first message report to %s", appConfig.FirstMessageOutputPath)
	csvFile, err := os.Create(appConfig.FirstMessageOutputPath)
	if err != nil {
		log.Fatalf("FATAL: Error creating output CSV: %v", err)
	}
	defer csvFile.Close()
	csvWriter := csv.NewWriter(bufio.NewWriter(csvFile))
	defer csvWriter.Flush()
	headers := []string{"AuthorID", "AuthorName", "DisplayName", "AccountCreationDateUTC", "FirstMessageDateUTC", "FirstMessageChannel", "FirstMessageContent", "FirstMessageReactions"}
	_ = csvWriter.Write(headers)
	for _, userData := range firstMessages {
		accountCreationDate, _ := getCreationTimeFromID(userData.Author.ID)
		displayName := userData.Author.Nickname
		if displayName == "" {
			displayName = userData.Author.Name
		}
		var reactionParts []string
		for _, reaction := range userData.FirstMessage.Reactions {
			reactionParts = append(reactionParts, fmt.Sprintf("%s:%d", reaction.Emoji.Name, reaction.Count))
		}
		record := []string{userData.Author.ID, userData.Author.Name, displayName, accountCreationDate.Format(time.RFC3339), userData.FirstMessage.Timestamp.UTC().Format(time.RFC3339), userData.FirstMessageChannelName, userData.FirstMessage.Content, strings.Join(reactionParts, "|")}
		_ = csvWriter.Write(record)
	}
	if err := csvWriter.Error(); err != nil {
		log.Fatalf("FATAL: Error flushing CSV writer: %v", err)
	}
	log.Printf("S-2000 (find-first-message) finished successfully in %s.", time.Since(startTime))
}

func runAnalyzeToJson(appConfig S2000AppConfig, startTime time.Time) {
	log.Println("--- Mode: Analyze to JSON per Channel ---")
	if appConfig.IntermediateExportDirectory == "" {
		log.Fatal("FATAL: 'intermediate_export_directory' must be set in config.yaml for input.")
	}
	if appConfig.ExtendedAnalysisJsonDirectory == "" {
		log.Fatal("FATAL: 'extended_analysis_json_directory' must be set in config.yaml for output.")
	}
	jsonFiles, err := getJSONFilesFromExportDir(appConfig.IntermediateExportDirectory)
	if err != nil {
		log.Fatalf("FATAL: Could not read JSON files from '%s': %v", appConfig.IntermediateExportDirectory, err)
	}
	if len(jsonFiles) == 0 {
		log.Fatalf("FATAL: No JSON files found to analyze in '%s'.", appConfig.IntermediateExportDirectory)
	}
	err = os.MkdirAll(appConfig.ExtendedAnalysisJsonDirectory, os.ModePerm)
	if err != nil {
		log.Fatalf("FATAL: Could not create output directory '%s': %v", appConfig.ExtendedAnalysisJsonDirectory, err)
	}
	bar := progressbar.NewOptions(len(jsonFiles), progressbar.OptionSetDescription("Analyzing Channels to JSON"), progressbar.OptionSetTheme(progressbar.Theme{Saucer: "[cyan]=[reset]", SaucerHead: "[cyan]>[reset]", BarStart: "[", BarEnd: "]"}), progressbar.OptionShowCount(), progressbar.OptionOnCompletion(func() { fmt.Fprint(os.Stderr, "\n") }))
	var wg sync.WaitGroup
	scraperConcurrency := runtime.NumCPU()
	if scraperConcurrency < 2 {
		scraperConcurrency = 2
	}
	sem := make(chan struct{}, scraperConcurrency)
	log.Printf("Analyzing %d JSON files and creating individual JSON reports (Concurrency: %d)...", len(jsonFiles), scraperConcurrency)
	for _, jsonPath := range jsonFiles {
		wg.Add(1)
		go processFileForJsonAnalysis(jsonPath, appConfig.ExtendedAnalysisJsonDirectory, &wg, sem, bar)
	}
	wg.Wait()
	log.Printf("Successfully created JSON analysis files in %s", appConfig.ExtendedAnalysisJsonDirectory)
	log.Printf("S-2000 (analyze-to-json) finished successfully in %s.", time.Since(startTime))
}

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
	"strings"
	"sync"
	"time"

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
	IntermediateExportDirectory string          `yaml:"intermediate_export_directory"`
	FinalCsvOutputPath          string          `yaml:"final_csv_output_path"`
	DceExportFormat             string          `yaml:"dce_export_format"` // Should likely be hardcoded to Json
	DceMaxConcurrent            int             `yaml:"dce_max_concurrent"`
	Channels                    []ChannelConfig `yaml:"channels"`
}

type RootConfig struct {
	Config S2000AppConfig `yaml:"config"`
}

type ScraperMessage struct {
	Author ScraperAuthor `json:"author"`
}

type ScraperAuthor struct {
	ID       string        `json:"id"`
	Name     string        `json:"name"`
	Nickname string        `json:"nickname"`
	Roles    []ScraperRole `json:"roles"`
}

type ScraperRole struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type UserRoleEntry struct {
	UserID      string
	Username    string
	DisplayName string
	RoleID      string
	RoleName    string
}

// --- Helper Functions ---
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

// --- Phase 1: DCE Orchestration ---
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

// CommandAndFileOutput stores the command arguments and the expected output JSON file path
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
		err := os.MkdirAll(channelOutputDir, os.ModePerm)
		if err != nil {
			log.Printf("Warning: error creating channel-specific directory '%s': %v.", channelOutputDir, err)
			// Continue, DCE might still write to base if subdir creation fails
		}

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
		if exportFormat == "" { // Default to Json if not specified
			exportFormat = "Json"
		}

		if len(effectiveDateRanges) == 0 {
			filenameSuffix := fmt.Sprintf("%s_all.%s", channel.ID, strings.ToLower(exportFormat))
			if channel.Name != "" {
				filenameSuffix = fmt.Sprintf("%s_%s_all.%s", sanitizeFilename(channel.Name), channel.ID, strings.ToLower(exportFormat))
			}
			outputJSONPath := filepath.Join(channelOutputDir, filenameSuffix)
			args := []string{
				appConfig.DceExecPath, "export", "-t", appConfig.Token,
				"-c", channel.ID, "-f", exportFormat, "-o", outputJSONPath,
			}
			commandsAndOutputs = append(commandsAndOutputs, CommandAndFileOutput{CommandArgs: args, OutputJSONPath: outputJSONPath})
		} else {
			for _, drStr := range effectiveDateRanges {
				parts := strings.Split(string(drStr), ";")
				if len(parts) != 2 {
					log.Printf("Warning: Invalid date range '%s' for channel %s. Skipping.", drStr, channel.ID)
					continue
				}
				startDateStr, endDateStr := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
				filenameSuffix := fmt.Sprintf("%s_from_%s_to_%s.%s", channel.ID, strings.ReplaceAll(startDateStr, "-", ""), strings.ReplaceAll(endDateStr, "-", ""), strings.ToLower(exportFormat))
				if channel.Name != "" {
					filenameSuffix = fmt.Sprintf("%s_%s_from_%s_to_%s.%s", sanitizeFilename(channel.Name), channel.ID, strings.ReplaceAll(startDateStr, "-", ""), strings.ReplaceAll(endDateStr, "-", ""), strings.ToLower(exportFormat))
				}
				outputJSONPath := filepath.Join(channelOutputDir, filenameSuffix)
				args := []string{
					appConfig.DceExecPath, "export", "-t", appConfig.Token,
					"-c", channel.ID, "-f", exportFormat,
					"--after", startDateStr, "--before", endDateStr, "-o", outputJSONPath,
				}
				commandsAndOutputs = append(commandsAndOutputs, CommandAndFileOutput{CommandArgs: args, OutputJSONPath: outputJSONPath})
			}
		}
	}
	return commandsAndOutputs, nil
}

func runDceCommand(cmdAndOutput CommandAndFileOutput, wg *sync.WaitGroup, sem chan struct{}, successfulFiles *[]string, muFiles *sync.Mutex) {
	defer wg.Done()
	sem <- struct{}{}
	defer func() { <-sem }()

	executable := cmdAndOutput.CommandArgs[0]
	argsOnly := cmdAndOutput.CommandArgs[1:]

	log.Printf("DCE Starting: %s %s", executable, strings.Join(argsOnly, " "))
	cmd := exec.Command(executable, argsOnly...)
	output, err := cmd.CombinedOutput()

	if err != nil {
		log.Printf("DCE ERROR: %s %s\nError: %v\nOutput (last 500 chars):\n%s\n",
			executable, strings.Join(argsOnly, " "), err, limitString(string(output), 500))
		return
	}
	log.Printf("DCE SUCCESS: %s %s (Output: %s)", executable, strings.Join(argsOnly, " "), cmdAndOutput.OutputJSONPath)
	muFiles.Lock()
	*successfulFiles = append(*successfulFiles, cmdAndOutput.OutputJSONPath)
	muFiles.Unlock()
}

// --- Phase 2: JSON Scraping Functions ---
func processJSONFileForScraping(filePath string, uniqueEntries map[string]UserRoleEntry, muData *sync.Mutex, wgScrape *sync.WaitGroup, semScrape chan struct{}) {
	defer wgScrape.Done()
	semScrape <- struct{}{}
	defer func() { <-semScrape }()

	log.Printf("Scraper Processing: %s", filePath)
	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("Scraper ERROR opening file %s: %v", filePath, err)
		return
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	decoder := json.NewDecoder(reader)
	_, err = decoder.Token() // Read '{'
	if err != nil {
		log.Printf("Scraper ERROR reading opening brace from %s: %v", filePath, err)
		return
	}

	foundMessages := false
	for decoder.More() {
		token, errToken := decoder.Token()
		if errToken == io.EOF {
			break
		}
		if errToken != nil {
			log.Printf("Scraper ERROR reading token key from %s: %v", filePath, errToken)
			return
		}
		key, ok := token.(string)
		if !ok {
			var dummy interface{}
			if errDec := decoder.Decode(&dummy); errDec != nil && errDec != io.EOF {
				log.Printf("Scraper ERROR skipping value in %s: %v", filePath, errDec)
				return
			}
			continue
		}
		if key == "messages" {
			foundMessages = true
			_, errToken = decoder.Token() // Read '['
			if errToken != nil {
				log.Printf("Scraper ERROR reading opening bracket for messages in %s: %v", filePath, errToken)
				return
			}
			for decoder.More() { // Outer loop for messages
				var msg ScraperMessage
				if errDec := decoder.Decode(&msg); errDec != nil { // Attempt to decode a ScraperMessage
					if errDec == io.EOF { // If EOF during message decode, break message loop
						break
					}
					// Log the initial decode error for ScraperMessage
					log.Printf("Scraper WARNING: Error decoding ScraperMessage object in %s: %v. Attempting to skip this value.", filePath, errDec)

					// Attempt to skip the entire JSON value that caused the ScraperMessage decode error.
					var skipDummy interface{}
					if errSkip := decoder.Decode(&skipDummy); errSkip != nil {
						log.Printf("Scraper ERROR: Also failed to skip the problematic value using Decode(&skipDummy) in %s: %v. The JSON stream might be badly corrupted. Stopping processing of messages for this file.", filePath, errSkip)
						// If skipping also fails, it's hard to recover robustly.
						// Break from the outer "for decoder.More()" (messages loop) for this file.
						break
					} else {
						log.Printf("Scraper INFO: Successfully skipped one problematic JSON value in %s. Will attempt to process next message.", filePath)
					}

					// After attempting to skip (successfully or not, followed by a break on failure),
					// continue to the next iteration of the *outer* message loop.
					// The decoder.More() will then check if there's more data for another message.
					continue
				}

				// If decode into ScraperMessage was successful:
				if msg.Author.ID == "" { // Basic validation for a successfully decoded message
					// log.Printf("Scraper INFO: Decoded message has no author ID in %s. Skipping.", filePath)
					continue
				}

				displayName := msg.Author.Nickname
				if displayName == "" {
					displayName = msg.Author.Name
				}

				if len(msg.Author.Roles) == 0 {
					entryKey := fmt.Sprintf("%s-NO_ROLE_ASSIGNED", msg.Author.ID)
					muData.Lock() // Assuming muData is the mutex for uniqueEntries
					if _, exists := uniqueEntries[entryKey]; !exists {
						uniqueEntries[entryKey] = UserRoleEntry{
							UserID:      msg.Author.ID,
							Username:    msg.Author.Name,
							DisplayName: displayName,
							RoleID:      "",
							RoleName:    "",
						}
					}
					muData.Unlock()
				} else {
					for _, role := range msg.Author.Roles {
						roleID := role.ID
						roleName := role.Name
						roleKeyPart := role.ID
						if roleKeyPart == "" {
							roleKeyPart = "EMPTY_ROLE_ID"
						}
						entryKey := fmt.Sprintf("%s-%s", msg.Author.ID, roleKeyPart)
						muData.Lock() // Assuming muData is the mutex for uniqueEntries
						if _, exists := uniqueEntries[entryKey]; !exists {
							uniqueEntries[entryKey] = UserRoleEntry{
								UserID:      msg.Author.ID,
								Username:    msg.Author.Name,
								DisplayName: displayName,
								RoleID:      roleID,
								RoleName:    roleName,
							}
						}
						muData.Unlock()
					}
				}
			}
			_, errToken = decoder.Token() // Read ']'
			if errToken != nil && errToken != io.EOF {
				log.Printf("Scraper ERROR reading closing bracket for messages in %s: %v", filePath, errToken)
			}
			break // Done with "messages" key
		} else {
			var dummy interface{}
			if errDec := decoder.Decode(&dummy); errDec != nil {
				if errDec == io.EOF {
					break
				}
				log.Printf("Scraper ERROR skipping value for key '%s' in %s: %v", key, filePath, errDec)
				return
			}
		}
	}
	if !foundMessages {
		log.Printf("Scraper WARNING: 'messages' key not found in %s", filePath)
	}
	// log.Printf("Scraper Finished: %s", filePath)
}

// --- Main S-2000 Program ---
func main() {
	startTime := time.Now()
	log.Println("S-2000 (Scrapper-2000) Initializing...")

	// --- Configuration Loading ---
	var configFile string
	flag.StringVar(&configFile, "config", "config.yaml", "Path to the configuration file")
	flag.Parse() // Parse command-line flags

	log.Printf("Using config file: %s", configFile)
	data, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatalf("FATAL: Error reading config file '%s': %v", configFile, err)
	}
	var rootCfg RootConfig
	err = yaml.Unmarshal(data, &rootCfg)
	if err != nil {
		log.Fatalf("FATAL: Error parsing YAML from '%s': %v", configFile, err)
	}
	appConfig := rootCfg.Config
	if appConfig.DceMaxConcurrent <= 0 {
		appConfig.DceMaxConcurrent = 4
		log.Printf("DCE MaxConcurrent not set or invalid, defaulting to %d", appConfig.DceMaxConcurrent)
	}
	if appConfig.DceExportFormat == "" || strings.ToLower(appConfig.DceExportFormat) != "json" {
		log.Printf("Warning: dce_export_format is not 'Json' or is empty. Forcing to 'Json' for S-2000 operation.")
		appConfig.DceExportFormat = "Json"
	}

	// --- Phase 1: Execute DCE Commands ---
	log.Println("--- Phase 1: Starting Discord Chat Exporter Tasks ---")
	dceCommandsAndOutputs, err := createDceCommands(appConfig)
	if err != nil {
		log.Fatalf("FATAL: Error creating DCE commands: %v", err)
	}
	if len(dceCommandsAndOutputs) == 0 {
		log.Println("No DCE export tasks generated. Exiting Phase 1.")
	} else {
		var wgDce sync.WaitGroup
		semDce := make(chan struct{}, appConfig.DceMaxConcurrent)
		var successfulJSONFiles []string
		var muFiles sync.Mutex // Mutex to protect successfulJSONFiles slice

		log.Printf("Launching %d DCE export tasks (Concurrency: %d)...", len(dceCommandsAndOutputs), appConfig.DceMaxConcurrent)
		for _, cmdAndOut := range dceCommandsAndOutputs {
			wgDce.Add(1)
			go runDceCommand(cmdAndOut, &wgDce, semDce, &successfulJSONFiles, &muFiles)
		}
		wgDce.Wait()
		log.Println("--- Phase 1: All DCE export tasks completed. ---")
		if len(successfulJSONFiles) == 0 {
			log.Println("No JSON files were successfully exported by DCE. Skipping scraping phase.")
			log.Printf("S-2000 finished in %s.", time.Since(startTime))
			return
		}
		log.Printf("%d JSON files successfully exported by DCE.", len(successfulJSONFiles))

		// --- Phase 2: Scrape JSON Files ---
		log.Println("--- Phase 2: Starting JSON Scraping Tasks ---")
		uniqueEntries := make(map[string]UserRoleEntry)
		var muData sync.Mutex // Mutex for uniqueEntries map
		var wgScrape sync.WaitGroup
		// Concurrency for scraping can be higher, as it's mostly CPU/disk bound locally
		// Let's use number of CPU cores or a reasonable default for scraper concurrency
		// For simplicity, using DceMaxConcurrent here, but ideally could be a separate config.
		scraperConcurrency := appConfig.DceMaxConcurrent * 2
		if scraperConcurrency < 4 {
			scraperConcurrency = 4
		}
		if scraperConcurrency > 16 {
			scraperConcurrency = 16
		}

		semScrape := make(chan struct{}, scraperConcurrency)

		log.Printf("Scraping %d JSON files (Concurrency: %d)...", len(successfulJSONFiles), scraperConcurrency)
		for _, jsonPath := range successfulJSONFiles {
			wgScrape.Add(1)
			go processJSONFileForScraping(jsonPath, uniqueEntries, &muData, &wgScrape, semScrape)
		}
		wgScrape.Wait()
		log.Println("--- Phase 2: All JSON scraping tasks completed. ---")

		// --- Output to CSV ---
		log.Printf("Writing %d unique user-role entries to CSV: %s", len(uniqueEntries), appConfig.FinalCsvOutputPath)
		csvFile, errCsv := os.Create(appConfig.FinalCsvOutputPath)
		if errCsv != nil {
			log.Fatalf("FATAL: Error creating CSV file %s: %v", appConfig.FinalCsvOutputPath, errCsv)
		}
		defer csvFile.Close()
		csvWriter := csv.NewWriter(bufio.NewWriter(csvFile))
		headers := []string{"UserID", "Username", "DisplayName", "RoleID", "RoleName"}
		if errHead := csvWriter.Write(headers); errHead != nil {
			log.Fatalf("FATAL: Error writing CSV header: %v", errHead)
		}
		for _, entry := range uniqueEntries {
			record := []string{entry.UserID, entry.Username, entry.DisplayName, entry.RoleID, entry.RoleName}
			if errWrite := csvWriter.Write(record); errWrite != nil {
				log.Printf("ERROR writing record to CSV: %v", errWrite)
			}
		}
		csvWriter.Flush()
		if errFlush := csvWriter.Error(); errFlush != nil {
			log.Fatalf("FATAL: Error flushing CSV writer: %v", errFlush)
		}
		log.Printf("Successfully wrote data to %s", appConfig.FinalCsvOutputPath)
	}

	log.Printf("S-2000 (Scrapper-2000) finished successfully in %s.", time.Since(startTime))
}

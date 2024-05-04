// Main  package
package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
	"github.com/valyala/fasthttp"
)

const (
	COCApiURL = "https://api.clashofclans.com/v1/players/%s"
)

// Config struct
type Config struct {
	COCApiKeys     []string `env:"COC_API_KEYS" envSeparator:","`
	PlayerTagsFile string   `env:"PLAYER_TAGS_FILE"`
	Workers        int      `env:"WORKERS" envDefault:"4"`
}

// Player struct
type Player struct {
	Tag string `json:"tag"`
}

func loadPlayerTags(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	byteValue, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var players []Player
	err = json.Unmarshal(byteValue, &players)
	if err != nil {
		return nil, err
	}

	var playerTags []string
	for _, player := range players {
		playerTags = append(playerTags, player.Tag)
	}

	return playerTags, nil
}

func getRandomAPIKey(apiKeys []string) string {
	rand.Seed(time.Now().UnixNano())
	return apiKeys[rand.Intn(len(apiKeys))]
}

func fetchPlayerData(workerNumber int, tags <-chan string, wg *sync.WaitGroup, apiKeys []string, successRequestCount *int64, notFoundRequestCount *int64, throttledRequestCount *int64) {
	defer wg.Done()
	client := &fasthttp.Client{
		ReadTimeout:  time.Second * 5,
    WriteTimeout: time.Second * 5,
	}
	processID := os.Getpid()
	log.Printf("Worker %d started with process ID: %d", workerNumber, processID)

	for tag := range tags {
		log.Printf("Worker %d processing tag %s", workerNumber, tag)

		req := fasthttp.AcquireRequest()
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", getRandomAPIKey(apiKeys)))
		req.SetRequestURI(fmt.Sprintf(COCApiURL, tag))

		resp := fasthttp.AcquireResponse()
		err := client.Do(req, resp)
		if err != nil {
			log.Printf("Error with tag %s - %v\n", tag, err)
		}

		switch resp.StatusCode() {
		case 200:
			*successRequestCount++
		case 404:
			*notFoundRequestCount++
		case 429:
			*throttledRequestCount++
		default:
			log.Printf("Worker %d - Tag %s - Status code: %d", workerNumber, tag, resp.StatusCode())
		}

		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
	}
}

func main() {
	// Load .env file
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Parse environment variables and store them in Config struct
	config := Config{}
	err = env.Parse(&config)
	if err != nil {
		log.Fatal(err)
	}

	// Load player tags from file
	playerTags, err := loadPlayerTags(config.PlayerTagsFile)
	if err != nil {
		log.Fatalf("Failed to load player tags: %v", err)
	}
	log.Print("Player tags loaded successfully. Total tags: ", len(playerTags))

	playerTagsChunk := make(chan string, len(playerTags))
	for _, tag := range playerTags {
		playerTagsChunk <- tag
	}
	close(playerTagsChunk)

	workerGroup := &sync.WaitGroup{}
	workerGroup.Add(config.Workers)

	start := time.Now()
	var successRequestCount int64
	var notFoundRequestCount int64
	var throttledRequestCount int64

	for workerNumber := 0; workerNumber < config.Workers; workerNumber++ {
		log.Printf("Starting worker %d", workerNumber)
		go fetchPlayerData(workerNumber, playerTagsChunk, workerGroup, config.COCApiKeys, &successRequestCount, & notFoundRequestCount, &throttledRequestCount)
	}

	workerGroup.Wait()

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	elapsed := time.Since(start)
	log.Printf("Total success requests: %d", successRequestCount)
	log.Printf("Total not found requests: %d", notFoundRequestCount)
	log.Printf("Total throttled requests: %d", throttledRequestCount)
	log.Printf("Total time taken: %s", elapsed)
	log.Printf("Memory usage: %d bytes", memStats.Alloc)
}

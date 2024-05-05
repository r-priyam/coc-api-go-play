// Main  package
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
	jsoniter "github.com/json-iterator/go"
	"github.com/redis/go-redis/v9"
)

const (
	cocAPIURL string = "https://api.clashofclans.com/v1/players/%s"
)

var json = jsoniter.ConfigFastest

// Config struct
type Config struct {
	COCApiKeys     []string `env:"COC_API_KEYS" envSeparator:","`
	PlayerTagsFile string   `env:"PLAYER_TAGS_FILE"`
	RedisURL       string   `env:"REDIS_URL" envDefault:"127.0.0.1:6379"`
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

	byteValue, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var (
		players    []Player
		playerTags []string
	)

	err = json.Unmarshal(byteValue, &players)
	if err != nil {
		return nil, err
	}

	for _, player := range players {
		playerTags = append(playerTags, player.Tag)
	}

	return playerTags, nil
}

func getIncrementalAPIKey(apiKeys []string, apiKeyIndex *int) string {
	// If the index is out of range, reset it to 0
	if len(apiKeys) <= *apiKeyIndex {
		*apiKeyIndex = 0
	}

	apiKey := apiKeys[*apiKeyIndex]

	if *apiKeyIndex+1 >= len(apiKeys) {
		*apiKeyIndex = 0
	} else {
		*apiKeyIndex++
	}

	return apiKey
}

func fetchPlayerData(
	ctx context.Context,
	client *http.Client,
	workerNumber int,
	tags <-chan string,
	wg *sync.WaitGroup,
	redisClient *redis.Client,
	apiKeys []string,
	successRequestCount *int64,
	notFoundRequestCount *int64,
	throttledRequestCount *int64,
) {
	defer wg.Done()
	processID := os.Getpid()
	log.Printf("Worker %d started with process ID: %d", workerNumber, processID)

	apiKeyIndex := workerNumber

	for tag := range tags {
		requestURL := strings.Replace(fmt.Sprintf(cocAPIURL, tag), "#", "%23", 1)
		req, _ := http.NewRequest("GET", requestURL, nil)
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", getIncrementalAPIKey(apiKeys, &apiKeyIndex)))

		resp, err := client.Do(req)
		if err != nil {
			log.Printf("Error with tag %s - %v\n", tag, err)
			continue
		}

		switch resp.StatusCode {
		case http.StatusOK:
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				log.Fatal("Error reading response body: ", err)
			}

			var playerData PlayerStruct
			if err := json.Unmarshal(body, &playerData); err != nil {
				log.Fatal("Error parsing response: ", err)
			}

			playerDataJSON, _ := json.Marshal(playerData)
			_, err = redisClient.Set(ctx, playerData.Tag, playerDataJSON, 0).Result()
			if err != nil {
				log.Printf("Redis error setting data for tag %s: %v", tag, err)
			}

			*successRequestCount++
		case http.StatusNotFound:
			*notFoundRequestCount++
		case http.StatusTooManyRequests:
			*throttledRequestCount++
		default:
			log.Printf("Worker %d - Tag %s - Status code: %d", workerNumber, tag, resp.StatusCode)
		}
	}
}

func main() {
	// Load .env file˝
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
	var (
		ctx                   = context.Background()
		successRequestCount   int64
		notFoundRequestCount  int64
		throttledRequestCount int64
	)

	redis := redis.NewClient(&redis.Options{
		Addr: config.RedisURL,
		DB:   0,
	})

	client := &http.Client{
		Timeout: time.Second * 10,
	}

	for workerNumber := 0; workerNumber < config.Workers; workerNumber++ {
		log.Printf("Starting worker %d", workerNumber)
		go fetchPlayerData(
			ctx,
			client,
			workerNumber,
			playerTagsChunk,
			workerGroup,
			redis,
			config.COCApiKeys,
			&successRequestCount,
			&notFoundRequestCount,
			&throttledRequestCount,
		)
	}

	workerGroup.Wait()

	elapsed := time.Since(start)
	log.Printf("Total success requests: %d", successRequestCount)
	log.Printf("Total not found requests: %d", notFoundRequestCount)
	log.Printf("Total throttled requests: %d", throttledRequestCount)
	log.Printf("Total time taken: %s", elapsed)
}

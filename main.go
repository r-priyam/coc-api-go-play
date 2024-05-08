// Main  package
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"runtime/pprof"
	"runtime/trace"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/goccy/go-json"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	cocAPIURL string = "https://api.clashofclans.com/v1/players/%s"
)

// Config struct
type Config struct {
	COCApiKeys     []string `env:"COC_API_KEYS" envSeparator:","`
	EnableCPUPprof bool     `env:"ENABLE_CPU_PPROF" envDefault:"false"`
	EnableTrace    bool     `env:"ENABLE_TRACE" envDefault:"false"`
	PlayerTagsFile string   `env:"PLAYER_TAGS_FILE"`
	RedisURL       string   `env:"REDIS_URL" envDefault:"127.0.0.1:6379"`
	Workers        int      `env:"WORKERS" envDefault:"4"`
	MongoDbURL     string   `env:"MONGODB_URL"`
}

// Player struct
type Player struct {
	Tag string `json:"tag"`
}

func loadPlayerTagChunks(filePath string) ([][]string, error) {
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
		players         []Player
		playerTags      []string
		playerTagChunks [][]string
	)

	const chunkSize = 50000

	err = json.Unmarshal(byteValue, &players)
	if err != nil {
		return nil, err
	}

	for _, player := range players {
		playerTags = append(playerTags, player.Tag)

		if len(playerTags) == chunkSize {
			playerTagChunks = append(playerTagChunks, playerTags)
			playerTags = []string{}
		}
	}

	if len(playerTags) > 0 {
		playerTagChunks = append(playerTagChunks, playerTags)
	}

	return playerTagChunks, nil
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
	mongoClient *mongo.Client,
	apiKeys []string,
	successRequestCount *int64,
	notFoundRequestCount *int64,
	throttledRequestCount *int64,
) {
	defer wg.Done()
	processID := os.Getpid()
	log.Printf("Worker %d started with process ID: %d", workerNumber, processID)

	models := []mongo.WriteModel{}
	apiKeyIndex := workerNumber

	for tag := range tags {
		requestURL := fmt.Sprintf(cocAPIURL, url.QueryEscape(tag))
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

			val, err := redisClient.Get(ctx, playerData.Tag).Result()
			if err != nil && err != redis.Nil {
				log.Printf("Redis error getting data for tag %s: %v", tag, err)
			}

			if val != "" && playerData.Trophies >= 5000 {
				var cached PlayerStruct
				if err := json.Unmarshal([]byte(val), &cached); err != nil {
					log.Fatal("Error parsing response: ", err)
				}

				if cached.Trophies != playerData.Trophies {
					_setOnInsert := bson.D{
						{Key: "initial", Value: cached.Trophies},
						{Key: "final", Value: cached.Trophies},
					}
					_set := bson.D{
						{Key: "name", Value: playerData.Name},
						{Key: "trophies", Value: playerData.Trophies},
					}

					_push := bson.D{
						{
							Key: "attacks",
							Value: bson.D{
								{Key: "timestamp", Value: time.Now().UnixMilli()},
								{Key: "initial", Value: cached.Trophies},
								{Key: "last", Value: playerData.Trophies},
								{Key: "gain", Value: playerData.Trophies - cached.Trophies},
							},
						},
					}

					models = append(
						models,
						mongo.NewUpdateOneModel().
							SetFilter(bson.D{{Key: "tag", Value: cached.Tag}}).
							SetUpdate(
								bson.D{
									{Key: "$set", Value: _set},
									{Key: "$push", Value: _push},
									{Key: "$setOnInsert", Value: _setOnInsert},
								},
							).
							SetUpsert(true),
					)
					log.Printf("Player %v made a new attack. From %v to %v (%v) [%v]", playerData.Name, cached.Trophies, playerData.Trophies, playerData.Trophies-cached.Trophies, playerData.AttackWins-cached.AttackWins)
				}
			}

			playerDataJSON, _ := json.Marshal(playerData)
			err = redisClient.Set(ctx, playerData.Tag, playerDataJSON, 0).Err()
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

	mongoStart := time.Now()
	log.Printf("Bulk inserting %v items", len(models))

	if len(models) > 0 {
		opts := options.BulkWrite().SetOrdered(false)
		collection := mongoClient.Database("db").Collection("legend_attacks")

		_, bulkErr := collection.BulkWrite(context.TODO(), models, opts)

		if bulkErr != nil {
			log.Fatal(bulkErr)
		}
		log.Printf("Bulk inserted in %v", time.Since(mongoStart))
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

	if config.EnableCPUPprof {
		f, err := os.Create("profile.prof")
		if err != nil {
			panic(err)
		}
		defer f.Close()

		if err := pprof.StartCPUProfile(f); err != nil {
			panic(err)
		}
		defer pprof.StopCPUProfile()
	}

	if config.EnableTrace {
		traceFile, err := os.Create("trace.out")
		if err != nil {
			panic(err)
		}
		defer traceFile.Close()

		if err := trace.Start(traceFile); err != nil {
			panic(err)
		}
		defer trace.Stop()
	}

	// Load player tags from file
	playerTagChunks, err := loadPlayerTagChunks(config.PlayerTagsFile)
	if err != nil {
		log.Fatalf("Failed to load player tags: %v", err)
	}
	log.Print("Player tags loaded successfully.")

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
	mongo := MongoClient(config.MongoDbURL)

	client := &http.Client{
		Timeout: time.Second * 10,
	}

	workerGroup := &sync.WaitGroup{}
	for _, playerTagChunk := range playerTagChunks {
		workerGroup.Add(config.Workers)

		playerTagsChunk := make(chan string, len(playerTagChunk))
		for _, tag := range playerTagChunk {
			playerTagsChunk <- tag
		}
		close(playerTagsChunk)

		for workerNumber := 0; workerNumber < config.Workers; workerNumber++ {
			log.Printf("Starting worker %d", workerNumber)
			go fetchPlayerData(
				ctx,
				client,
				workerNumber,
				playerTagsChunk,
				workerGroup,
				redis,
				mongo,
				config.COCApiKeys,
				&successRequestCount,
				&notFoundRequestCount,
				&throttledRequestCount,
			)
		}

		workerGroup.Wait()
	}

	elapsed := time.Since(start)
	log.Printf("Total success requests: %d", successRequestCount)
	log.Printf("Total not found requests: %d", notFoundRequestCount)
	log.Printf("Total throttled requests: %d", throttledRequestCount)
	log.Printf("Total time taken: %s", elapsed)
}

package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/websocket"
	"github.com/serialx/hashring"
)

const pubReportPeriod = 2 * time.Second
const recvReportPeriod = 2 * time.Second

var upgrader = websocket.Upgrader{}
var publishingChannels map[string](chan string)
var rescvChannels map[string](chan []byte)
var rescvChannelsMapLock sync.RWMutex
var redisPubSubClients map[string]*redis.PubSubConn
var playerToSocketMap map[string]*websocket.Conn
var mapLock sync.RWMutex
var redisLock sync.RWMutex
var redisPubSubLock sync.RWMutex
var ring *hashring.HashRing
var lastPubStatTime = time.Now()
var statPubProcessedMessages uint32
var lastRecvStatTime = time.Now()
var statRecvProcessedMessages uint32

func clearPlayer(playerID, channel string, c *websocket.Conn) {
	redisPubSubLock.Lock()
	redisPubSub := getRedisPubSubForPlayer(playerID)
	redisPubSub.Unsubscribe(channel)
	redisPubSubLock.Unlock()
	c.Close()
	removePlayerSocket(playerID)
	removePlayerRescvChannel(playerID)
}

func setPlayerRescvChannel(playerID string, c chan []byte) {
	rescvChannelsMapLock.Lock()
	defer rescvChannelsMapLock.Unlock()
	rescvChannels[playerID] = c
}

func getPlayerRescvChannel(playerID string) (chan []byte, bool) {
	rescvChannelsMapLock.RLock()
	defer rescvChannelsMapLock.RUnlock()
	c, ok := rescvChannels[playerID]
	return c, ok
}

func removePlayerRescvChannel(playerID string) {
	rescvChannelsMapLock.Lock()
	defer rescvChannelsMapLock.Unlock()
	channel := rescvChannels[playerID]
	delete(rescvChannels, playerID)
	close(channel)
}

func setPlayerSocket(playerID string, c *websocket.Conn) {
	mapLock.Lock()
	defer mapLock.Unlock()
	playerToSocketMap[playerID] = c
}

func getPlayerSocket(playerID string) (*websocket.Conn, bool) {
	mapLock.RLock()
	defer mapLock.RUnlock()
	c, ok := playerToSocketMap[playerID]
	return c, ok
}

func removePlayerSocket(playerID string) {
	mapLock.Lock()
	defer mapLock.Unlock()
	delete(playerToSocketMap, playerID)
}

func getPublishingChannelForPlayer(playerID string) chan string {
	server, _ := ring.GetNode(playerID)
	return publishingChannels[server]
}

func getRedisPubSubForPlayer(playerID string) *redis.PubSubConn {
	server, _ := ring.GetNode(playerID)
	return redisPubSubClients[server]
}

func reportThroughput(tag string, lastStatTime *time.Time, msgCount *uint32, period time.Duration) {
	if elapsed := time.Now().Sub(*lastStatTime); elapsed > period {
		log.Printf("%s rate: %.2f msg/sec", tag, float32(*msgCount)/float32(elapsed/1000000000))
		*lastStatTime = time.Now()
		*msgCount = 0
	}
}

func publishToEnemy(message []byte) {
	stringMsg := string(message)
	parts := strings.Split(stringMsg, ",")
	enemyID := parts[0]
	channel := getPublishingChannelForPlayer(enemyID)
	select {
	case channel <- stringMsg:
	default:
		fmt.Println("channel full. discarding message")
	}

}

func publisher(redisURL string, pendingPublish <-chan string) {
	minBatchSize, err := strconv.ParseInt(getEnv("MIN_BATCH_SIZE", "10"), 10, 64)
	maxBatchSize, err := strconv.ParseInt(getEnv("MAX_BATCH_SIZE", "100"), 10, 64)
	maxLatencyInMs, err := strconv.ParseUint(getEnv("MAX_BATCH_WAIT_MS", "10"), 10, 64)
	buffer := []string{}
	redisClient, err := redis.DialURL(redisURL)
	if err != nil {
		log.Println(err.Error())
	}
	defer redisClient.Close()
	d := time.Duration(maxLatencyInMs) * time.Millisecond

	flushTimeout := make(chan bool, 1)
	timing := false
	for {
		select {
		case <-flushTimeout:
			// time to flush!
			timing = false
			if len(buffer) < 1 {
				break
			}
			flushRedisConnection(redisClient, &buffer)
		case m := <-pendingPublish: // channel too big!
			buffer = append(buffer, m)
			// fetch up to 100 messages
			for i := 0; i < 100; i++ {
				if len(buffer) > int(maxBatchSize) {
					break
				}
				select {
				case m := <-pendingPublish:
					buffer = append(buffer, m)
				default:
					break
				}
			}
			if len(buffer) < int(minBatchSize) {
				// set a timer to garantee a delivery in less than d
				if !timing {
					timing = true
					go func() {
						time.Sleep(d)
						flushTimeout <- true
					}()
				}
				break
			}
			flushRedisConnection(redisClient, &buffer)

		}
	}
}

func flushRedisConnection(c redis.Conn, buffer *[]string) {
	//log.Printf("flushing %d msg\n", len(*buffer))
	for _, message := range *buffer {
		parts := strings.Split(string(message), ",")
		enemyID := parts[0]
		channel := fmt.Sprintf("%s.messages", enemyID)
		c.Send("PUBLISH", channel, message)
	}
	c.Flush()
	atomic.AddUint32(&statPubProcessedMessages, uint32(len(*buffer)))
	reportThroughput("pub", &lastPubStatTime, &statPubProcessedMessages, pubReportPeriod)
	*buffer = (*buffer)[:0]
}

func sendMessagesToPlayer(in <-chan []byte, c *websocket.Conn) {
	for m := range in {
		c.WriteMessage(websocket.TextMessage, m)
		atomic.AddUint32(&statRecvProcessedMessages, 1)
		reportThroughput("recv", &lastRecvStatTime, &statRecvProcessedMessages, recvReportPeriod)
	}
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	playerID := r.RequestURI[1:]
	channel := fmt.Sprintf("%s.messages", playerID)

	maxRecvInflight, err := strconv.ParseUint(getEnv("MAX_RESCV_INFLIGHT", "1000"), 10, 64)
	rescvChannel := make(chan []byte, maxRecvInflight)
	setPlayerRescvChannel(playerID, rescvChannel)

	redisPubSubLock.Lock()
	redisPubSub := getRedisPubSubForPlayer(playerID)
	redisPubSub.Subscribe(channel)
	redisPubSubLock.Unlock()
	c, err := upgrader.Upgrade(w, r, nil)

	if err != nil {
		return
	}
	defer clearPlayer(playerID, channel, c)

	log.Printf("new connection player id: %s", playerID)
	setPlayerSocket(playerID, c)

	go sendMessagesToPlayer(rescvChannel, c)

	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			break
		}
		publishToEnemy(message)
	}
}

func receiveFromRedis(redisURL string) {
	redisPubSubClient, err := redis.DialURL(redisURL)
	if err != nil {
		log.Println(err.Error())
	}
	defer redisPubSubClient.Close()

	redisPubSub := &redis.PubSubConn{Conn: redisPubSubClient}
	redisPubSubLock.Lock()
	redisPubSubClients[redisURL] = redisPubSub
	redisPubSubLock.Unlock()

	for {
		switch v := redisPubSub.Receive().(type) {
		case redis.Message:
			parts := strings.Split(string(v.Data), ",")
			enemyID := parts[0]

			if enemyChannel, ok := getPlayerRescvChannel(enemyID); ok {
				enemyChannel <- v.Data
			} else {
				log.Printf("enemyId is %s\n", enemyID)
			}
		case error:
			log.Printf(v.Error())
		}
	}
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}

func main() {
	playerToSocketMap = map[string]*websocket.Conn{}
	publishingChannels = make(map[string](chan string))
	rescvChannels = make(map[string](chan []byte))
	redisPubSubClients = map[string]*redis.PubSubConn{}
	http.HandleFunc("/", wsHandler)
	var err error
	redisURLs := strings.Split(os.Getenv("REDIS_URL"), ",")
	maxInflight, err := strconv.ParseUint(getEnv("MAX_INFLIGHT", "10000"), 10, 64)
	ring = hashring.New(redisURLs)
	for _, redisURL := range redisURLs {
		// one goroutine publishing per redis connection
		// capacity 1000 is enough, will flush every ~5ms, batch should be
		// in the tenths of messages.
		publishChannel := make(chan string, maxInflight)
		publishingChannels[redisURL] = publishChannel
		defer close(publishChannel)
		go publisher(redisURL, publishChannel)
		go receiveFromRedis(redisURL)
	}
	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		panic(fmt.Sprintf("ListenAndServe: %s", err.Error()))
	}
}

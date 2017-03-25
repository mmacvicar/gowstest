package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/websocket"
	"github.com/serialx/hashring"
)

var upgrader = websocket.Upgrader{}
var redisClients map[string]redis.Conn
var redisPubSubClients map[string]*redis.PubSubConn
var playerToSocketMap map[string]*websocket.Conn
var lock sync.RWMutex
var redisLock sync.RWMutex
var redisPubSubLock sync.RWMutex
var ring *hashring.HashRing

func clearPlayer(playerID, channel string, c *websocket.Conn) {
	redisPubSubLock.Lock()
	redisPubSub := getRedisPubSubForPlayer(playerID)
	redisPubSub.Unsubscribe(channel)
	redisPubSubLock.Unlock()
	c.Close()
	lock.Lock()
	delete(playerToSocketMap, playerID)
	lock.Unlock()
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	playerID := r.RequestURI[1:]
	channel := fmt.Sprintf("%s.messages", playerID)
	redisPubSubLock.Lock()
	redisPubSub := getRedisPubSubForPlayer(playerID)
	redisPubSub.Subscribe(channel)
	redisPubSubLock.Unlock()
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer clearPlayer(playerID, channel, c)
	lock.Lock()
	log.Printf("the player id: %s", playerID)
	playerToSocketMap[playerID] = c
	lock.Unlock()

	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			break
		}
		redisLock.Lock()
		redisClient := getRedisForPlayer(playerID)
		_, err = redisClient.Do("PUBLISH", channel, message)
		redisLock.Unlock()
		if err != nil {
			log.Printf(err.Error())
			break
		}
	}
}

func getRedisForPlayer(playerID string) redis.Conn {
	server, _ := ring.GetNode(playerID)
	return redisClients[server]
}

func getRedisPubSubForPlayer(playerID string) *redis.PubSubConn {
	server, _ := ring.GetNode(playerID)
	return redisPubSubClients[server]
}

func receiveFromRedis(redisURL string) {
	redisPubSubClient, err := redis.DialURL(redisURL)
	if err != nil {
		log.Println(err.Error())
	}
	defer redisPubSubClient.Close()
	redisPubSub := &redis.PubSubConn{Conn: redisPubSubClient}
	redisPubSubClients[redisURL] = redisPubSub
	for {
		switch v := redisPubSub.Receive().(type) {
		case redis.Message:
			parts := strings.Split(string(v.Data), ",")
			enemyID := parts[0]
			lock.RLock()
			if enemySocket, ok := playerToSocketMap[enemyID]; ok {
				enemySocket.WriteMessage(websocket.TextMessage, v.Data)
			} else {
				// log.Printf("enemyId is %s\n", enemyID)
			}
			lock.RUnlock()
		case error:
			log.Printf(v.Error())
		}
	}
}

func main() {
	playerToSocketMap = map[string]*websocket.Conn{}
	redisClients = map[string]redis.Conn{}
	redisPubSubClients = map[string]*redis.PubSubConn{}
	http.HandleFunc("/", wsHandler)
	var err error
	redisURLs := strings.Split(os.Getenv("REDIS_URL"), ",")
	ring = hashring.New(redisURLs)
	for _, redisURL := range redisURLs {
		go receiveFromRedis(redisURL)
		redisClient, err := redis.DialURL(redisURL)
		if err != nil {
			log.Println(err.Error())
		}
		redisClients[redisURL] = redisClient
		defer redisClient.Close()
	}
	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		panic(fmt.Sprintf("ListenAndServe: %s", err.Error()))
	}
}

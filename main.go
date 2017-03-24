package main

import (
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/websocket"
	"log"
	"net/http"
	"os"
	"sync"
)

/*
   route: 'player.message',
   args: {
     playerId: randomUUID,
     msg: 'andre presunto',
   },
*/

var upgrader = websocket.Upgrader{}
var redisPubSubClient redis.Conn
var redisPubSub *redis.PubSubConn
var playerToSocketMap map[string]*websocket.Conn
var redisClient redis.Conn
var lock sync.RWMutex
var redisLock sync.RWMutex
var redisPubSubLock sync.RWMutex

type msg struct {
	route string
	args  map[string]string
}

func clearPlayer(playerID, channel string, c *websocket.Conn) {
	redisPubSubLock.Lock()
	redisPubSub.Unsubscribe(channel)
	redisPubSubLock.Unlock()
	c.Close()
	lock.Lock()
	delete(playerToSocketMap, playerID)
	lock.Unlock()
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	userID := r.RequestURI[1:]
	channel := fmt.Sprintf("%s.messages", userID)
	redisPubSubLock.Lock()
	redisPubSub.Subscribe(channel)
	redisPubSubLock.Unlock()
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer clearPlayer(userID, channel, c)
	lock.Lock()
	playerToSocketMap[userID] = c
	lock.Unlock()

	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			break
		}
		redisLock.Lock()
		_, err = redisClient.Do("PUBLISH", channel, message)
		redisLock.Unlock()
		if err != nil {
			log.Printf(err.Error())
			break
		}
	}
}

func receiveFromRedis() {
	var err error
	redisURL := os.Getenv("REDIS_URL")
	redisPubSubClient, err = redis.DialURL(redisURL)
	if err != nil {
		log.Println(err.Error())
	}
	defer redisPubSubClient.Close()
	redisPubSub = &redis.PubSubConn{Conn: redisPubSubClient}
	for {
		switch v := redisPubSub.Receive().(type) {
		case redis.Message:
			var playerMessage msg
			json.Unmarshal(v.Data, &playerMessage)
			enemyID := playerMessage.args["playerID"]
			lock.RLock()
			if enemySocket, ok := playerToSocketMap[enemyID]; ok {
				enemySocket.WriteMessage(websocket.TextMessage, v.Data)
			}
			lock.RUnlock()
		case error:
			log.Printf(v.Error())
		}
	}
}

func main() {
	playerToSocketMap = map[string]*websocket.Conn{}
	http.HandleFunc("/", wsHandler)
	var err error
	redisURL := os.Getenv("REDIS_URL")
	redisClient, err = redis.DialURL(redisURL)
	if err != nil {
		log.Println(err.Error())
	}
	defer redisClient.Close()
	go receiveFromRedis()
	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		panic(fmt.Sprintf("ListenAndServe: %s", err.Error()))
	}
}

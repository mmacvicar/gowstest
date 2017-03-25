package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/nats-io/go-nats"
)

var natsConn *nats.Conn
var upgrader = websocket.Upgrader{}
var playerToSocketMap map[string]*websocket.Conn
var lock sync.RWMutex

//var redisClients map[string]redis.Conn
//var redisPubSubClients map[string]*redis.PubSubConn
//var redisLock sync.RWMutex
//var redisPubSubLock sync.RWMutex
//var ring *hashring.HashRing

func clearPlayer(playerID, channel string, c *websocket.Conn, sub *nats.Subscription) {
	log.Printf("Unsubscribing to %s\n", channel)
	sub.Unsubscribe()
	c.Close()
	lock.Lock()
	delete(playerToSocketMap, playerID)
	lock.Unlock()
	log.Printf("Player %s has disconnected.\n", playerID)
}

func publishToSocket(playerID string, msg []byte) {
	lock.RLock()
	if s, ok := playerToSocketMap[playerID]; ok {
		s.WriteMessage(websocket.TextMessage, msg)
		log.Printf("Sent message to player %s: %s\n", playerID, string(msg))
	} else {
		log.Printf("Could not find player with id %s. Ignoring message...\n", playerID)
	}
	lock.RUnlock()
}

func publishToEnemy(message []byte) {
	parts := strings.Split(string(message), ",")
	enemyID := parts[0]
	channel := fmt.Sprintf("%s.messages", enemyID)
	//log.Printf("Sending message to %s...\n", channel)
	natsConn.Publish(channel, message)
}

func subscribeToPlayerMessages(playerID string, c *websocket.Conn) *nats.Subscription {
	channel := fmt.Sprintf("%s.messages", playerID)
	log.Printf("Subscribing to %s\n", channel)
	sub, err := natsConn.Subscribe(channel, func(m *nats.Msg) {
		log.Printf("Received message for player %s: %s\n", playerID, string(m.Data))
		publishToSocket(playerID, m.Data)
	})
	if err != nil {
		panic(err)
	}
	return sub
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	playerID := r.RequestURI[1:]
	channel := fmt.Sprintf("%s.messages", playerID)
	//redisPubSubLock.Lock()
	//redisPubSub := getRedisPubSubForPlayer(playerID)
	//redisPubSub.Subscribe(channel)
	//redisPubSubLock.Unlock()
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	sub := subscribeToPlayerMessages(playerID, c)
	defer clearPlayer(playerID, channel, c, sub)

	lock.Lock()
	log.Printf("the player id: %s", playerID)
	playerToSocketMap[playerID] = c
	lock.Unlock()

	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			break
		}
		publishToEnemy(message)
		//redisLock.Lock()
		//redisClient := getRedisForPlayer(playerID)
		//_, err = redisClient.Do("PUBLISH", channel, message)
		//redisLock.Unlock()
		//if err != nil {
		//log.Printf(err.Error())
		//break
		//}
	}
}

//func getRedisForPlayer(playerID string) redis.Conn {
//server, _ := ring.GetNode(playerID)
//return redisClients[server]
//}

//func getRedisPubSubForPlayer(playerID string) *redis.PubSubConn {
//server, _ := ring.GetNode(playerID)
//return redisPubSubClients[server]
//}

//func receiveFromRedis(redisURL string) {
//redisPubSubClient, err := redis.DialURL(redisURL)
//if err != nil {
//log.Println(err.Error())
//}
//defer redisPubSubClient.Close()
//redisPubSub := &redis.PubSubConn{Conn: redisPubSubClient}
//redisPubSubClients[redisURL] = redisPubSub
//for {
//switch v := redisPubSub.Receive().(type) {
//case redis.Message:
//parts := strings.Split(string(v.Data), ",")
//enemyID := parts[0]
//lock.RLock()
//if enemySocket, ok := playerToSocketMap[enemyID]; ok {
//enemySocket.WriteMessage(websocket.TextMessage, v.Data)
//} else {
//// log.Printf("enemyId is %s\n", enemyID)
//}
//lock.RUnlock()
//case error:
//log.Printf(v.Error())
//}
//}
//}

func connectNats() {
	var err error
	natsURLs := os.Getenv("NATS_URL")
	natsConn, err = nats.Connect(natsURLs)
	if err != nil {
		panic("Could not connect to nats")
	}
}

func main() {
	connectNats()

	playerToSocketMap = map[string]*websocket.Conn{}
	//redisClients = map[string]redis.Conn{}
	//redisPubSubClients = map[string]*redis.PubSubConn{}
	http.HandleFunc("/", wsHandler)
	//var err error
	//redisURLs := strings.Split(os.Getenv("REDIS_URL"), ",")
	//ring = hashring.New(redisURLs)
	//for _, redisURL := range redisURLs {
	//go receiveFromRedis(redisURL)
	//redisClient, err := redis.DialURL(redisURL)
	//if err != nil {
	//log.Println(err.Error())
	//}
	//redisClients[redisURL] = redisClient
	//defer redisClient.Close()
	//}
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		panic(fmt.Sprintf("ListenAndServe: %s", err.Error()))
	}
}

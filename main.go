package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"

	//	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/websocket"
	"github.com/nsqio/go-nsq"
)

var upgrader = websocket.Upgrader{}
var playerToSocketMap map[string]*websocket.Conn
var playerToSocketMapLock sync.RWMutex
var nsqProducer *nsq.Producer

func clearPlayer(playerID, channel string, c *websocket.Conn) {
	c.Close()
	playerToSocketMapLock.Lock()
	delete(playerToSocketMap, playerID)
	playerToSocketMapLock.Unlock()
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	playerID := r.RequestURI[1:]
	topic := fmt.Sprintf("%s.messages", playerID)
	nsqConfig := nsq.NewConfig()
	nsqConfig.Set("max_in_flight", 50)
	nsqConsumer, err := nsq.NewConsumer(topic, "messages", nsqConfig)
	nsqConsumer.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
		parts := strings.Split(string(m.Body), ",")
		enemyID := parts[0]
		playerToSocketMapLock.RLock()
		if enemySocket, ok := playerToSocketMap[enemyID]; ok {
			enemySocket.WriteMessage(websocket.TextMessage, m.Body)
		} else {
			// log.Printf("enemyId is %s\n", enemyID)
		}
		playerToSocketMapLock.RUnlock()
		return nil
	}))
	nsqlookupds := os.Getenv("NSQLOOKUPDS")
	nsqConsumer.ConnectToNSQLookupds(strings.Split(nsqlookupds, ","))
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	playerToSocketMapLock.Lock()
	playerToSocketMap[playerID] = c
	playerToSocketMapLock.Unlock()

	defer nsqConsumer.Stop()

	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			break
		}
		parts := strings.Split(string(message), ",")
		enemyID := parts[0]
		enemyTopic := fmt.Sprintf("%s.messages", enemyID)
		err = nsqProducer.Publish(enemyTopic, message)
		if err != nil {
			log.Printf(err.Error())
			break
		}
	}
}

func main() {
	nsqdAddr := os.Getenv("NSQD_ADDR")
	nsqConfig := nsq.NewConfig()
	nsqConfig.Set("max_in_flight", 200)
	var err error
	nsqProducer, err = nsq.NewProducer(nsqdAddr, nsqConfig)
	defer nsqProducer.Stop()
	playerToSocketMap = map[string]*websocket.Conn{}
	http.HandleFunc("/", wsHandler)
	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		panic(fmt.Sprintf("ListenAndServe: %s", err.Error()))
	}
}

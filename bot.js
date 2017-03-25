const WebSocket = require('ws')
const fs = require('fs')

const serverAddr = process.env.SERVER_ADDR || 'wss://sniper3d-pvp.fungames-forfree.com'
const numBots = process.env.NUM_BOTS || 800
let intervalBetweenMessages = process.env.INTERVAL_BETWEEN_MESSAGES || 60
intervalBetweenMessages = parseInt(intervalBetweenMessages, 10)
let botId = process.env.BOT_ID || 0
botId = parseInt(botId, 10)

const base = 5000 / (5000 / numBots)

const text = fs.readFileSync('ids').toString('utf-8')
const ids = text.split('\n')

const log = (id, message) => {
  console.log(id, message)
}

const sendMessage = (ws) => {
  const randomUUID = ids[Math.floor(Math.random() * (ids.length - 1)) + 1]
  ws.send(`${randomUUID},3.14,5.12,6.55,4.44,7.77`)
}

for (let i = 0; i < numBots; i += 1) {
  const uuid = ids[(base * botId) + i]
  const ws = new WebSocket(`${serverAddr}/${uuid}`)
  ws.on('open', () => {
    log(i, `connection open ${uuid}`)
    setInterval(() => { sendMessage(ws) }, intervalBetweenMessages)
  })

  ws.on('message', (data) => {
    log(i, `message received: ${data}`)
  })
}

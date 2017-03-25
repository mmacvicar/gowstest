deps:
	@docker-compose -p gowstest up -d

deps-stop:
	@docker-compose -p gowstest stop
	@docker-compose -p gowstest rm -f

run:
	@NATS_URL=nats://localhost:4222,nats://localhost:4223,nats://localhost:4224 go run main.go

load:
	@SERVER_ADDR=ws://localhost:8080 NUM_BOTS=200 node bot.js

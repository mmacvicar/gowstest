deps:
	@docker-compose -p gowstest up -d

deps-stop:
	@docker-compose -p gowstest stop
	@docker-compose -p gowstest rm -f

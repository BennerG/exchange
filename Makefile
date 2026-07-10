.PHONY: up run-producer run-matcher run-settler dev

up:
	docker-compose up -d

run-producer:
	go run ./cmd/producer

run-matcher:
	go run ./cmd/matcher

run-settler:
	go run ./cmd/settler

dev: up
	$(MAKE) -j3 run-producer run-matcher run-settler

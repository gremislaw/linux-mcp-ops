.PHONY: kafka-up kafka-down kafka-topics kafka-test test build

kafka-up:
	docker-compose up -d

kafka-down:
	docker-compose down -v

kafka-topics:
	docker exec redops-kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 --describe

kafka-produce:
	docker exec -it redops-kafka /opt/kafka/bin/kafka-console-producer.sh \
		--bootstrap-server localhost:9092 --topic tg.requests

kafka-consume:
	docker exec -it redops-kafka /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 --topic tg.requests --from-beginning

test:
	go test ./...

build:
	go build -o bin/kafka-smoke ./cmd/kafka-smoke

smoke-produce:
	./bin/kafka-smoke -mode produce

smoke-consume:
	./bin/kafka-smoke -mode consume

# gorabbit — library targets and the end-to-end harness.

SCENARIO ?= all
E2E_DIR  := e2e
COMPOSE  := docker compose -f $(E2E_DIR)/docker-compose.yml
RUNNER   := cd $(E2E_DIR) && ./bin/runner -scenario $(SCENARIO)

.PHONY: test build vet fmt e2e e2e-up e2e-down e2e-build e2e-run e2e-keep e2e-kill e2e-list e2e-logs

test:
	go test -count=1 ./...

build:
	go build ./...

vet:
	go vet ./...

fmt:
	gofmt -l .

## e2e runs the whole harness: infrastructure up, scripts, infrastructure down.
e2e: e2e-kill e2e-up e2e-build
	@set +e; ( $(RUNNER) ); status=$$?; $(COMPOSE) down -v; exit $$status

## e2e-keep leaves the broker, redis and the last script's processes running.
e2e-keep: e2e-up e2e-build
	$(RUNNER) -keep
	@echo
	@echo "environment left up: management http://127.0.0.1:15673 (guest/guest), redis 127.0.0.1:6380"
	@echo "tear it all down with: make e2e-down"

e2e-up:
	$(COMPOSE) up -d --wait

e2e-down: e2e-kill
	$(COMPOSE) down -v

## e2e-kill stops harness processes a previous e2e-keep left behind.
e2e-kill:
	-@pkill -f "bin/(publisher|consumer) -amqp amqp://" 2>/dev/null; true

e2e-build:
	cd $(E2E_DIR) && go build -o bin/publisher ./cmd/publisher
	cd $(E2E_DIR) && go build -o bin/consumer ./cmd/consumer
	cd $(E2E_DIR) && go build -o bin/runner ./cmd/runner

## e2e-run needs the infrastructure already up; SCENARIO picks one script.
e2e-run: e2e-kill e2e-build
	$(RUNNER)

e2e-list: e2e-build
	cd $(E2E_DIR) && ./bin/runner -list

e2e-logs:
	$(COMPOSE) logs -f

# SPDX-FileCopyrightText: 2021 FerretDB Inc.
#
# SPDX-FileCopyrightText: 2022 SAP SE or an SAP affiliate company
#
# SPDX-License-Identifier: Apache-2.0

all: fmt test

help:                                  ## Display this help message
	@echo "Please use \`make <target>\` where <target> is one of:"
	@grep '^[a-zA-Z]' $(MAKEFILE_LIST) | \
		awk -F ':.*?## ' 'NF==2 {printf "  %-26s%s\n", $$1, $$2}'

env-up: env-up-detach env-setup        ## Start development environment
	docker-compose logs --follow

env-up-detach:
	docker-compose up --always-recreate-deps --force-recreate --remove-orphans --renew-anon-volumes --detach

env-setup: gen-version
	go run ./cmd/envtool/main.go

env-pull:
	docker-compose pull --include-deps --quiet

env-down:                              ## Stop development environment
	docker-compose down --remove-orphans

init: gen-version                      ## Install development tools
	go mod tidy
	cd tools && go mod tidy
	go mod verify
	cd tools && go generate -tags=tools -x

gen: bin/gofumpt                       ## Generate code
	go generate -x ./...
	$(MAKE) fmt

gen-version:
	go generate -x ./internal/util/version

fmt: bin/gofumpt                       ## Format code
	bin/gofumpt -w .

test:                                  ## Run tests
	go test -race -shuffle=on -coverprofile=cover.txt -coverpkg=./... ./...
	go test -race -shuffle=on -bench=. -benchtime=1x ./...

# That's not quite correct: https://github.com/golang/go/issues/15513
# But good enough for us.
fuzz-init: gen-version
	go test -count=0 ./...

fuzz-short:                            ## Fuzz for 1 minute
	go test -list='Fuzz.*' ./...
	go test -fuzz=FuzzArray -fuzztime=1m ./internal/bson/
	go test -fuzz=FuzzDocument -fuzztime=1m ./internal/bson/
	go test -fuzz=FuzzArray -fuzztime=1m ./internal/fjson/
	go test -fuzz=FuzzDocument -fuzztime=1m ./internal/fjson/
	go test -fuzz=FuzzMsg -fuzztime=1m ./internal/wire/
	go test -fuzz=FuzzQuery -fuzztime=1m ./internal/wire/
	go test -fuzz=FuzzReply -fuzztime=1m ./internal/wire/

bench-short:                           ## Benchmark for 5 seconds
	go test -list='Benchmark.*' ./...
	rm -f new.txt
	go test -bench=BenchmarkArray    -benchtime=5s ./internal/bson/  | tee -a new.txt
	go test -bench=BenchmarkDocument -benchtime=5s ./internal/bson/  | tee -a new.txt
	go test -bench=BenchmarkArray    -benchtime=5s ./internal/fjson/ | tee -a new.txt
	go test -bench=BenchmarkDocument -benchtime=5s ./internal/fjson/ | tee -a new.txt
	bin/benchstat old.txt new.txt

build-testcover: gen-version           ## Build bin/SAPHANAcompatibilitylayer-testcover
	go test -c -o=bin/SAPHANAcompatibilitylayer-testcover -trimpath -tags=testcover -race -coverpkg=./... ./cmd/SAPHANACompatibilityLayer

# Default value for TLS if not given
TLS := false

run: build-testcover                   ## Run SAP HANA compatibility layer for MongoDB Wire Protocol with following flags: HANAConnectString, TLS, certFile, keyFile
	bin/SAPHANAcompatibilitylayer-testcover -test.coverprofile=cover.txt -mode=normal -listen-addr=:27017 -HANAConnectString=$(HANAConnectString) -tls=$(TLS) -certFile=$(certFile) -keyFile=$(keyFile)

lint: bin/go-sumtype bin/golangci-lint ## Run linters
	bin/go-sumtype ./...
	bin/golangci-lint run --config=.golangci-required.yml
	bin/golangci-lint run --config=.golangci.yml
	bin/go-consistent -pedantic ./...

# Default value for the database name used in MongoDB connect string
DB := DB_NAME

mongosh:                                ## Run mongosh. Flags: DB
	docker-compose exec mongodb mongosh mongodb://host.docker.internal:27017/$(DB)?heartbeatFrequencyMS=300000 \
		--verbose --eval 'disableTelemetry()' --shell

mongosh-sudo:                          ## Run mongosh with sudo. Flags: DB
	sudo docker-compose exec mongodb mongosh mongodb://host.docker.internal:27017/$(DB)?heartbeatFrequencyMS=300000 \
		--verbose --eval 'disableTelemetry()' --shell

mongo:                                  ## Run (legacy) mongo shell. Flags: DB
	docker-compose exec mongodb mongo mongodb://host.docker.internal:27017/$(DB)?heartbeatFrequencyMS=300000 \
		--verbose

mongosh-tls:							## Run mongosh with tls. Flags: DB, certFile, CAFile
	docker-compose exec mongodb mongosh \
	"mongodb://host.docker.internal:27017/$(DB)?heartbeatFrequencyMS=300000&tls=true&authMechanism=MONGODB-X509&tlsCertificateKeyFile=$(certFile)&tlsCAFile=$(CAFile)" \
	--verbose --eval 'disableTelemetry()' --shell
		
# docker-init:
# 	docker buildx create --driver=docker-container --name=SAPHANACompatibilityLayer

# docker-build: build-testcover
# 	env GOOS=linux GOARCH=arm64            go test -c -o=bin/SAPHANACompatibilityLayer-arm64 -trimpath -tags=testcover -coverpkg=./... ./cmd/SAPHANACompatibilityLayer
# 	env GOOS=linux GOARCH=amd64 GOAMD64=v2 go test -c -o=bin/SAPHANACompatibilityLayer-amd64 -trimpath -tags=testcover -coverpkg=./... ./cmd/SAPHANACompatibilityLayer

# docker-local: docker-build
# 	docker buildx build --builder=SAPHANACompatibilityLayer --tag=ghcr.io/SAPHANACompatibilityLayer/SAPHANACompatibilityLayer:local --load .

# docker-push: docker-build
# 	test $(DOCKER_TAG)
# 	docker buildx build --builder=SAPHANACompatibilityLayer --platform=linux/arm64,linux/amd64 --tag=ghcr.io/SAPHANACompatibilityLayer/SAPHANACompatibilityLayer:$(DOCKER_TAG) --push .

bin/golangci-lint:
	$(MAKE) init

bin/go-sumtype:
	$(MAKE) init

bin/gofumports:
	$(MAKE) init

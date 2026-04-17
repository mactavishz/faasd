GitCommit := $(shell git rev-parse HEAD)
LDFLAGS := "-s -w -X github.com/openfaas/faasd/pkg.GitCommit=$(GitCommit)"
CONTAINERD_VER := 1.7.27
CNI_VERSION := v0.9.1
ARCH := amd64

.PHONY: all
all: unit-test dist hashgen

.PHONY: publish
publish: dist hashgen

local:
	CGO_ENABLED=0 go build -o bin/faasd

.PHONY: install
install: dist-local
	# Install faasd binary (required for faasd install command)
	echo "Installing new faasd binary..."
	sudo rm -f /usr/local/bin/faasd
	sudo cp bin/faasd /usr/local/bin/faasd
	sudo chmod +x /usr/local/bin/faasd

	# Verify the binary was installed
	echo "Verifying binary..."
	ls -lh /usr/local/bin/faasd
	/usr/local/bin/faasd version

	# Install faasd services using the new binary
	sudo /usr/local/bin/faasd install

	
.PHONY: uninstall
uninstall:
	sudo rm -rf /usr/local/bin/faasd
	sudo rm -rf /usr/local/bin/faasd-gateway
	sudo rm -rf /var/lib/faasd
	sudo rm -rf /usr/lib/systemd/system/faasd-provider.service
	sudo rm -rf /usr/lib/systemd/system/faasd.service
	sudo rm -rf /usr/lib/systemd/system/faasd-gateway.service
	sudo rm -rf /lib/systemd/system/faasd-provider.service
	sudo rm -rf /lib/systemd/system/faasd.service
	sudo rm -rf /lib/systemd/system/faasd-gateway.service

.PHONY: clean
clean:
	./hack/clean.sh

.PHONY: down
down:
	./hack/down.sh

.PHONY: unit-test
unit-test:
	CGO_ENABLED=0 go test -ldflags $(LDFLAGS) $(shell go list ./... | grep -v '/test/integrations')

.PHONY: integration-test
integration-test:
	go test -count=1 -v -timeout 10m ./test/integrations/...

.PHONY: dist-local
dist-local:
	CGO_ENABLED=0 go build -ldflags $(LDFLAGS) -o bin/faasd

.PHONY: dist
dist:
	CGO_ENABLED=0 go build -ldflags $(LDFLAGS) -o bin/faasd
	CGO_ENABLED=0 GOARCH=arm64 go build -ldflags $(LDFLAGS) -o bin/faasd-arm64

.PHONY: hashgen
hashgen:
	for f in bin/faasd*; do shasum -a 256 $$f > $$f.sha256; done

verify-compose:
	@echo Verifying docker-compose.yaml images in remote registries && \
	arkade chart verify --verbose=$(VERBOSE) -f ./docker-compose.yaml

upgrade-compose:
	@echo Checking for newer images in remote registries && \
	arkade chart upgrade -f ./docker-compose.yaml --write

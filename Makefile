.PHONY: generate test run
PYTHON ?= python3

generate:
	$(PYTHON) -m grpc_tools.protoc -I artifacts --python_out=src/spaceport_test_server/generated artifacts/bazaar.proto

test:
	$(PYTHON) -m pytest

run:
	$(PYTHON) -m spaceport_test_server.demo_server --scenario scenarios/default.json

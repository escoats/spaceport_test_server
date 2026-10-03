.PHONY: generate test run
PYTHON ?= python3
SCENARIO ?= default.json
SCENARIO_PATH = $(if $(findstring /,$(SCENARIO)),$(SCENARIO),scenarios/$(SCENARIO))
BOTS ?=
MIN ?=

generate:
	$(PYTHON) -m grpc_tools.protoc -I artifacts --python_out=src/spaceport_test_server/generated artifacts/bazaar.proto

test:
	$(PYTHON) -m pytest

run:
	$(PYTHON) -m spaceport_test_server.demo_server --scenario "$(SCENARIO_PATH)" --host 0.0.0.0 --credential-file ./demo-credentials.json $(if $(MIN),--minimum-ready-stations $(MIN),) $(foreach station,$(BOTS),--bot-station $(station))

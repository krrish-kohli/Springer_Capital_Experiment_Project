## NiFi flow export

This folder contains the NiFi flow artifacts and a reproducible flow definition.

### What’s included
- `flow_definition.json`: high-level flow outline
- `flow_definition.md`: step-by-step configuration to rebuild/import the flow in NiFi UI

### Quick note
In this MVP, NiFi calls the `runner` service over HTTP. The runner executes dbt and persists audit outputs to ClickHouse.

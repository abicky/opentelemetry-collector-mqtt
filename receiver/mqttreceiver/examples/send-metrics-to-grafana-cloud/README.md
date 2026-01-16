# Send metrics to Grafana Cloud example

This example converts messages from the MQTT broker into metrics and sends the resulting metrics to Grafana Cloud.
The message payload must be in JSON format and must contain the `data.value` field as a numeric value.

## Environment variables

| Name                                            | Required  | Default              | Description                                                                    |
|-------------------------------------------------|-----------|----------------------|--------------------------------------------------------------------------------|
| MQTT_BROKER                                     | Yes       | tcp://127.0.0.1:1883 | MQTT broker URL.                                                               |
| MQTT_TOPIC                                      | Yes       |                      | MQTT topics to subscribe to.                                                   |
| MQTT_USERNAME                                   | No        |                      | Username used for broker authentication.                                       |
| MQTT_PASSWORD                                   | No        |                      | Password used for broker authentication.                                       |
| GRAFANA_CLOUD_OTLP_ENDPOINT                     | Yes       |                      | Grafana Cloud OTLP endpoint, e.g., `https://otlp-gateway-...`                  |
| GRAFANA_CLOUD_PROMETHEUS_REMOTE_WRITE_ENDPOINT  | Yes       |                      | Grafana Cloud Prometheus remote write endpoint, e.g., `https://prometheus-...` |
| GRAFANA_CLOUD_API_KEY                           | Yes       |                      | Grafana Cloud API key with logs and metrics write permissions.                 |
| GRAFANA_CLOUD_INSTANCE_ID                       | Yes       |                      | Grafana Cloud instance ID (used for Basic authentication).                     |
| GRAFANA_CLOUD_PROMETHEUS_USER                   | Yes       |                      | Grafana Cloud Prometheus user (used for Basic authentication).                 |
| GRAFANA_CLOUD_OTLP_BASIC_AUTH                   | Yes       |                      | Base64-encoded `GRAFANA_CLOUD_INSTANCE_ID:GRAFANA_CLOUD_API_KEY`.              |

# Convert logs to metrics example

This example converts messages from the MQTT broker into metrics and displays the resulting metrics on stdout.
The message payload must be in JSON format and must contain the `data.value` field as a numeric value.

## Environment variables

| Name          | Required | Default              | Description                              |
|---------------|----------|----------------------|------------------------------------------|
| MQTT_BROKER   | Yes      | tcp://127.0.0.1:1883 | MQTT broker URL.                         |
| MQTT_TOPIC    | Yes      |                      | MQTT topics to subscribe to.             |
| MQTT_USERNAME | No       |                      | Username used for broker authentication. |
| MQTT_PASSWORD | No       |                      | Password used for broker authentication. |

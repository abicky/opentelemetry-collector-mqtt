# Basic example

This example displays messages from the MQTT broker on stdout.

## Environment variables

| Name          | Required | Default              | Description                              |
|---------------|----------|----------------------|------------------------------------------|
| MQTT_BROKER   | Yes      | tcp://127.0.0.1:1883 | MQTT broker URL.                         |
| MQTT_TOPIC    | Yes      |                      | MQTT topics to subscribe to.             |
| MQTT_USERNAME | No       |                      | Username used for broker authentication. |
| MQTT_PASSWORD | No       |                      | Password used for broker authentication. |

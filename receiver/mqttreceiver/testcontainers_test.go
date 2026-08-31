package mqttreceiver

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"strings"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var mqttBrokerContainer *testcontainers.DockerContainer

func beforeAll() {
	mqttBrokerContainer = startMQTTBroker()
}

func afterAll() {
	stopMQTTBroker(mqttBrokerContainer)
}

func startMQTTBroker() *testcontainers.DockerContainer {
	ctx := context.Background()
	c, err := testcontainers.Run(
		ctx,
		"eclipse-mosquitto:2.1.2-alpine",
		testcontainers.WithExposedPorts("1883/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("1883/tcp"),
		),
		// Bind 1883 port for TestComponentLifecycle, where the port cannot be set dynamically
		testcontainers.WithHostConfigModifier(func(hostConfig *container.HostConfig) {
			hostConfig.PortBindings = network.PortMap{network.MustParsePort("1883/tcp"): []network.PortBinding{
				{
					HostIP:   netip.MustParseAddr("127.0.0.1"),
					HostPort: "1883",
				},
			}}
		}),
		testcontainers.WithFiles(
			testcontainers.ContainerFile{
				Reader: strings.NewReader(`listener 1883
allow_anonymous true

plugin /usr/lib/mosquitto_dynamic_security.so
plugin_opt_config_file /mosquitto/config/dynamic-security.json`),
				ContainerFilePath: "/mosquitto/config/mosquitto.conf",
				FileMode:          0o644,
			},
			testcontainers.ContainerFile{
				Reader: strings.NewReader(`{
  "defaultACLAccess": {
    "publishClientSend": true,
    "publishClientReceive": true,
    "subscribe": false,
    "unsubscribe": true
  },
  "clients": [
    {
      "username": "username",
      "roles": [
        {
          "rolename": "topic-observe"
        }
      ],
      "//": "The decoded password is \"password\"",
      "encoded_password": "$7$1000$N8teyv1c3sfUa4DPT8tBzN4EnGUP65S0pRlwigDSpOE2jwdXkrb1vsgHwqy4+rLd098zHelhMBVC0m313PfHlw==$WV896GIjW2GCid6QPi/9wA84SoGWTFOgQPequnDtR6DlSUL+ZvZn5RiTaENq0ijqJEyNYB7wEIPcy+X65221LA=="
    }
  ],
  "groups": [
    {
      "groupname": "unauthenticated",
      "roles": [
        {
          "rolename": "topic-observe"
        }
      ]
    }
  ],
  "roles": [
    {
      "rolename": "topic-observe",
      "acls": [
        {
          "acltype": "subscribeLiteral",
          "topic": "test/topic/rejected",
          "priority": 100,
          "allow": false
        },
        {
          "acltype": "subscribePattern",
          "topic": "#",
          "priority": 0,
          "allow": true
        }
      ]
    }
  ],
  "anonymousGroup": "unauthenticated"
}`),
				ContainerFilePath: "/mosquitto/config/dynamic-security.json",
				FileMode:          0o644,
			},
		),
	)
	if err != nil {
		panic(err)
	}

	return c
}

func stopMQTTBroker(c *testcontainers.DockerContainer) {
	if err := testcontainers.TerminateContainer(c); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to terminate container %q: %v\n", c.ID, err)
	}
}

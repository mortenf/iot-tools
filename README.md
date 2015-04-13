IoT Tools
=============

This is a - small, possibly growing - collection of tools for working with [IoT](http://en.wikipedia.org/wiki/Internet_of_Things) - the Internet of Things.

Currently the collection consists of a python script for forwarding messages via [MQTT](http://mqtt.org/).

## mqtt-forward.py

A Python script that subscribes to a topic using MQTT and publishes all messages to another topic (possibly on another broker).

### Usage

```bash
./mqtt-forward.py mqtt-forward.conf weather_opensensors &
```

### Sample Configuration

```
[weather_opensensors]
sub = local_pywws
pub = opensensors_wh1080

[local_pywws]
topic = weather/pywws
hostname = localhost

[opensensors_wh1080]
topic = /users/<username>/test
hostname = opensensors.io
client_id = <device client id>
auth = True
user = <username>
password = <password>
```

## License

This collection is provided under the GPL License.

## Contribution

Please do!

IoT Tools
=============

This is a - small, possibly growing - collection of tools for working with [IoT](http://en.wikipedia.org/wiki/Internet_of_Things) - the Internet of Things.

Currently the collection consists of a python script for forwarding messages via [MQTT](http://mqtt.org/).

## mqtt-forward.py

A Python script that subscribes to a topic using MQTT and publishes all messages to another topic (possibly on another broker).
Requires [paho-mqtt](https://pypi.python.org/pypi/paho-mqtt) and [jq](https://pypi.python.org/pypi/jq/).

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
transform = [ .temp_out_c, .temp_in_c ]
```

### Sample Output

```
2015-04-30 21:29:34.036263: message from @weather/pywws received: {"displayname": "Rosenborg 68750", "wind_dir_txt": "E", "rel_pressure_hpa": 1009.7, "guid": "8B22C249-CF00-40C7-B460-38C559C69F42", "wind_dir_deg": 90, "rain_mm": 0.0, "hum_in_perc": 46, "timecreated": "2015-04-30T21:29:31Z", "hum_out_perc": 89, "temp_out_c": 6.2, "wind_gust_mps": 0.3, "wind_speed_mps": 0.0, "location": "Risbjerg, Hvidovre, Denmark", "organization": "Morten Frederiksen", "temp_in_c": 20.6}
2015-04-30 21:29:34.053457: 2 message(s) published: [{'topic': 'test/test', 'retain': 0, 'qos': 0, 'payload': '6.2'}, {'topic': 'test/test', 'retain': 0, 'qos': 0, 'payload': '20.6'}]
```

## License

This collection is provided under the GPL License.

## Contribution

Please do!

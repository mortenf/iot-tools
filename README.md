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

### Sample Output

```
2015-04-13 21:57:32.249259: message from @weather/pywws received: {"rel_pressue": "30.2623", "wind_ave": "3.80", "rain": "0", "rainin": "0", "hum_in": "44", "temp_in_f": "68.0", "dailyrainin": "0.318898", "wind_dir": "315", "temp_in_c": "20.0", "hum_out": "66", "dailyrain": "8.1", "wind_gust": "9.17", "idx": "2015-04-13 21:57:26", "temp_out_f": "40.5", "temp_out_c": "4.7"}
2015-04-13 21:57:32.549034: message(s) published: [{'topic': '/users/mortenhf/test', 'retain': 0, 'qos': 0, 'payload': '{"rel_pressue": "30.2623", "wind_ave": "3.80", "rain": "0", "rainin": "0", "hum_in": "44", "temp_in_f": "68.0", "dailyrainin": "0.318898", "wind_dir": "315", "temp_in_c": "20.0", "hum_out": "66", "dailyrain": "8.1", "wind_gust": "9.17", "idx": "2015-04-13 21:57:26", "temp_out_f": "40.5", "temp_out_c": "4.7"}'}]
```

## License

This collection is provided under the GPL License.

## Contribution

Please do!

var mqtt = require('mqtt');
var amqp = require('amqp10/lib/amqp_client');

var config = require('./mqtt-amqp.conf');

var puburi = 'amqps://' + encodeURIComponent(config.amqp.sas.name) + ':' + encodeURIComponent(config.amqp.sas.key) + '@' + config.amqp.host;
var suburi = 'mqtt://' + config.mqtt.host + ':' + config.mqtt.port;

var pub = new amqp(amqp.policies.EventHubPolicy);
var sub = mqtt.connect(suburi);
console.log("sub connecting: "+suburi);
sub.on('connect', function () {
  console.log("sub connected");
  console.log("pub connecting: "+puburi);
  pub.connect(puburi, function () {
    console.log("pub connected")
    sub.subscribe(config.mqtt.topic);
    console.log("subscribed");
  })
})
 
sub.on('message', function (topic, message) {
  console.log("received: " + topic + ": " + message.toString());
  pub.send(message.toString(), config.amqp.name, { 'x-opt-partition-key' : topic }, function () {
    console.log("sent: " + topic + ": " + message.toString());
  });
});

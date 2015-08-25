#!/usr/bin/env node

var mqtt = require('mqtt');
var amqp = require('amqp10');
var program = require('commander');

// Arguments and configuration
program
  .usage('[options] <configuration file>')
  .option('-v, --verbose', 'increase number of reassuring output messages', function(v, vv) { return vv + 1; }, 0)
  .parse(process.argv);
if (program.args.length < 1) {
   console.error('Error, missing configuration file argument!');
   program.help();
}
var config = require(program.args[0]);

// MQTT
var suburi = 'mqtt://' + config.mqtt.host + ':' + config.mqtt.port;
if ((typeof config.mqtt.username !== 'undefined') && (typeof config.mqtt.password !== 'undefined'))
  suburi = suburi.replace('mqtt://', 'mqtt://' + config.mqtt.username + ':' + config.mqtt.password + '@');

// AMQP
var puburi = 'amqps://' + encodeURIComponent(config.amqp.sas.name) + ':' + encodeURIComponent(config.amqp.sas.key) + '@' + config.amqp.host;

// Connect and subscribe
var pub = new amqp(amqp.policies.EventHubPolicy);
var sub = mqtt.connect(suburi);
console.log("sub connecting: " + suburi.replace(/^(\w+:..).+?:.+?@(.+)$/, '$1$2'));
sub.on('connect', function () {
  console.log("sub connected");
  console.log("pub connecting: " + puburi.replace(/^(\w+:..).+?:.+?@(.+)$/, '$1$2'));
  pub.connect(puburi, function () {
    console.log("pub connected")
    sub.subscribe(config.mqtt.topic);
    console.log("subscribed");
  })
})

// Publish 
sub.on('message', function (topic, message) {
  if (program.verbose)
    console.log("received: " + topic + ": " + message.toString());
  pub.send(message.toString(), config.amqp.name, { 'x-opt-partition-key' : topic }, function () {
    if (program.verbose)
      console.log("sent: " + topic + ": " + message.toString());
  });
});

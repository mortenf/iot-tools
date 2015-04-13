#!/usr/bin/env python

import getopt
from ConfigParser import SafeConfigParser
import paho.mqtt.client as mqtt
import sys
import os
import datetime

__usage__ = """
 usage: python mqtt-forward.py [options] configuration_file configuration_section
 options are:
  -h or --help     display this help
  -v or --verbose  increase amount of reassuring messages
"""

# The callback for when the subscriber receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    (verbose, pub, subtopic) = userdata
    if (verbose > 0):
        print("sub connected with result code "+str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(subtopic)


# Internal function for publishing the first message in the publisher userdata queue
def _do_publish(c):
    (verbose, msgs, pubtopic) = c._userdata
    m = msgs[0]
    msgs = msgs[1:]
    c._userdata = (verbose, msgs, pubtopic)
    (topic, payload, qos, retain) = m
    c.publish(topic, payload, qos, retain)
    if verbose > 0:
        print(str(datetime.datetime.utcnow())+": message to @"+topic+" published: "+str(payload))


# The callback for when a message has been published by the publisher
def on_publish(c, userdata, mid):
    (verbose, msgs, pubtopic) = userdata
    if len(msgs) != 0:
        _do_publish(c)


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    (verbose, pub, subtopic) = userdata
    if verbose > 0:
        print(str(datetime.datetime.utcnow())+": message from @"+msg.topic+" received: "+str(msg.payload))
    (verbose, msgs, pubtopic) = pub._userdata
    pub._userdata = (verbose, [(pubtopic, msg.payload, msg.qos, msg.retain)], pubtopic)
    _do_publish(pub)


def do_mqtt_forward(config, section, verbose):
    # configuration
    cfg = SafeConfigParser({"client_id": "mqtt-forward-"+section+'-'+str(os.getpid()), "hostname": "localhost", "port": "1883", "retain": "False", "auth": "False"})
    cfg.optionxform = str
    cfg.read(config)
    pubcfg = cfg.get(section, "pub")
    subcfg = cfg.get(section, "sub")

    # publication setup
    pub = mqtt.Client(client_id=cfg.get(pubcfg, "client_id"), userdata=(verbose, [], cfg.get(pubcfg, "topic")))
    pub.on_publish = on_publish
    if eval(cfg.get(pubcfg, "auth")):
        pub.username_pw_set(cfg.get(pubcfg, "user"), cfg.get(pubcfg, "password"))
    pub.connect(cfg.get(pubcfg, "hostname"), eval(cfg.get(pubcfg, "port")), 60)

    # subscription setup
    sub = mqtt.Client(client_id=cfg.get(subcfg, "client_id"), userdata=(verbose, pub, cfg.get(subcfg, "topic")))
    sub.on_connect = on_connect
    sub.on_message = on_message
    if eval(cfg.get(subcfg, "auth")):
        sub.username_pw_set(cfg.get(subcfg, "user"), cfg.get(subcfg, "password"))
    sub.connect(cfg.get(subcfg, "hostname"), eval(cfg.get(subcfg, "port")), 60)

    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.
    sub.loop_forever()
    return 0


def main(argv=None):
    if argv is None:
        argv = sys.argv
    try:
        opts, args = getopt.getopt(
            argv[1:], "hv", ['help', 'verbose'])
    except getopt.error, msg:
        print >>sys.stderr, 'Error: %s\n' % msg
        print >>sys.stderr, __usage__.strip()
        return 1
    # process options
    verbose = 0
    for o, a in opts:
        if o == '-h' or o == '--help':
            print __usage__.strip()
            return 0
        elif o == '-v' or o == '--verbose':
            verbose += 1
    # check arguments
    if len(args) != 2:
        print >>sys.stderr, "Error: 2 arguments required"
        print >>sys.stderr, __usage__.strip()
        return 2
    return do_mqtt_forward(args[0], args[1], verbose)

if __name__ == "__main__":
    sys.exit(main())


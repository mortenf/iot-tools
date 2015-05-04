#!/usr/bin/env python

import getopt
from ConfigParser import SafeConfigParser
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
import sys
import os
import datetime
import socket
from jq import jq

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


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    (verbose, pub, subtopic) = userdata
    if verbose > 0:
        print(str(datetime.datetime.utcnow())+": message from @"+msg.topic+" received: "+str(msg.payload))
    # Generate outgoing topic by splicing with incoming topic at #
    pubtopic = pub["topic"]
    if subtopic.index('#') != -1 and pubtopic.index('#') != -1:
        pubtopic = pubtopic[0:pubtopic.index('#')]+msg.topic[subtopic.index('#'):]
    # Transform topic and message as JSON via jq
    if pub["transform"] != None:
        msgs = []
        try:
            mm = jq( pub["transform"] ).transform(text=msg.payload, multiple_output=True)
            if isinstance(mm, list):
                mm = mm[0]
            if not isinstance(mm, list):
                mm = [mm]
            for m in mm:
                if isinstance(m, dict) and m.keys() == ["topic", "payload"]:
                    topic = str(m["topic"])
                    m = m["payload"]
                else:
                    topic = pubtopic
                m = jq( '.' ).transform(m, text_output=True)
                msgs.append( { "topic": topic, "payload": str(m), "qos": msg.qos, "retain": msg.retain } )
        except Exception, e:
            print >>sys.stderr, "%s: jq error: %s" % (str(datetime.datetime.utcnow()), e)
            return
    else:
        msgs = [ { "topic": pubtopic, "payload": msg.payload, "qos": msg.qos, "retain": msg.retain } ]
    try:
        publish.multiple(msgs, hostname=pub["hostname"], port=pub["port"], client_id=pub["client_id"], auth=pub["auth"])
        if verbose > 0:
            print(str(datetime.datetime.utcnow())+": "+str(len(msgs))+" message(s) published: "+str(msgs))
    except Exception, e:
        print >>sys.stderr, "%s: publishing error: %s" % (str(datetime.datetime.utcnow()), e)

def do_mqtt_forward(config, section, verbose):
    # configuration
    cfg = SafeConfigParser({"client_id": "mqtt-forward-"+section+'-'+str(os.getpid()), "hostname": "localhost", "port": "1883", "retain": "False", "auth": "False", "transform": None})
    cfg.optionxform = str
    cfg.read(config)
    pubcfg = cfg.get(section, "pub")
    subcfg = cfg.get(section, "sub")

    # publication setup
    if eval(cfg.get(pubcfg, "auth")):
        pubauth = { "username": cfg.get(pubcfg, "user"), "password": cfg.get(pubcfg, "password") }
    else:
        pubauth = None
    pub = { "hostname": cfg.get(pubcfg, "hostname"), "port": eval(cfg.get(pubcfg, "port")), "auth": pubauth, "client_id": cfg.get(pubcfg, "client_id"), "topic": cfg.get(pubcfg, "topic"), "transform": cfg.get(pubcfg, "transform") }

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


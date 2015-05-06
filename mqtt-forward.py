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
from time import sleep
import threading
import signal

__usage__ = """
 usage: python mqtt-forward.py [options] configuration_file configuration_section(s)
 options are:
  -h or --help     display this help
  -v or --verbose  increase amount of reassuring messages
"""

# Exit signals
done = 0
reload = 0

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
    if subtopic.find('#') != -1 and pubtopic.find('#') != -1:
        pubtopic = pubtopic[0:pubtopic.find('#')]+msg.topic[subtopic.find('#'):]
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
                msgs.append( { "topic": topic, "payload": str(m), "qos": pub["qos"], "retain": pub["retain"] } )
        except Exception, e:
            print >>sys.stderr, "%s: jq error: %s" % (str(datetime.datetime.utcnow()), e)
            return
    else:
        msgs = [ { "topic": pubtopic, "payload": msg.payload, "qos": pub["qos"], "retain": pub["retain"] } ]
    try:
        publish.multiple(msgs, hostname=pub["hostname"], port=pub["port"], client_id=pub["client_id"], auth=pub["auth"])
        if verbose > 0:
            print(str(datetime.datetime.utcnow())+": "+str(len(msgs))+" message(s) published: "+str(msgs))
    except Exception, e:
        print >>sys.stderr, "%s: publishing error: %s" % (str(datetime.datetime.utcnow()), e)

def do_mqtt_forward(config, section, verbose):
    # configuration
    cfg = SafeConfigParser({"client_id": "mqtt-forward-"+section+'-'+str(os.getpid()), "hostname": "localhost", "port": "1883", "retain": "False", "auth": "False", "transform": None, "retain": "False", "qos": "0"})
    cfg.optionxform = str
    cfg.read(config)
    pubcfg = cfg.get(section, "pub")
    subcfg = cfg.get(section, "sub")

    # publication setup
    if eval(cfg.get(pubcfg, "auth")):
        pubauth = { "username": cfg.get(pubcfg, "user"), "password": cfg.get(pubcfg, "password") }
    else:
        pubauth = None
    pub = { "hostname": cfg.get(pubcfg, "hostname"), "port": eval(cfg.get(pubcfg, "port")), "auth": pubauth, "client_id": cfg.get(pubcfg, "client_id"), "topic": cfg.get(pubcfg, "topic"), "transform": cfg.get(pubcfg, "transform"), "retain": eval(cfg.get(pubcfg, "retain")), "qos": eval(cfg.get(pubcfg, "qos")) }

    # subscription setup
    sub = mqtt.Client(client_id=cfg.get(subcfg, "client_id"), userdata=(verbose, pub, cfg.get(subcfg, "topic")))
    sub.on_connect = on_connect
    sub.on_message = on_message
    if eval(cfg.get(subcfg, "auth")):
        sub.username_pw_set(cfg.get(subcfg, "user"), cfg.get(subcfg, "password"))
    sub.connect(cfg.get(subcfg, "hostname"), eval(cfg.get(subcfg, "port")), 60)

    # Loop until done...
    sub.loop_start()
    while not done:
        sleep(1)
    sub.loop_stop()
    return 0


class mqttThread(threading.Thread):
    def __init__(self, config, section, verbose):
        threading.Thread.__init__(self)
        self.config = config
        self.section = section
        self.verbose = verbose
    def run(self):
        if self.verbose > 0:
            print(str(datetime.datetime.utcnow())+": starting thread for "+str(self.section))
        do_mqtt_forward(self.config, self.section, self.verbose)
        if self.verbose > 0:
            print(str(datetime.datetime.utcnow())+": ending thread for "+str(self.section))

def signal_handler(signum, frame):
    global reload, done
    signame = tuple((v) for v, k in signal.__dict__.iteritems() if k == signum)[0]
    print >>sys.stderr, "%s: %s received" % (str(datetime.datetime.utcnow()), signame)
    if signum == signal.SIGHUP:
        reload = 1
    done = 1

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
    if len(args) < 2:
        print >>sys.stderr, "Error: At least 2 arguments required"
        print >>sys.stderr, __usage__.strip()
        return 2
    config = args.pop(0)
    sections = args
    global done, reload
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGHUP, signal_handler)
    while not done:
        threads = []
        for section in sections:
            try:
                thread = mqttThread(config, section, verbose)
                thread.start()
                sleep(0.1)
            except Exception, e:
                print >>sys.stderr, "%s: thread error for %s: %s" % (str(datetime.datetime.utcnow()), section, e)
        while not done:
            sleep(1)
        if verbose > 0:
            print(str(datetime.datetime.utcnow())+": waiting for threads to finish...")
        for thread in threads:
            thread.join()
        while len(threading.enumerate()) > 1:
            sleep(0.1)
        if reload:
            done = 0
            reload = 0
    if verbose > 0:
        print(str(datetime.datetime.utcnow())+": finished")

if __name__ == "__main__":
    sys.exit(main())


#!/usr/bin/env python

import getopt
from ConfigParser import SafeConfigParser
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
import sys
import os
import re
import datetime
import socket
from jq import jq
from time import sleep
import threading
import signal
import urllib3
urllib3.disable_warnings()

__usage__ = """
 usage: python mqtt-forward.py [options] configuration_file configuration_section(s)
 options are:
  -h or --help     display this help
  -v or --verbose  increase amount of reassuring messages
"""

# Transformations
transforms = {
    'xctd2ctd': 'def xctd(x): x as $meta | .[0] | to_entries | map({"measurename": (.key / "_" | .[:-1] | join("_")), "unitofmeasure": (.key / "_" | .[-1:] | join("")), "value": .value }) | map(. + $meta); [with_entries(select(.key!="guid" and .key!="displayname" and .key!="organization" and .key!="location" and .key!="timecreated")),with_entries(select(.key!="guid" and .key!="displayname" and .key!="organization" and .key!="location" and .key!="timecreated"|not))] | xctd(.[1])'
}

# Exit signals
done = 0
reload = 0

# The callback for when the subscriber receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    (verbose, pub, subtopic, subplain) = userdata
    if (verbose > 0):
        print("sub for "+subtopic+" connected with result code "+str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(subtopic)


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    (verbose, pub, subtopic, subplain) = userdata
    if verbose > 0:
        print(str(datetime.datetime.utcnow())+" @"+msg.topic+": message received: "+str(msg.payload))
    if verbose > 3:
        print(str(datetime.datetime.utcnow())+" @"+msg.topic+": pub settings = "+str(pub))
    # Generate outgoing topic by splicing with incoming topic at #
    pubtopic = pub["topic"]
    plain = pub["plain"]
    qos = pub["qos"]
    retain = pub["retain"]
    if subtopic.find('#') != -1 and pubtopic != None and pubtopic.find('#') != -1:
        pubtopic = pubtopic[0:pubtopic.find('#')]+msg.topic[subtopic.find('#'):]
    # Transform topic and message as JSON via jq
    if pub["transform"] != None:
        msgs = []
        try:
            if subplain:
                msg.payload = '"' + msg.payload + '"'
            msg.payload = unicode(msg.payload, 'utf-8')
            try:
                mm = jq(pub["transform"].replace("$TOPIC$", msg.topic)).transform(text=msg.payload, multiple_output=True)
            except Exception, e:
                print >>sys.stderr, "%s @%s: jq transformation error: %s (%s)" % (str(datetime.datetime.utcnow()), msg.topic, e, msg.payload)
                return
            if verbose > 3:
                print(str(datetime.datetime.utcnow())+" @"+msg.topic+": transformed message(s) = "+str(mm))
            if isinstance(mm, list):
                mm = mm[0]
            if not isinstance(mm, list):
                mm = [mm]
            for m in mm:
                plain = pub["plain"]
                qos = pub["qos"]
                retain = pub["retain"]
                if isinstance(m, dict) and "topic" in m and "payload" in m:
                    topic = str(m["topic"])
                    if "plain" in m:
                        plain = eval(m["plain"])
                    if "qos" in m:
                        qos = eval(m["qos"])
                    if "retain" in m:
                        retain = eval(m["retain"])
                    m = m["payload"]
                else:
                    topic = pubtopic
                try:
                    m = str(jq('.').transform(m, text_output=True))
                except Exception, e:
                    print >>sys.stderr, "%s @%s[%i]: jq normalization error: %s (%s)" % (str(datetime.datetime.utcnow()), msg.topic, len(msgs), e, m)
                    return
                if verbose > 2:
                    print(str(datetime.datetime.utcnow())+" @"+msg.topic+"["+str(len(msgs))+"]: transformed message = "+str(m))
                try:
                    m = re.sub('<<<(.+?)>>>', lambda s: eval(s.group(1)), m)
                except Exception, e:
                    print >>sys.stderr, "%s @%s[%i]: eval error: %s" % (str(datetime.datetime.utcnow()), msg.topic, len(msgs), e)
                    return
                if plain and (m[0] == m[-1]) and m.startswith('"'):
                    m = m[1:-1]
                if verbose > 1:
                    print(str(datetime.datetime.utcnow())+" @"+msg.topic+"["+str(len(msgs))+"]: tweaked message = "+str(m))
                msgs.append( { "topic": topic, "payload": m, "qos": qos, "retain": retain } )
        except Exception, e:
            print >>sys.stderr, "%s @%s[%i]: message error: %s" % (str(datetime.datetime.utcnow()), msg.topic, len(msgs), e)
            return
    else:
        msgs = [ { "topic": pubtopic, "payload": msg.payload, "qos": pub["qos"], "retain": pub["retain"] } ]
    try:
        if len(msgs) > 0:
            pubc = mqtt.Client(client_id=pub["client_id"])
            if pub["auth"] is not None:
                pubc.username_pw_set(pub["auth"]["username"], pub["auth"]["password"])
            pubc.connect(pub["hostname"], pub["port"], 60)
            pubc.loop_start()
            for m in msgs:
                if verbose > 1:
                    print(str(datetime.datetime.utcnow())+" @"+msg.topic+": publishing "+str(m))
                if len(m["topic"]) > 7 and (m["topic"][:7] == "http://" or m["topic"][:8] == "https://"):
                    pool = urllib3.PoolManager()
                    pool.urlopen('POST', m["topic"], headers={'Content-Type':'application/json'}, body=m["payload"])
                else:
                    pubc.publish(m["topic"], m["payload"], m["qos"], m["retain"])
            pubc.loop_stop()
            pubc.disconnect()
        if verbose > 0:
            print(str(datetime.datetime.utcnow())+" @"+msg.topic+": "+str(len(msgs))+" message(s) published: "+str(msgs))
    except Exception, e:
        print >>sys.stderr, "%s @%s: publishing error, %s, for %s" % (str(datetime.datetime.utcnow()), msg.topic, e, str(msgs))

def do_mqtt_forward(config, section, verbose):
    # default configuration
    default = SafeConfigParser({"hostname": "localhost", "port": "1883", "auth": "False", "user": "?", "password": "?", "retain": "False", "qos": "0"})
    default.optionxform = str
    default.read(config)

    # configuration
    cfg = SafeConfigParser({"client_id": "mqtt-forward-"+section+'-'+str(os.getpid()), "hostname": default.get("mqtt-forward", "hostname"), "port": default.get("mqtt-forward", "port"), "auth": default.get("mqtt-forward", "auth"), "transform": None, "plain": "False", "retain": default.get("mqtt-forward", "retain"), "qos": default.get("mqtt-forward", "qos"), "user": default.get("mqtt-forward", "user"), "password": default.get("mqtt-forward", "password")})
    cfg.optionxform = str
    cfg.read(config)

    # publication setup
    pubsection = cfg.get(section, "pub")
    if not cfg.has_section(pubsection):
        transform = pubsection
        pubtopic = None
        pubsection = section
    else:
        transform = cfg.get(pubsection, "transform")
        pubtopic = cfg.get(pubsection, "topic")
    if transform in transforms:
        transform = transforms[transform]
    if eval(cfg.get(pubsection, "auth")):
        pubauth = { "username": cfg.get(pubsection, "user"), "password": cfg.get(pubsection, "password") }
    else:
        pubauth = None
    pub = { "hostname": cfg.get(pubsection, "hostname"), "port": eval(cfg.get(pubsection, "port")), "auth": pubauth, "client_id": cfg.get(pubsection, "client_id")+"_pub", "topic": pubtopic, "transform": transform, "plain": eval(cfg.get(pubsection, "plain")), "retain": eval(cfg.get(pubsection, "retain")), "qos": eval(cfg.get(pubsection, "qos")) }

    # subscription setup
    subsection = cfg.get(section, "sub")
    if not cfg.has_section(subsection):
        subtopic = subsection
        subsection = section
    else:
        subtopic = cfg.get(subsection, "topic")
    sub = mqtt.Client(client_id=cfg.get(subsection, "client_id")+"_sub", userdata=(verbose, pub, subtopic, eval(cfg.get(subsection, "plain"))))
    sub.on_connect = on_connect
    sub.on_message = on_message
    if eval(cfg.get(subsection, "auth")):
        sub.username_pw_set(cfg.get(subsection, "user"), cfg.get(subsection, "password"))
    sub.connect(cfg.get(subsection, "hostname"), eval(cfg.get(subsection, "port")), 60)

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
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
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
    global done, reload
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGHUP, signal_handler)
    while not done:
        sections = args
        # use special section with global configuration and section names?
        cfg = SafeConfigParser({"verbose": str(verbose)})
        cfg.optionxform = str
        cfg.read(config)
        verbose = eval(cfg.get("mqtt-forward", "verbose"))
        if len(sections) == 1 and sections[0] == "mqtt-forward":
            sections = filter(None, cfg.get("mqtt-forward", "sections").split())
        # start threads
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


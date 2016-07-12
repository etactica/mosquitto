#!/usr/bin/env python3
# Karl Palsson <karlp@etactica.com> July, 2016
# given a directory of files, load them and republish them to a given broker..
# expected to be used with a hacked mosquitto db dump tool to "save" some messages after
# having to move the db aside for other problems

import argparse
import glob
import logging
import os.path
import re
import shutil
import time
import paho.mqtt.client

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(name)s - %(message)s",
                    #filename="/var/log/blah.log"
)
log = logging.getLogger("main")

class PubRecord():
    def __init__(self, mi, fn):
        self.mi = mi
        self.fn = fn

    def isdone(self):
        return self.mi.is_published()

def make_path(p):
    """Ensure the path p exists"""
    if not os.path.exists(p):
        os.makedirs(p)
    if not os.path.isdir(p):
        raise NotADirectoryError(p)
    if not os.access(p, os.W_OK):
        raise PermissionError(p)

def wait_for(outstanding, dir):
    while True:
        done_now = set([x for x in outstanding if x.isdone()])
        for x in done_now:
            shutil.move(x.fn, dir)
        outstanding = outstanding - done_now # woop woop set maths
        if len(outstanding) == 0:
            break
        else:
            log.info("Still waiting on %d remaining", len(outstanding))
            time.sleep(0.2)
    return outstanding


def domain(opts):
    make_path(opts.dirgood)
    client = paho.mqtt.client.Client("missing-repub")
    client.connect(opts.host, port=opts.port)
    client.loop_start()

    rex = re.compile(opts.pattern)
    outstanding = set()
    fcount = 0
    for fn in glob.iglob(opts.files):
        log.debug("Considering file: %s", fn)
        m = rex.match(fn)
        if not m:
            log.error("File matched glob but not expected name pattern! %s", fn)
            continue
        fcount += 1
        qos = int(m.group(2))
        topic = m.group(3).replace("+", "/")
        #log.debug("msgid = %s, qos= %d, topic: %s -> %s", m.group(1), qos, m.group(3), topic)
        with open(fn) as f:
            info = client.publish(topic, payload=f.read(), qos=qos)
            if info.is_published():
                shutil.move(fn, opts.dirgood)
            else:
                outstanding.add(PubRecord(info, fn))
        if opts.batch > 0 and fcount % opts.batch == 0:
            log.info("Batch of %d finished, waiting for completion...", opts.batch)
            outstanding = wait_for(outstanding, opts.dirgood)

    log.info("Published %d messges, waiting on %d remaining pubacks", fcount, len(outstanding))
    wait_for(outstanding, opts.dirgood)
    client.loop_stop()
    log.info("All done!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Republish a set of messages',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-H", "--host", help="MQTT broker to publish to", default="localhost")
    parser.add_argument("-P", "--port", help="port for MQTT broker", default=1883)
    parser.add_argument("-p", "--pattern", help="pattern for files to scan", default="msgdump_(\d+):(\d):([\w\+]+)")
    parser.add_argument("-f", "--files", help="glob for files to scan", default="msgdump_*")
    parser.add_argument("-d", "--dirgood", help="Directory to move completed files to", default="repub-ok")
    # Like max in flight, except we don't do continual queue processing....
    parser.add_argument("--batch", help="After this many messages, wait for confirmations before proceeding", default=1000)
    options = parser.parse_args()
    domain(options)

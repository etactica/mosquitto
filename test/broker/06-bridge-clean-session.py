#!/usr/bin/env python3

# Does a bridge handle clean session settings correctly.
# This tests both cleansession and local_cleansession on a local broker
# connected to a remote broker.
# Tests (settings on broker A, broker B settings are irrelevant, though you'll need persistence
# enabled to test, unless you can simulate network interruptions)
# t  | LCS | CS |  queued from
#               | A->B | B->A
#  1 |  -(t| t  |  no  | no
# *2 |  -(f| f  |  yes | yes
#  3 |  t  | t  |  no  | no  (as per #1)
# *4 |  t  | f  |  no  | yes
#  5 |  f  | t  |  yes | no
# *6 |  f  | f  |  yes | yes (as per #2)
# * tests are covered here.

#Karl - start with testing just the A->B tests.


from mosq_test_helper import *

# this is our "A" broker
def write_config(filename, port1, port2, protocol_version, cs=False, lcs=None):
    with open(filename, 'w') as f:
        f.write("port %d\n" % (port2))
        f.write("\n")
        f.write("connection bridge_sample\n")
        f.write("address 127.0.0.1:%d\n" % (port1))
        f.write("topic bridge/# both 1\n")
        f.write("notifications false\n")
        f.write("restart_timeout 5\n")
        f.write("cleansession %s\n" % ("true" if cs else "false"))
        # Ensure defaults are tested
        if lcs is not None:
            f.write("local_cleansession %s\n" % ("true" if lcs else "false"))
        f.write("bridge_protocol_version %s\n" % (protocol_version))


def do_test(proto_ver, cs, lcs=None):
    if proto_ver == 4:
        bridge_protocol = "mqttv311"
        proto_ver_connect = 128+4
    else:
        bridge_protocol = "mqttv50"
        proto_ver_connect = 5

    # Match default behaviour of broker
    expect_queued = True
    if lcs is None:
        lcs = cs
    if lcs:
        expect_queued = False

    (port1, port2) = mosq_test.get_port(2)
    conf_file = os.path.basename(__file__).replace('.py', '.conf')
    write_config(conf_file, port1, port2, bridge_protocol, cs=cs, lcs=lcs)

    checkpoints = 0
    checkpoints_expected = 3 if expect_queued else 2
    keepalive = 60
    client_id = socket.gethostname()+".bridge_sample"
    connect_packet = mosq_test.gen_connect(client_id, keepalive=keepalive, clean_session=cs, proto_ver=proto_ver_connect)
    connack_packet = mosq_test.gen_connack(rc=0, proto_ver=proto_ver)

    if proto_ver == 5:
        opts = mqtt5_opts.MQTT_SUB_OPT_NO_LOCAL | mqtt5_opts.MQTT_SUB_OPT_RETAIN_AS_PUBLISHED
    else:
        opts = 0

    ssock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ssock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ssock.settimeout(40)
    ssock.bind(('', port1))
    ssock.listen(5)


    mid = 1
    subscribe_packet = mosq_test.gen_subscribe(mid, "bridge/#", 1 | opts, proto_ver=proto_ver)
    suback_packet = mosq_test.gen_suback(mid, 1, proto_ver=proto_ver)

    # clean session on this "client" is irrelvant....
    helper_connect_packet = mosq_test.gen_connect("helper", keepalive=keepalive, clean_session=True, proto_ver=proto_ver)
    helper_connack_packet = mosq_test.gen_connack(rc=0, proto_ver=proto_ver)
    # ... but qos must be > 0 to test queueing!
    # These mids are... artificial.  They're irrelevant for the client to the A broker, but necessary to line up so that A->B can be as expected

    # Publish one to get it working.
    mid = 2
    helper_publish_packet = mosq_test.gen_publish("bridge/queuing-clean-session/test", mid=mid, qos=1, retain=False, payload="message11", proto_ver=proto_ver)
    helper_puback_packet = mosq_test.gen_puback(mid, proto_ver=proto_ver)

    # publish a second after disconnecting
    mid = 3
    helper_publish_packet2 = mosq_test.gen_publish("bridge/queuing-clean-session/test", mid=mid, qos=1, retain=False, payload="message22", proto_ver=proto_ver)
    helper_puback_packet2 = mosq_test.gen_puback(mid, proto_ver=proto_ver)
    mid = 4
    subscribe2_packet = mosq_test.gen_subscribe(mid, "bridge/#", 1 | opts, proto_ver=proto_ver)
    suback2_packet = mosq_test.gen_suback(mid, 1, proto_ver=proto_ver)

    # Publish a third after reconnecting
    mid = 5
    helper_publish_packet3 = mosq_test.gen_publish("bridge/queuing-clean-session/test", mid=mid, qos=1, retain=False, payload="message33", proto_ver=proto_ver)
    helper_puback_packet3 = mosq_test.gen_puback(mid, proto_ver=proto_ver)



    try:
        broker = mosq_test.start_broker(filename=os.path.basename(__file__), port=port2, use_conf=True)

        (bridge, address) = ssock.accept()
        bridge.settimeout(20)

        # broker is "A" and is under test, bridge is this test code, being "B". for this scenario at least.
        # let the broker connect, we publish to _it_, let it flow to us, we publish again, then re-open and expect to get second message queued or not...


        if mosq_test.expect_packet(bridge, "connect", connect_packet):
            bridge.send(connack_packet)

            if mosq_test.expect_packet(bridge, "subscribe", subscribe_packet):
                bridge.send(suback_packet)

                print("stage 1, con/sub good so far")
                # Broker is now connected to us on port1.
                # Connect our client to the broker on port2 and send a publish
                # message, which we will then receive by way of the bridge
                helper = mosq_test.do_client_connect(helper_connect_packet, helper_connack_packet, port=port2)
                helper.send(helper_publish_packet)
                helper.close()

                print("published via client to test broker")
                # so far so good, but this is just normal receive on bridge
                if mosq_test.expect_packet(bridge, "publish", helper_publish_packet):
                    bridge.send(helper_puback_packet)
                    checkpoints += 1
                    print("ok, got the normal first one")
                else:
                    print("got an unexpected at publish stage?")
            else:
                print("got an unexpected subscribe1 stage?")
        else:
            print("got an unexpected connect1 stage?")

        # break our end
        bridge.close()

        # send to the broker while we're offline
        helper = mosq_test.do_client_connect(helper_connect_packet, helper_connack_packet, port=port2)
        helper.send(helper_publish_packet2)
        helper.close()

        # and now let it reconnect to us
        (bridge, address) = ssock.accept()
        bridge.settimeout(20)

        if mosq_test.expect_packet(bridge, "connect", connect_packet):
            bridge.send(connack_packet)

            if mosq_test.expect_packet(bridge, "subscribe", subscribe2_packet):
                bridge.send(suback2_packet)

                print("stage 2, got the con/sub good so far")

                # Now we send a third message.  clean session settings dictates whether we get 1,2,3 or 1,3
                helper = mosq_test.do_client_connect(helper_connect_packet, helper_connack_packet, port=port2)
                helper.send(helper_publish_packet3)
                helper.close()

                if expect_queued:
                    if mosq_test.expect_packet(bridge, "publish", helper_publish_packet2):
                        print("received a queued offline message, whether we expected it or not!")
                        checkpoints += 1
                        bridge.send(helper_puback_packet2)
                    else:
                        print("got an unexpected at publish2 stage?")

                # Always expect message 3
                if mosq_test.expect_packet(bridge, "publish", helper_publish_packet3):
                    print("received a regular online bridged message as expected")
                    checkpoints += 1
                    bridge.send(helper_puback_packet2)
                else:
                    print("got an unexpected at publish2 stage?")

            else:
                print("got an unexpected subscribe2 stage?")
        else:
            print("got an unexpected connect2 stage?")


        bridge.close()
    finally:
        #os.remove(conf_file)
        try:
            bridge.close()
        except NameError:
            pass

        broker.terminate()
        broker.wait()
        (stdo, stde) = broker.communicate()
        ssock.close()
        if checkpoints != checkpoints_expected:
            print(stde.decode('utf-8'))
            exit(1)

for cs in [False]:
    for lcs in [None, True, False]:
        print("RUnning tests with local cleansession= ", lcs)
        do_test(proto_ver=4, cs=cs, lcs=lcs)
        do_test(proto_ver=5, cs=cs, lcs=lcs)

exit(0)

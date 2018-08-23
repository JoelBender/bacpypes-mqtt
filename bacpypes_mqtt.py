#!/usr/bin/env python

"""
This module provides a virtual link layer using MQTT.
"""

from functools import reduce
from struct import unpack

from bacpypes.errors import EncodingError, DecodingError
from bacpypes.debugging import bacpypes_debugging, DebugContents, ModuleLogger, btox

from bacpypes.comm import Server, ServiceAccessPoint, ApplicationServiceElement
from bacpypes.core import deferred

from bacpypes.pdu import Address, LocalBroadcast, PCI, PDUData, PDU

import paho.mqtt.client as _paho_mqtt

# some debugging
_debug = 0
_log = ModuleLogger(globals())

# settings
ADDRESS_LENGTH = 3
BROADCAST_TOPIC = '@all'

default_lan_name = 'b'
default_broker_host = 'test.mosquitto.org'
default_broker_port = 1883
default_broker_keepalive = 60

#
#   str_address
#

def str_address(addr):
    """Return a decimal encoded string of the address."""
    return str(unpack(">I", b'\0' + addr.addrAddr))

# a dictionary of message type values and classes
bvl_pdu_types = {}

def register_bvlpdu_type(klass):
    bvl_pdu_types[klass.messageType] = klass

#
#   BVLCI
#

@bacpypes_debugging
class BVLCI(PCI, DebugContents):

    _debug_contents = ('bvlciType', 'bvlciFunction', 'bvlciLength')

    result                              = 0x00
    poll                                = 0x01
    online                              = 0x02
    offline                             = 0x03
    lostConnection                      = 0x04
    originalUnicastNPDU                 = 0x05
    originalBroadcastNPDU               = 0x06
    joinGroup                           = 0x07
    leaveGroup                          = 0x08

    def __init__(self, *args, **kwargs):
        if _debug: BVLCI._debug("__init__ %r %r", args, kwargs)
        super(BVLCI, self).__init__(*args, **kwargs)

        self.bvlciType = 0x84
        self.bvlciFunction = None
        self.bvlciLength = None

    def update(self, bvlci):
        PCI.update(self, bvlci)
        self.bvlciType = bvlci.bvlciType
        self.bvlciFunction = bvlci.bvlciFunction
        self.bvlciLength = bvlci.bvlciLength

    def encode(self, pdu):
        """encode the contents of the BVLCI into the PDU."""
        if _debug: BVLCI._debug("encode %s", str(pdu))

        # copy the basics
        PCI.update(pdu, self)

        pdu.put( self.bvlciType )               # 0x84
        pdu.put( self.bvlciFunction )

        if (self.bvlciLength != len(self.pduData) + 4):
            raise EncodingError("invalid BVLCI length")

        pdu.put_short( self.bvlciLength )

    def decode(self, pdu):
        """decode the contents of the PDU into the BVLCI."""
        if _debug: BVLCI._debug("decode %s", str(pdu))

        # copy the basics
        PCI.update(self, pdu)

        self.bvlciType = pdu.get()
        if self.bvlciType != 0x84:
            raise DecodingError("invalid BVLCI type")

        self.bvlciFunction = pdu.get()
        self.bvlciLength = pdu.get_short()

        if (self.bvlciLength != len(pdu.pduData) + 4):
            raise DecodingError("invalid BVLCI length")

    def bvlci_contents(self, use_dict=None, as_class=dict):
        """Return the contents of an object as a dict."""
        if _debug: BVLCI._debug("bvlci_contents use_dict=%r as_class=%r", use_dict, as_class)

        # make/extend the dictionary of content
        if use_dict is None:
            use_dict = as_class()

        # save the mapped value
        use_dict.__setitem__('type', self.bvlciType)
        use_dict.__setitem__('function', self.bvlciFunction)
        use_dict.__setitem__('length', self.bvlciLength)

        # return what we built/updated
        return use_dict

#
#   BVLPDU
#

@bacpypes_debugging
class BVLPDU(BVLCI, PDUData):

    def __init__(self, *args, **kwargs):
        if _debug: BVLPDU._debug("__init__ %r %r", args, kwargs)
        super(BVLPDU, self).__init__(*args, **kwargs)

    def encode(self, pdu):
        BVLCI.encode(self, pdu)
        pdu.put_data(self.pduData)

    def decode(self, pdu):
        BVLCI.decode(self, pdu)
        self.pduData = pdu.get_data(len(pdu.pduData))

    def bvlpdu_contents(self, use_dict=None, as_class=dict):
        return PDUData.pdudata_contents(self, use_dict=use_dict, as_class=as_class)

    def dict_contents(self, use_dict=None, as_class=dict, key_values=()):
        """Return the contents of an object as a dict."""
        if _debug: BVLPDU._debug("dict_contents use_dict=%r as_class=%r key_values=%r", use_dict, as_class, key_values)

        # make/extend the dictionary of content
        if use_dict is None:
            use_dict = as_class()

        # call the superclasses
        self.bvlci_contents(use_dict=use_dict, as_class=as_class)
        self.bvlpdu_contents(use_dict=use_dict, as_class=as_class)

        # return what we built/updated
        return use_dict

    def as_bytes(self):
        if _debug: BVLPDU._debug("as_bytes")

        bvlpdu = BVLPDU()
        self.encode(bvlpdu)

        pdu = PDU()
        bvlpdu.encode(pdu)

        return pdu.pduData

#
#   key_value_contents
#

@bacpypes_debugging
def key_value_contents(use_dict=None, as_class=dict, key_values=()):
    """Return the contents of an object as a dict."""
    if _debug: key_value_contents._debug("key_value_contents use_dict=%r as_class=%r key_values=%r", use_dict, as_class, key_values)

    # make/extend the dictionary of content
    if use_dict is None:
        use_dict = as_class()

    # loop through the values and save them
    for k, v in key_values:
        if v is not None:
            if hasattr(v, 'dict_contents'):
                v = v.dict_contents(as_class=as_class)
            use_dict.__setitem__(k, v)

    # return what we built/updated
    return use_dict

#------------------------------

#
#   Result
#

class Result(BVLPDU):

    _debug_contents = ('bvlciResultCode',)

    messageType = BVLCI.result

    def __init__(self, addr=None, code=None, *args, **kwargs):
        super(Result, self).__init__(*args, **kwargs)

        self.bvlciFunction = BVLCI.result
        self.bvlciLength = 4 + ADDRESS_LENGTH + 2
        self.bvlciAddress = addr
        self.bvlciResultCode = code

    def encode(self, bvlpdu):
        BVLCI.update(bvlpdu, self)
        bvlpdu.put_data(self.bvlciAddress.addrAddr)
        bvlpdu.put_short( self.bvlciResultCode )

    def decode(self, bvlpdu):
        BVLCI.update(self, bvlpdu)
        self.bvlciAddress = Address(bvlpdu.get_data(ADDRESS_LENGTH))
        self.bvlciResultCode = bvlpdu.get_short()

    def bvlpdu_contents(self, use_dict=None, as_class=dict):
        """Return the contents of an object as a dict."""
        return key_value_contents(use_dict=use_dict, as_class=as_class,
            key_values=(
                ('function', 'Result'),
                ('result_code', self.bvlciResultCode),
            ))

register_bvlpdu_type(Result)

#
#   Poll
#

class Poll(BVLPDU):

    _debug_contents = ('bvlciAddress',)

    messageType = BVLCI.poll

    def __init__(self, addr=None, *args, **kwargs):
        super(Online, self).__init__(*args, **kwargs)

        self.bvlciFunction = BVLCI.poll
        self.bvlciLength = 4 + ADDRESS_LENGTH
        self.bvlciAddress = addr

    def encode(self, bvlpdu):
        BVLCI.update(bvlpdu, self)
        bvlpdu.put_data(self.bvlciAddress.addrAddr)

    def decode(self, bvlpdu):
        BVLCI.update(self, bvlpdu)
        self.bvlciAddress = Address(bvlpdu.get_data(ADDRESS_LENGTH))

    def bvlpdu_contents(self, use_dict=None, as_class=dict):
        """Return the contents of an object as a dict."""
        return key_value_contents(use_dict=use_dict, as_class=as_class,
            key_values=(
                ('function', 'Poll'),
                ('address', self.bvlciAddress),
            ))

register_bvlpdu_type(Poll)

#
#   Online
#

class Online(BVLPDU):

    _debug_contents = ('bvlciAddress',)

    messageType = BVLCI.online

    def __init__(self, addr=None, *args, **kwargs):
        super(Online, self).__init__(*args, **kwargs)

        self.bvlciFunction = BVLCI.online
        self.bvlciLength = 4 + ADDRESS_LENGTH
        self.bvlciAddress = addr

    def encode(self, bvlpdu):
        BVLCI.update(bvlpdu, self)
        bvlpdu.put_data(self.bvlciAddress.addrAddr)

    def decode(self, bvlpdu):
        BVLCI.update(self, bvlpdu)
        self.bvlciAddress = Address(bvlpdu.get_data(ADDRESS_LENGTH))

    def bvlpdu_contents(self, use_dict=None, as_class=dict):
        """Return the contents of an object as a dict."""
        return key_value_contents(use_dict=use_dict, as_class=as_class,
            key_values=(
                ('function', 'Online'),
                ('address', self.bvlciAddress),
            ))

register_bvlpdu_type(Online)

#
#   Offline
#

class Offline(BVLPDU):

    _debug_contents = ('bvlciAddress',)

    messageType = BVLCI.offline

    def __init__(self, addr=None, *args, **kwargs):
        super(Offline, self).__init__(*args, **kwargs)

        self.bvlciFunction = BVLCI.offline
        self.bvlciLength = 4 + ADDRESS_LENGTH
        self.bvlciAddress = addr

    def encode(self, bvlpdu):
        BVLCI.update(bvlpdu, self)
        bvlpdu.put_data(self.bvlciAddress.addrAddr)

    def decode(self, bvlpdu):
        BVLCI.update(self, bvlpdu)
        self.bvlciAddress = Address(bvlpdu.get_data(ADDRESS_LENGTH))

    def bvlpdu_contents(self, use_dict=None, as_class=dict):
        """Return the contents of an object as a dict."""
        return key_value_contents(use_dict=use_dict, as_class=as_class,
            key_values=(
                ('function', 'Offline'),
                ('address', self.bvlciAddress),
            ))

register_bvlpdu_type(Offline)

#
#   LostConnection
#

class LostConnection(BVLPDU):

    _debug_contents = ('bvlciAddress',)

    messageType = BVLCI.lostConnection

    def __init__(self, addr=None, *args, **kwargs):
        super(LostConnection, self).__init__(*args, **kwargs)

        self.bvlciFunction = BVLCI.lostConnection
        self.bvlciLength = 4 + ADDRESS_LENGTH
        self.bvlciAddress = addr

    def encode(self, bvlpdu):
        BVLCI.update(bvlpdu, self)
        bvlpdu.put_data(self.bvlciAddress.addrAddr)

    def decode(self, bvlpdu):
        BVLCI.update(self, bvlpdu)
        self.bvlciAddress = Address(bvlpdu.get_data(ADDRESS_LENGTH))

    def bvlpdu_contents(self, use_dict=None, as_class=dict):
        """Return the contents of an object as a dict."""
        return key_value_contents(use_dict=use_dict, as_class=as_class,
            key_values=(
                ('function', 'LostConnection'),
                ('address', self.bvlciAddress),
            ))

register_bvlpdu_type(LostConnection)

#
#   OriginalUnicastNPDU
#

class OriginalUnicastNPDU(BVLPDU):

    _debug_contents = ('bvlciAddress',)

    messageType = BVLCI.originalUnicastNPDU

    def __init__(self, addr=None, *args, **kwargs):
        super(OriginalUnicastNPDU, self).__init__(*args, **kwargs)

        self.bvlciFunction = BVLCI.originalUnicastNPDU
        self.bvlciLength = 4 + ADDRESS_LENGTH + len(self.pduData)
        self.bvlciAddress = addr

    def encode(self, bvlpdu):
        # make sure the length is correct
        self.bvlciLength = 4 + ADDRESS_LENGTH + len(self.pduData)

        BVLCI.update(bvlpdu, self)

        # encode the address
        bvlpdu.put_data(self.bvlciAddress.addrAddr)

        # encode the rest of the data
        bvlpdu.put_data( self.pduData )

    def decode(self, bvlpdu):
        BVLCI.update(self, bvlpdu)

        # get the address
        self.bvlciAddress = Address(bvlpdu.get_data(ADDRESS_LENGTH))

        # get the rest of the data
        self.pduData = bvlpdu.get_data(len(bvlpdu.pduData))

    def bvlpdu_contents(self, use_dict=None, as_class=dict):
        """Return the contents of an object as a dict."""

        # make/extend the dictionary of content
        if use_dict is None:
            use_dict = as_class()

        # call the normal procedure
        key_value_contents(use_dict=use_dict, as_class=as_class,
            key_values=(
                ('function', 'OriginalUnicastNPDU'),
                ('address', self.bvlciAddress),
            ))

        # this message has data
        PDUData.dict_contents(self, use_dict=use_dict, as_class=as_class)

        # return what we built/updated
        return use_dict

register_bvlpdu_type(OriginalUnicastNPDU)

#
#   OriginalBroadcastNPDU
#

class OriginalBroadcastNPDU(BVLPDU):

    _debug_contents = ('bvlciAddress',)

    messageType = BVLCI.originalBroadcastNPDU

    def __init__(self, addr=None, *args, **kwargs):
        super(OriginalBroadcastNPDU, self).__init__(*args, **kwargs)

        self.bvlciFunction = BVLCI.originalBroadcastNPDU
        self.bvlciLength = 4 + ADDRESS_LENGTH + len(self.pduData)
        self.bvlciAddress = addr

    def encode(self, bvlpdu):
        # make sure the length is correct
        self.bvlciLength = 4 + ADDRESS_LENGTH + len(self.pduData)

        BVLCI.update(bvlpdu, self)

        # encode the address
        bvlpdu.put_data(self.bvlciAddress.addrAddr)

        # encode the rest of the data
        bvlpdu.put_data( self.pduData )

    def decode(self, bvlpdu):
        BVLCI.update(self, bvlpdu)

        # get the address
        self.bvlciAddress = Address(bvlpdu.get_data(ADDRESS_LENGTH))

        # get the rest of the data
        self.pduData = bvlpdu.get_data(len(bvlpdu.pduData))

    def bvlpdu_contents(self, use_dict=None, as_class=dict):
        """Return the contents of an object as a dict."""

        # make/extend the dictionary of content
        if use_dict is None:
            use_dict = as_class()

        # call the normal procedure
        key_value_contents(use_dict=use_dict, as_class=as_class,
            key_values=(
                ('function', 'OriginalBroadcastNPDU'),
                ('address', self.bvlciAddress),
            ))

        # this message has data
        PDUData.dict_contents(self, use_dict=use_dict, as_class=as_class)

        # return what we built/updated
        return use_dict

register_bvlpdu_type(OriginalBroadcastNPDU)

#
#   JoinGroup
#

class JoinGroup(BVLPDU):

    _debug_contents = ('bvlciAddress',)

    messageType = BVLCI.joinGroup

    def __init__(self, addr=None, *args, **kwargs):
        super(JoinGroup, self).__init__(*args, **kwargs)

        self.bvlciFunction = BVLCI.joinGroup
        self.bvlciLength = 4 + ADDRESS_LENGTH
        self.bvlciAddress = addr

    def encode(self, bvlpdu):
        BVLCI.update(bvlpdu, self)
        bvlpdu.put_data(self.bvlciAddress.addrAddr)

    def decode(self, bvlpdu):
        BVLCI.update(self, bvlpdu)
        self.bvlciAddress = Address(bvlpdu.get_data(ADDRESS_LENGTH))

    def bvlpdu_contents(self, use_dict=None, as_class=dict):
        """Return the contents of an object as a dict."""
        return key_value_contents(use_dict=use_dict, as_class=as_class,
            key_values=(
                ('function', 'JoinGroup'),
                ('address', self.bvlciAddress),
            ))

register_bvlpdu_type(JoinGroup)

#
#   LeaveGroup
#

class LeaveGroup(BVLPDU):

    _debug_contents = ('bvlciAddress',)

    messageType = BVLCI.leaveGroup

    def __init__(self, addr=None, *args, **kwargs):
        super(LeaveGroup, self).__init__(*args, **kwargs)

        self.bvlciFunction = BVLCI.leaveGroup
        self.bvlciLength = 4 + ADDRESS_LENGTH
        self.bvlciAddress = addr

    def encode(self, bvlpdu):
        BVLCI.update(bvlpdu, self)
        bvlpdu.put_data(self.bvlciAddress.addrAddr)

    def decode(self, bvlpdu):
        BVLCI.update(self, bvlpdu)
        self.bvlciAddress = Address(bvlpdu.get_data(ADDRESS_LENGTH))

    def bvlpdu_contents(self, use_dict=None, as_class=dict):
        """Return the contents of an object as a dict."""
        return key_value_contents(use_dict=use_dict, as_class=as_class,
            key_values=(
                ('function', 'LeaveGroup'),
                ('address', self.bvlciAddress),
            ))

register_bvlpdu_type(LeaveGroup)

#
#   MQTTClient
#
#   This class is a mapping between the client/server pattern and the
#   MQTT API.
#

@bacpypes_debugging
class MQTTClient(ServiceAccessPoint, Server):

    def __init__(self,
        lan, client,
        host=default_broker_host, port=default_broker_port, keepalive=default_broker_keepalive,
        sap=None, sid=None,
    ):
        if _debug: MQTTClient._debug("__init__ %r %r %r %r %r", lan, client, host, port, keepalive)
        ServiceAccessPoint.__init__(self, sap)
        Server.__init__(self, sid)

        # save the lan and client address
        self.lan = lan
        self.client = client

        # save the connection parameters
        self.host = host
        self.port = port
        self.keepalive = keepalive

        # create a client and set the callbacks
        self.mqtt_client = _paho_mqtt.Client()
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_disconnect = self.on_disconnect
        self.mqtt_client.on_subscribe = self.on_subscribe
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.on_publish = self.on_publish
        self.mqtt_client.on_unsubscribe = self.on_unsubscribe

        # we are not connected
        self.mqtt_connected = False

        self.local_topic = self.lan + '/' + str_address(client)
        if _debug: MQTTClient._debug("    - local topic: %r", self.local_topic)

        self.broadcast_topic = self.lan + '/' + BROADCAST_TOPIC
        if _debug: MQTTClient._debug("    - broadcast topic: %r", self.broadcast_topic)

        # build a LostConnection last will, encode it
        lost_connection = LostConnection(self.client)

        # set the last will
        response = self.mqtt_client.will_set(self.broadcast_topic, lost_connection.as_bytes(), 0, False)
        if _debug: MQTTClient._debug("    - will_set: %r", response)

    def on_connect(self, client, userdata, flags, rc):
        """Callback for when the client receives a CONNACK response from the server.
        """
        if _debug: MQTTClient._debug("on_connect %r %r %r %r", client, userdata, flags, rc)

        # we are connected
        self.mqtt_connected = True

    def on_disconnect(self, *args):
        if _debug: MQTTClient._debug("on_disconnect %r", args)

        # we are no longer connected
        self.mqtt_connected = False

    def on_subscribe(self, client, userdata, mid, granted_qos):
        if _debug: MQTTClient._debug("on_subscribe %r %r %r %r", client, userdata, mid, granted_qos)

    def on_message(self, client, userdata, msg):
        """Callback for when a PUBLISH message is received from the server.
        """
        if _debug: MQTTClient._debug("on_message %r, %s", msg.topic, btox(msg.payload, '.'))

        # wrap it up and decode it
        pdu = PDU(msg.payload)
        bvlpdu = BVLPDU()
        bvlpdu.decode(pdu)
        if _debug: MQTTClient._debug("    - bvlpdu: %r", bvlpdu)

        # decode the next layer
        xpdu = bvl_pdu_types[bvlpdu.bvlciFunction]()
        xpdu.decode(bvlpdu)
        if _debug: MQTTClient._debug("    - xpdu: %r", xpdu)

        if isinstance(xpdu, OriginalUnicastNPDU):
            # from ourselves?
            if xpdu.bvlciAddress == self.client:
                if _debug: MQTTClient._debug("    - from ourselves")
                return

            # build a PDU with the client address
            ypdu = PDU(xpdu.pduData, source=xpdu.bvlciAddress, destination=self.client, user_data=xpdu.pduUserData)
            if _debug: MQTTClient._debug("    - upstream ypdu: %r", ypdu)

            deferred(self.response, ypdu)

        elif isinstance(xpdu, OriginalBroadcastNPDU):
            # from ourselves?
            if xpdu.bvlciAddress == self.client:
                if _debug: MQTTClient._debug("    - from ourselves")
                return

            # build a PDU with a local broadcast address
            ypdu = PDU(xpdu.pduData, source=xpdu.bvlciAddress, destination=LocalBroadcast(), user_data=xpdu.pduUserData)
            if _debug: MQTTClient._debug("    - upstream ypdu: %r", ypdu)

            deferred(self.response, ypdu)

    def on_publish(self, client, userdata, mid):
        """Callback for when the data is published."""
        if _debug: MQTTClient._debug("on_publish ...")

    def on_unsubscribe(self, client, userdata, mid):
        if _debug: MQTTClient._debug("on_unsubscribe ...")

    def indication(self, pdu):
        if _debug: MQTTClient._debug("indication %r", pdu)

        # check for local stations
        if pdu.pduDestination.addrType == Address.localStationAddr:
            # make an original unicast PDU
            xpdu = OriginalUnicastNPDU(self.client, pdu, user_data=pdu.pduUserData)
            if _debug: MQTTClient._debug("    - original unicast xpdu: %r", xpdu)

            destination_topic = self.lan + '/' + str_address(pdu.pduDestination)
            if _debug: MQTTClient._debug("    - destination_topic: %r", destination_topic)

            # send it to the address
            response = self.mqtt_client.publish(destination_topic, xpdu.as_bytes())
            if _debug: MQTTClient._debug("    - publish: %r", response)

        # check for broadcasts
        elif pdu.pduDestination.addrType == Address.localBroadcastAddr:
            # make an original broadcast PDU
            xpdu = OriginalBroadcastNPDU(self.client, pdu, user_data=pdu.pduUserData)
            if _debug: MQTTClient._debug("    - original broadcast xpdu: %r", xpdu)

            # send it to the address
            response = self.mqtt_client.publish(self.broadcast_topic, xpdu.as_bytes())
            if _debug: MQTTClient._debug("    - publish: %r", response)

        else:
            MQTTClient._warning("invalid destination address: %r", pdu.pduDestination)

    def sap_indication(self, blvpdu):
        if _debug: MQTTClient._debug("sap_indication %r", blvpdu)

    def sap_confirmation(self, blvpdu):
        if _debug: MQTTClient._debug("sap_confirmation %r", blvpdu)

#
#   MQTTServiceElement
#

@bacpypes_debugging
class MQTTServiceElement(ApplicationServiceElement):

    def __init__(self, eid=None):
        if _debug: MQTTServiceElement._debug("__init__ eid=%r", eid)
        ApplicationServiceElement.__init__(self, eid)

    def startup(self):
        if _debug: MQTTServiceElement._debug("startup")

        # service access point has client
        sap = self.elementService

        # queue up a start the connection process
        response = sap.mqtt_client.connect(sap.host, sap.port, sap.keepalive)
        if _debug: MQTTServiceElement._debug("    - connect: %r", response)

        # subscribe to both topics at the same time
        result, mid = sap.mqtt_client.subscribe([
            (sap.local_topic, 1),
            (sap.broadcast_topic, 1),
            ])
        if _debug: MQTTServiceElement._debug("    - local subscribe result, mid: %r, %r", result, mid)

        # build an Online PDU, encode it
        online = Online(sap.client)

        # we are ready to roll
        response = sap.mqtt_client.publish(sap.broadcast_topic, online.as_bytes())
        if _debug: MQTTServiceElement._debug("    - publish online: %r", response)

        # start the client loop
        response = sap.mqtt_client.loop_start()
        if _debug: MQTTServiceElement._debug("    - loop_start: %r", response)

    def shutdown(self):
        if _debug: MQTTServiceElement._debug("shutdown")

        # service access point has client
        sap = self.elementService

        # stop listening
        result, mid = sap.mqtt_client.unsubscribe(sap.broadcast_topic)
        if _debug: MQTTServiceElement._debug("    - broadcast unsubscribe result, mid: %r, %r", result, mid)

        result, mid = sap.mqtt_client.unsubscribe(sap.local_topic)
        if _debug: MQTTServiceElement._debug("    - local unsubscribe result, mid: %r, %r", result, mid)

        # build an Offline PDU, encode it
        offline = Offline(sap.client)

        # we are going away
        response = sap.mqtt_client.publish(sap.broadcast_topic, offline.as_bytes())
        if _debug: MQTTServiceElement._debug("    - publish offline: %r", response)

        # disconnect
        response = sap.mqtt_client.disconnect()
        if _debug: MQTTServiceElement._debug("    - disconnect: %r", response)

        # stop the client loop
        response = sap.mqtt_client.loop_stop()
        if _debug: MQTTServiceElement._debug("    - loop_stop: %r", response)


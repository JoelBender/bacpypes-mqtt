# BACpypes-MQTT

Exploring MQTT as a virtual link layer for BACnet

During the BACnet IT Working Group discussions about the upcoming BACnet/SC
addenda there was brief mention of using other kinds of message brokers
that are based on existing standards.  This demonstration project is to
explore the use of MQTT.

## Installation

1. Clone the repository:

2. Install the project dependancies in a virtual environment:

3. Duplicate the `BACpypes~.ini` template and modify the BACnet device
   settings:

    $ cp BACpypes~.ini BACpypes.ini

4. Run the console application, by default will use the `bacpypes-mqtt` network on the `test.mosquitto.org` broker.

    $ pipenv run python console_client.py

5. See who else is online:

    > whois

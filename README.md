# BACpypes-MQTT

[![Join the chat at https://gitter.im/bacpypes-mqtt/Lobby](https://badges.gitter.im/bacpypes-mqtt/Lobby.svg)](https://gitter.im/bacpypes-mqtt/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Exploring MQTT as a virtual link layer for BACnet

During the BACnet IT Working Group discussions about the upcoming BACnet/SC
addenda there was brief mention of using other kinds of message brokers
that are based on existing standards.  This demonstration project is to
explore the use of MQTT.

## Installation

1. Clone the repository:

```bash
$ git clone https://github.com/JoelBender/bacpypes-mqtt.git
```

2. Install the project dependancies in a virtual environment:

```bash
$ cd bacpypes-mqtt
$ pipenv install
```

3. Duplicate the `BACpypes~.ini` template and modify the BACnet device
   settings:

```bash
$ cp BACpypes~.ini BACpypes.ini
$ vi BACpypes.ini
```

4. Run the console application, by default will use the `bacpypes-mqtt` network on the `test.mosquitto.org` broker.

```bash
$ pipenv run python console_client.py
```

5. See who else is online:

```bash
> whois
```

For more information see the [wiki](https://github.com/JoelBender/bacpypes-mqtt/wiki).

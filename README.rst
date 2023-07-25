============
bluetti_mqtt
============

This tool provides a monitor for Bluetti power stations.
All data will be stored in a mariaDB.

Credits:
The underlying groundwork (scanning, discovery and the bluetooth-stuff) is taken from https://github.com/warhammerkid/bluetti_mqtt.

Installation
------------

.. code-block:: bash

    $ pip install bluetti_monitor

Usage
-----

.. code-block:: bash

    $ bluetti-monitor --scan
    Found AC3001234567890123: address 00:11:22:33:44:55
    $ bluetti-monitor --dbuser {mariaDBuser} --dbpass {mariaDBpass} --interval 20 00:11:22:33:44:55

If your mariaDB is not running on localhost:3306, host and port can be specified with --dbhost and --dbport



If you have multiple devices within bluetooth range, you can monitor all of
them with just a single command. We can only talk to one device at a time, so
you may notice some irregularity in the collected data, especially if you have
not set an interval.
I have not tested this and taken it directly from the source-repository, since I do not have access to more than one bluetti.
.. code-block:: bash

    $ bluetti-mqtt --broker [MQTT_BROKER_HOST] 00:11:22:33:44:55 00:11:22:33:44:66

Background Service
------------------

If you are running on a platform with systemd, you can use the following as a
template. It should be placed in ``/etc/systemd/system/bluetti-monitor.service``.
Once you've written the file, you'll need to run
``sudo systemctl start bluetti-monitor``. If you want it to run automatically after
rebooting, you'll also need to run ``sudo systemctl enable bluetti-monitor``.

.. code-block:: bash

    [Unit]
    Description=Bluetti Monitor
    After=network.target
    StartLimitIntervalSec=0

    [Service]
    Type=simple
    Restart=always
    RestartSec=30
    TimeoutStopSec=15
    User=bluePi
    ExecStart=/home/your_username_here/.local/bin/bluetti-monitor --dbuser {dbusername} --dbpass {dbpass} --interval 20 00:00:00:00:00:00


    [Install]
    WantedBy=multi-user.target


Reverse Engineering
-------------------

For research purposes you can also use the ``bluetti-logger`` command to poll
the device and log in a standardised format.

.. code-block:: bash

    $ bluetti-logger --log the-log-file.log 00:11:22:33:44:55

While the logger is running, change settings on the device and take note of the
time when you made the change, waiting ~ 1 minute between changes. Note that
not every setting that can be changed on the device can be changed over
bluetooth.

If you're looking to add support to control something that the app can change
but cannot be changed directly from the device screen, both iOS and Android
support collecting bluetooth logs from running apps. Additionally, with the
correct hardware Wireshark can be used to collect logs. With these logs and a
report of what commands were sent at what times, this data can be used to
reverse engineer support.

For supporting new devices, the ``bluetti-discovery`` command is provided. It
will scan from 0 to 12500 assuming MODBUS-over-Bluetooth. This will take a
while and requires that the scanned device be in close Bluetooth range for
optimal performance.

.. code-block:: bash

    $ bluetti-discovery --scan
    Found AC3001234567890123: address 00:11:22:33:44:55
    $ bluetti-discovery --log the-log-file.log 00:11:22:33:44:55
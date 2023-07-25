import asyncio
from dataclasses import dataclass
from enum import auto, Enum, unique
import json
import logging
import re
from typing import List, Optional

import mariadb
from paho.mqtt.client import MQTTMessage
from bluetti_monitor.bus import CommandMessage, EventBus, ParserMessage
from bluetti_monitor.core import BluettiDevice, DeviceCommand


@unique
class MessageFieldType(Enum):
    NUMERIC = auto()
    BOOL = auto()
    ENUM = auto()
    BUTTON = auto()


@dataclass(frozen=True)
class MessageFieldConfig:
    type: MessageFieldType
    setter: bool
    advanced: bool  # Do not export by default to Home Assistant
    home_assistant_extra: dict
    id_override: Optional[str] = None  # Used to override Home Assistant field id


@dataclass(frozen=True)
class DBTableConfig:
    table_name: str
    fields: [str]


COMMAND_TOPIC_RE = re.compile(r'^bluetti/command/(\w+)-(\d+)/([a-z_]+)$')
DATABASE_TABLES = {
    'general_data':
        DBTableConfig(
            table_name="general_data",
            fields=['device_type', 'serial_number', 'arm_version', 'dsp_version', 'dc_input_power', 'ac_input_power',
                    'ac_output_power', 'dc_output_power', 'power_generation', 'total_battery_percent', 'ac_output_on',
                    'dc_output_on']
        ),
    'internal_data':
        DBTableConfig(
            table_name="internal_data",
            fields=['ac_output_mode', 'internal_ac_voltage', 'internal_current_one', 'internal_power_one',
                    'internal_ac_frequency', 'internal_current_two', 'internal_power_two', 'ac_input_voltage',
                    'internal_current_three', 'internal_power_three', 'ac_input_frequency', 'internal_dc_input_voltage',
                    'internal_dc_input_power', 'internal_dc_input_current']
        ),
    'setting_data':
        DBTableConfig(
            table_name="settings_data",
            fields=['ups_mode', 'split_phase_on', 'split_phase_machine_mode', 'pack_num', 'ac_output_on',
                    'dc_output_on', 'grid_charge_on', 'time_control_on', 'battery_range_start', 'battery_range_end',
                    'bluetooth_connected', 'auto_sleep_mode']
        ),
    'battery_pack_data':
        DBTableConfig(
            table_name="battery_pack_data",
            fields=['pack_num_max', 'total_battery_voltage', 'pack_num', 'pack_voltage', 'pack_battery_percent',
                    'cell_voltages']
        )
}
NORMAL_DEVICE_FIELDS = {
    'dc_input_power': MessageFieldConfig(
        type=MessageFieldType.NUMERIC,
        setter=False,
        advanced=False,
        home_assistant_extra={
            'name': 'DC Input Power',
            'unit_of_measurement': 'W',
            'device_class': 'power',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'ac_input_power': MessageFieldConfig(
        type=MessageFieldType.NUMERIC,
        setter=False,
        advanced=False,
        home_assistant_extra={
            'name': 'AC Input Power',
            'unit_of_measurement': 'W',
            'device_class': 'power',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'ac_output_power': MessageFieldConfig(
        type=MessageFieldType.NUMERIC,
        setter=False,
        advanced=False,
        home_assistant_extra={
            'name': 'AC Output Power',
            'unit_of_measurement': 'W',
            'device_class': 'power',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'dc_output_power': MessageFieldConfig(
        type=MessageFieldType.NUMERIC,
        setter=False,
        advanced=False,
        home_assistant_extra={
            'name': 'DC Output Power',
            'unit_of_measurement': 'W',
            'device_class': 'power',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'power_generation': MessageFieldConfig(
        type=MessageFieldType.NUMERIC,
        setter=False,
        advanced=False,
        home_assistant_extra={
            'name': 'Total Power Generation',
            'unit_of_measurement': 'kWh',
            'device_class': 'energy',
            'state_class': 'total_increasing',
        }
    ),
    'total_battery_percent': MessageFieldConfig(
        type=MessageFieldType.NUMERIC,
        setter=False,
        advanced=False,
        home_assistant_extra={
            'name': 'Total Battery Percent',
            'unit_of_measurement': '%',
            'device_class': 'battery',
            'state_class': 'measurement',
        }
    ),
    'ac_output_on': MessageFieldConfig(
        type=MessageFieldType.BOOL,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'AC Output',
            'device_class': 'outlet',
        }
    ),
    'dc_output_on': MessageFieldConfig(
        type=MessageFieldType.BOOL,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'DC Output',
            'device_class': 'outlet',
        }
    ),
    'ac_output_mode': MessageFieldConfig(
        type=MessageFieldType.ENUM,
        setter=False,
        advanced=True,
        home_assistant_extra={
            'name': 'AC Output Mode',
        }
    ),
    'internal_ac_voltage': MessageFieldConfig(
        type=MessageFieldType.NUMERIC,
        setter=False,
        advanced=True,
        home_assistant_extra={
            'name': 'Internal AC Voltage',
            'unit_of_measurement': 'V',
            'device_class': 'voltage',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'internal_current_one': MessageFieldConfig(
        type=MessageFieldType.NUMERIC,
        setter=False,
        advanced=True,
        home_assistant_extra={
            'name': 'Internal Current Sensor 1',
            'unit_of_measurement': 'A',
            'device_class': 'current',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'internal_power_one': MessageFieldConfig(
        type=MessageFieldType.NUMERIC,
        setter=False,
        advanced=True,
        home_assistant_extra={
            'name': 'Internal Power Sensor 1',
            'unit_of_measurement': 'W',
            'device_class': 'power',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'internal_ac_frequency': MessageFieldConfig(
        type=MessageFieldType.NUMERIC,
        setter=False,
        advanced=True,
        home_assistant_extra={
            'name': 'Internal AC Frequency',
            'unit_of_measurement': 'Hz',
            'device_class': 'frequency',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'internal_current_two': MessageFieldConfig(
        type=MessageFieldType.NUMERIC,
        setter=False,
        advanced=True,
        home_assistant_extra={
            'name': 'Internal Current Sensor 2',
            'unit_of_measurement': 'A',
            'device_class': 'current',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'internal_power_two': MessageFieldConfig(
        type=MessageFieldType.NUMERIC,
        setter=False,
        advanced=True,
        home_assistant_extra={
            'name': 'Internal Power Sensor 2',
            'unit_of_measurement': 'W',
            'device_class': 'power',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'ac_input_voltage': MessageFieldConfig(
        type=MessageFieldType.NUMERIC,
        setter=False,
        advanced=True,
        home_assistant_extra={
            'name': 'AC Input Voltage',
            'unit_of_measurement': 'V',
            'device_class': 'voltage',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'internal_current_three': MessageFieldConfig(
        type=MessageFieldType.NUMERIC,
        setter=False,
        advanced=True,
        home_assistant_extra={
            'name': 'Internal Current Sensor 3',
            'unit_of_measurement': 'A',
            'device_class': 'current',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'internal_power_three': MessageFieldConfig(
        type=MessageFieldType.NUMERIC,
        setter=False,
        advanced=True,
        home_assistant_extra={
            'name': 'Internal Power Sensor 3',
            'unit_of_measurement': 'W',
            'device_class': 'power',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'ac_input_frequency': MessageFieldConfig(
        type=MessageFieldType.NUMERIC,
        setter=False,
        advanced=True,
        home_assistant_extra={
            'name': 'AC Input Frequency',
            'unit_of_measurement': 'Hz',
            'device_class': 'frequency',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'total_battery_voltage': MessageFieldConfig(
        type=MessageFieldType.NUMERIC,
        setter=False,
        advanced=True,
        home_assistant_extra={
            'name': 'Total Battery Voltage',
            'unit_of_measurement': 'V',
            'device_class': 'voltage',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'total_battery_current': MessageFieldConfig(
        type=MessageFieldType.NUMERIC,
        setter=False,
        advanced=True,
        home_assistant_extra={
            'name': 'Total Battery Current',
            'unit_of_measurement': 'A',
            'device_class': 'current',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'ups_mode': MessageFieldConfig(
        type=MessageFieldType.ENUM,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'UPS Working Mode',
            'options': ['CUSTOMIZED', 'PV_PRIORITY', 'STANDARD', 'TIME_CONTROL'],
        }
    ),
    'split_phase_on': MessageFieldConfig(
        type=MessageFieldType.BOOL,
        setter=False,  # For safety purposes, I'm not exposing this as a setter
        advanced=False,
        home_assistant_extra={
            'name': 'Split Phase',
        }
    ),
    'split_phase_machine_mode': MessageFieldConfig(
        type=MessageFieldType.ENUM,
        setter=False,  # For safety purposes, I'm not exposing this as a setter
        advanced=False,
        home_assistant_extra={
            'name': 'Split Phase Machine',
        }
    ),
    'grid_charge_on': MessageFieldConfig(
        type=MessageFieldType.BOOL,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'Grid Charge',
        }
    ),
    'time_control_on': MessageFieldConfig(
        type=MessageFieldType.BOOL,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'Time Control',
        }
    ),
    'battery_range_start': MessageFieldConfig(
        type=MessageFieldType.NUMERIC,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'Battery Range Start',
            'step': 1,
            'min': 0,
            'max': 100,
            'unit_of_measurement': '%',
        }
    ),
    'battery_range_end': MessageFieldConfig(
        type=MessageFieldType.NUMERIC,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'Battery Range End',
            'step': 1,
            'min': 0,
            'max': 100,
            'unit_of_measurement': '%',
        }
    ),
    'led_mode': MessageFieldConfig(
        type=MessageFieldType.ENUM,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'LED Mode',
            'icon': 'mdi:lightbulb',
            'options': ['LOW', 'HIGH', 'SOS', 'OFF'],
        }
    ),
    'power_off': MessageFieldConfig(
        type=MessageFieldType.BUTTON,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'Power Off',
            'payload_press': 'ON',
        }
    ),
    'auto_sleep_mode': MessageFieldConfig(
        type=MessageFieldType.ENUM,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'Screen Auto Sleep Mode',
            'icon': 'mdi:sleep',
            'options': ['THIRTY_SECONDS', 'ONE_MINUTE', 'FIVE_MINUTES', 'NEVER'],
        }
    ),
    'eco_on': MessageFieldConfig(
        type=MessageFieldType.BOOL,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'ECO',
            'icon': 'mdi:sprout',
        }
    ),
    'eco_shutdown': MessageFieldConfig(
        type=MessageFieldType.ENUM,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'ECO Shutdown',
            'icon': 'mdi:sprout',
            'options': ['ONE_HOUR', 'TWO_HOURS', 'THREE_HOURS', 'FOUR_HOURS'],
        }
    ),
    'charging_mode': MessageFieldConfig(
        type=MessageFieldType.ENUM,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'Charging Mode',
            'icon': 'mdi:battery-charging',
            'options': ['STANDARD', 'SILENT', 'TURBO'],
        }
    ),
    'power_lifting_on': MessageFieldConfig(
        type=MessageFieldType.BOOL,
        setter=True,
        advanced=False,
        home_assistant_extra={
            'name': 'Power Lifting',
            'icon': 'mdi:arm-flex',
        }
    ),
}
DC_INPUT_FIELDS = {
    'dc_input_voltage1': MessageFieldConfig(
        type=MessageFieldType.NUMERIC,
        setter=False,
        advanced=False,
        home_assistant_extra={
            'name': 'DC Input Voltage 1',
            'unit_of_measurement': 'V',
            'device_class': 'voltage',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'dc_input_power1': MessageFieldConfig(
        type=MessageFieldType.NUMERIC,
        setter=False,
        advanced=False,
        home_assistant_extra={
            'name': 'DC Input Power 1',
            'unit_of_measurement': 'W',
            'device_class': 'power',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
    'dc_input_current1': MessageFieldConfig(
        type=MessageFieldType.NUMERIC,
        setter=False,
        advanced=False,
        home_assistant_extra={
            'name': 'DC Input Current 1',
            'unit_of_measurement': 'A',
            'device_class': 'current',
            'state_class': 'measurement',
            'force_update': True,
        }
    ),
}


class MonitorService:
    devices: List[BluettiDevice]
    message_queue: asyncio.Queue

    def __init__(self, bus: EventBus, hostname: str, port: int, uname: str, passwd: str):
        self.bus = bus
        self.devices = []
        self.db_hostname = hostname
        self.db_port = port
        self.db_uname = uname
        self.db_passwd = passwd
        self.conn = self.initDB()

    def initDB(self):
        try:
            conn = mariadb.connect(
                user=self.db_uname,
                password=self.db_passwd,
                host=self.db_hostname,
                port=self.db_port,
                database="bluetti_db"
            )
            logging.info("successfully connected to database")
            return conn
        except mariadb.Error as e:
            logging.error(f'failed to connect to database')
            return None

    async def run(self):
        try:
            while True:
                # Connect to event bus
                self.message_queue = asyncio.Queue()
                self.bus.add_parser_listener(self.handle_message)

                # Handle pub/sub
                await asyncio.gather(
                    self._handle_commands(),
                    self._handle_messages()
                )
        except asyncio.CancelledError as e:
            self.conn.close()
            logging.info("Closed database connection")

    async def handle_message(self, msg: ParserMessage):
        await self.message_queue.put(msg)

    async def _handle_commands(self):
        pass
        # async with client.filtered_messages('bluetti/command/#') as messages:
        #     await client.subscribe('bluetti/command/#')
        #     async for mqtt_message in messages:
        #         await self._handle_command(mqtt_message)

    async def _handle_messages(self):
        while True:
            msg: ParserMessage = await self.message_queue.get()
            if msg.device not in self.devices:
                await self._init_device(msg.device)
            await self._handle_message(msg)
            self.message_queue.task_done()

    async def _init_device(self, device: BluettiDevice):
        # Register device
        self.devices.append(device)


    async def _handle_command(self, mqtt_message: MQTTMessage):
        # Parse the mqtt_message.topic
        m = COMMAND_TOPIC_RE.match(mqtt_message.topic)
        if not m:
            logging.warn(f'unknown command topic: {mqtt_message.topic}')
            return

        # Find the matching device for the command
        device = next((d for d in self.devices if d.type == m[1] and d.sn == m[2]), None)
        if not device:
            logging.warn(f'unknown device: {m[1]} {m[2]}')
            return

        # Check if the device supports setting this field
        if not device.has_field_setter(m[3]):
            logging.warn(f'Received command for unknown topic: {m[3]} - {mqtt_message.topic}')
            return

        cmd: DeviceCommand = None
        if m[3] in NORMAL_DEVICE_FIELDS:
            field = NORMAL_DEVICE_FIELDS[m[3]]
            if field.type == MessageFieldType.ENUM:
                value = mqtt_message.payload.decode('ascii')
                cmd = device.build_setter_command(m[3], value)
            elif field.type == MessageFieldType.BOOL or field.type == MessageFieldType.BUTTON:
                value = mqtt_message.payload == b'ON'
                cmd = device.build_setter_command(m[3], value)
            elif field.type == MessageFieldType.NUMERIC:
                value = int(mqtt_message.payload.decode('ascii'))
                cmd = device.build_setter_command(m[3], value)
            else:
                raise AssertionError(f'unexpected enum type: {field.type}')
        else:
            logging.warn(f'Received command for unhandled topic: {m[3]} - {mqtt_message.topic}')
            return

        await self.bus.put(CommandMessage(device, cmd))

    async def _handle_message(self, msg: ParserMessage):
        logging.debug(f'Got a message from {msg.device}: {msg.parsed}')
        device_name = f'{msg.device.type}-{msg.device.sn}'

        for tableName, tableConfig in DATABASE_TABLES.items():
            valueOrder = ""
            values = [device_name]

            for name, value in msg.parsed.items():
                if name not in tableConfig.fields:
                    break
                valueOrder += name + ", "
                # cell voltages are passed as an array and handled special
                if name == 'cell_voltages':
                    value = [str(v) for v in value]
                values.append(str(value) if type(value) != type(True) else value)
            else:

                query = f'INSERT INTO {tableConfig.table_name} (device, {valueOrder[:-2]}) VALUES ({("%s, " * len(values))[:-2]})'

                cursor = self.conn.cursor()
                cursor.execute(query, values)
                cursor.close()
                self.conn.commit()
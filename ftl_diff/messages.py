
from collections import namedtuple
import yaml
import json
import itertools
import datetime


def sequence():
    '''
    Sequence numbers are associated with a stream.  They are used to determine
    the order of the messages sent in a stream.
    '''
    return itertools.count(1)


def now():
    '''
    Returns a string formatted timestamp for now in UTC time.
    '''
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def serialize(message):
    '''
    Serializes a message to YAML format.
    '''
    return [message.__class__.__name__.encode(), yaml.dump(dict(message._asdict())).encode()]


def json_serialize(message):
    '''
    Serializes a message to JSON format.
    '''
    return json.dumps([message.__class__.__name__, dict(message._asdict())]).encode()


def json_deserialize(message):
    '''
    Deserializes a message from JSON format.
    '''
    data = json.loads(message)
    if isinstance(data, list):
        msg_type = data[0]
        msg_data = data[1]
        if msg_type in msg_types:
            try:
                return msg_types[msg_type](**msg_data)
            except BaseException as e:
                print(e)
                raise
    return None


Hello = namedtuple('Hello', ['seq_num', 'timestamp'])
FSMState = namedtuple('FSMState', ['seq_num', 'timestamp', 'state'])
Diff = namedtuple('Diff', ['seq_num', 'timestamp', 'diff'])
ValidationResult = namedtuple('ValidationResult', ['seq_num', 'timestamp', 'host', 'result'])
ValidationTask = namedtuple(
    'ValidationTask', ['seq_num', 'timestamp', 'host', 'task_action', 'result'])
Stdout = namedtuple('Stdout', ['seq_num', 'timestamp', 'stdout'])


DesiredState = namedtuple('DesiredState', ['seq_num', 'timestamp', 'id', 'client_id', 'desired_state'])
ActualState = namedtuple('ActualState', ['id', 'client_id', 'actual_state'])
Poll = namedtuple('Poll', [])
Complete = namedtuple('Complete', [])
Difference = namedtuple('Difference', [])
NoDifference = namedtuple('NoDifference', [])
Success = namedtuple('Success', [])
Failure = namedtuple('Failure', [])

Inventory = namedtuple('Inventory', ['seq_num', 'timestamp', 'inventory'])
Rules = namedtuple('Rules', ['seq_num', 'timestamp', 'rules'])

Control = namedtuple('Control', ['seq_num', 'timestamp', 'id'])
System = namedtuple('System', ['id', 'control_id'])
Monitor = namedtuple('Monitor', ['id', 'system_id', 'control_id'])

DesiredSystemState = namedtuple(
    'DesiredSystemState', ['id', 'client_id', 'desired_state'])

Shutdown = namedtuple('Shutdown', [])

ServiceInstance = namedtuple('ServiceInstance', ['id',
                                                 'service_id',
                                                 'created_at',
                                                 'deleted_at',
                                                 'name',
                                                 'config',
                                                 'inventory',
                                                 'inventory_id',
                                                 'collection',
                                                 'service_name',
                                                 'schema_name',
                                                 'rules_name',
                                                 'status'])

msg_types = {x.__name__: x for x in [
    DesiredState, ActualState, Hello, Control, ServiceInstance]}

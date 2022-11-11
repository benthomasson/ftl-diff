

from ftl_diff.messages import json_serialize, json_deserialize, Hello, Control
from ftl_diff.messages import sequence, now

import datetime


def test_hello():
    t = now()
    s = json_serialize(Hello(0, t))
    o = json_deserialize(s)
    assert o == Hello(0, t)


def test_control():
    t = now()
    s = json_serialize(Control(0, t, 5))
    o = json_deserialize(s)
    assert o == Control(0, t, 5)


def test_sequence():
    s = sequence()
    assert next(s) == 1
    assert next(s) == 2


def test_now():
    t = now()
    assert isinstance(t, str)
    assert datetime.datetime.fromisoformat(t)

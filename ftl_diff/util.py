
from itertools import count
import yaml
import os

# Escape the pattern with a limited set of regexp special characters
# To allow some of the regexps to be used.
# This might now work out in the long term
# We might want to use glob pattern matching or something simple like that
_special_chars_map = {i: '\\' + chr(i) for i in b'()[]{}-|^$.&~# \t\n\r\v\f'}


def escape(pattern):
    return pattern.translate(_special_chars_map)


def make_matcher(pattern):
    pattern = escape(pattern)
    return f'^({pattern}).*$'


def ensure_directory(d):
    if not os.path.exists(d):
        os.mkdir(d)


def check_state(state):
    yaml.safe_load(state)


class ConsoleTraceLog(object):

    def __init__(self):
        self.counter = count(start=1, step=1)

    def trace_order_seq(self):
        return next(self.counter)

    def send_trace_message(self, message):
        print(message)


def build_rule_selector(dotted_selector):
    '''
    The dotted selector should have the form of:
        root
        root.key
        root.key[.key]...
        root.key.index

    Where [.key]... means a repeating element
    index is a special key that means a list index
    root is a special key that only matches the root
    of the state.

    This function will return a regular expression
    that can be fed into deepdiff.extract.
    '''

    selector = []

    parts = dotted_selector.split('.')

    if parts[0] == "root":
        selector.append('root')
        start = 1
    elif parts[0] == "node":
        selector.append('node')
        start = 1
    else:
        start = 0

    for part in parts[start:]:
        if part == "index":
            selector.append(r'[\d+]')
        else:
            selector.append(f"['{part}']")

    return "".join(selector)


build_inventory_selector = build_rule_selector

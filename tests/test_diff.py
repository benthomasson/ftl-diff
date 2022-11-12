from deepdiff import DeepDiff, extract

from functools import partial

import yaml
import re

from typing import NamedTuple, Optional, Any

UnorderedDeepDiff = partial(DeepDiff, ignore_order=True)


class Action(NamedTuple):

    operation: str
    path: str
    old_value: Optional[Any] = None
    new_value: Optional[Any] = None


UPDATE = "update"
CREATE = "create"
DELETE = "delete"


def convert_to_actions(diff):
    for key in diff.keys():
        if key == "values_changed":
            for path, value in diff[key].items():
                yield Action(UPDATE, path, value["old_value"], value["new_value"])
        elif key == "iterable_item_added":
            for path, value in diff[key].items():
                yield Action(CREATE, path, None, value)
        elif key == "iterable_item_removed":
            for path, value in diff[key].items():
                yield Action(DELETE, path, value, None)


def test_same():
    t1 = {1: 1, 2: 2, 3: 3}
    t2 = t1.copy()
    assert UnorderedDeepDiff(t1, t2) == {}


def test_different():
    t1 = {1: 1, 2: 2, 3: 3}
    t2 = {1: 2, 2: 2, 3: 3}
    assert UnorderedDeepDiff(t1, t2) == {
        "values_changed": {"root[1]": {"new_value": 2, "old_value": 1}}
    }
    actions = convert_to_actions(UnorderedDeepDiff(t1, t2))
    assert next(actions) == (UPDATE, "root[1]", 1, 2)
    assert extract(t1, "root[1]") == 1
    assert extract(t2, "root[1]") == 2
    assert extract(t2, "root") == {1: 2, 2: 2, 3: 3}
    # Update root[1] from 1 to 2


def test_diff_tree_add_item():

    t1 = yaml.safe_load(
        """
    routers:
        - name: R1
        - name: R2
    """
    )

    t2 = yaml.safe_load(
        """
    routers:
        - name: R1
        - name: R2
        - name: R3
    """
    )

    assert UnorderedDeepDiff(t1, t2) == {
        "iterable_item_added": {"root['routers'][2]": {"name": "R3"}}
    }
    actions = convert_to_actions(UnorderedDeepDiff(t1, t2))
    assert next(actions) == (CREATE, "root['routers'][2]", None, {"name": "R3"})
    # Create root['routers'][2] with {'name': 'R3'}


def test_diff_tree_remove_item():

    t1 = yaml.safe_load(
        """
    routers:
        - name: R1
        - name: R2
    """
    )

    t2 = yaml.safe_load(
        """
    routers:
        - name: R1
    """
    )

    assert UnorderedDeepDiff(t1, t2) == {
        "iterable_item_removed": {"root['routers'][1]": {"name": "R2"}}
    }
    actions = convert_to_actions(UnorderedDeepDiff(t1, t2))
    assert next(actions) == (DELETE, "root['routers'][1]", {"name": "R2"}, None)
    # Delete root['routers'][1] with {'name': 'R2'}


def test_diff_tree_change_item():

    t1 = yaml.safe_load(
        """
    routers:
        - name: R1
        - name: R2
    """
    )

    t2 = yaml.safe_load(
        """
    routers:
        - name: R1
        - name: R3
    """
    )

    # Escape the pattern with a limited set of regexp special characters
    # To allow some of the regexps to be used.
    # This might now work out in the long term
    # We might want to use glob pattern matching or something simple like that
    _special_chars_map = {i: "\\" + chr(i) for i in b"()[]{}-|^$.&~# \t\n\r\v\f"}

    def escape(pattern):
        return pattern.translate(_special_chars_map)

    def make_matcher(pattern):
        pattern = escape(pattern)
        return f"^{pattern}.*"

    diff = UnorderedDeepDiff(t1, t2)
    assert list(diff["values_changed"].keys())[0] == "root['routers'][1]['name']"
    assert (
        re.escape("root['routers'][1]['name']") == r"root\['routers'\]\[1\]\['name'\]"
    )
    assert re.match(
        re.escape("root['routers'][1]['name']"), "root['routers'][1]['name']"
    )
    assert re.match(r"root\['routers'\]\[\d+\]\['name'\]", "root['routers'][1]['name']")
    assert re.match(r"root\['routers'\]\[.*\]\['name'\]", "root['routers'][1]['name']")
    assert re.match(
        r"^root\['routers'\]\[.*\]\['name'\].*", "root['routers'][1]['name']"
    )
    assert re.match(r"^root\['routers'\].*", "root['routers'][1]['name']")
    assert re.match(make_matcher("root['routers']"), "root['routers'][1]['name']")
    assert re.match(make_matcher(r"root['routers'][\d+]"), "root['routers'][1]['name']")
    assert diff == {
        "values_changed": {
            "root['routers'][1]['name']": {"new_value": "R3", "old_value": "R2"}
        }
    }

    actions = convert_to_actions(UnorderedDeepDiff(t1, t2))
    assert next(actions) == Action(operation='update', path="root['routers'][1]['name']", old_value='R2', new_value='R3')


def test_extract_and_modify():

    t1 = yaml.safe_load(
        """
    routers:
        - name: R1
        - name: R2
    """
    )

    assert extract(t1, "root['routers'][0]") == {"name": "R1"}
    extract(t1, "root['routers']")[0] = {"name": "R3"}
    assert extract(t1, "root['routers'][0]") == {"name": "R3"}

    # find parent and index of node

    # List case
    match = re.match(r"(.*)\[(\d+)\]$", "root['routers'][0]")
    assert match.groups()[0] == "root['routers']"
    assert match.groups()[1] == "0"
    parent = match.groups()[0]
    index = int(match.groups()[1])
    extract(t1, parent)[index] = {"name": "R4"}

    assert extract(t1, "root['routers']") == [{"name": "R4"}, {"name": "R2"}]

    # Dictionary case
    match = re.match(r"(.*)\['(\S+)'\]$", "root['routers']")
    assert match.groups()[0] == "root"
    assert match.groups()[1] == "routers"
    parent = match.groups()[0]
    index = match.groups()[1]
    extract(t1, parent)[index] = [{"name": "R5"}, {"name": "R2"}]

    assert extract(t1, "root") == {"routers": [{"name": "R5"}, {"name": "R2"}]}

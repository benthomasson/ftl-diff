"""
Usage:
    desired-state [options] from <initial-state.yml> to <new-state.yml>
    desired-state [options] validate <state.yml> <schema.yml>

Options:
    -h, --help              Show this page
    --debug                 Show debug logging
    --verbose               Show verbose logging
"""

from .validate import get_errors, validate
from .collection import split_collection_name, has_schema, load_schema
from .types import get_meta
from docopt import docopt
import yaml
import os
import sys
import logging
from deepdiff import DeepDiff, extract

from functools import partial

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


def convert_to_actions(diff, old_state, new_state):
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
        elif key == "dictionary_item_added":
            for path in diff[key]:
                value = extract(new_state, path)
                yield Action(CREATE, path, None, value)
        elif key == "dictionary_item_removed":
            for path in diff[key]:
                value = extract(old_state, path)
                yield Action(DELETE, path, value, None)


FORMAT = "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"
logging.basicConfig(
    filename="/tmp/desired_state.log", level=logging.DEBUG, format=FORMAT
)  # noqa
logging.debug("Logging started")
logging.debug("Loading runner")
logging.debug("Loaded runner")

logger = logging.getLogger("cli")


def main(args=None):
    """
    Main function for the CLI.
    """

    if args is None:
        args = sys.argv[1:]
    parsed_args = docopt(__doc__, args)
    if parsed_args["--debug"]:
        logging.basicConfig(level=logging.DEBUG)
    elif parsed_args["--verbose"]:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARNING)

    if parsed_args["from"] and parsed_args["to"]:
        return desired_state_from_to(parsed_args)
    elif parsed_args["validate"]:
        return desired_state_validate(parsed_args)
    else:
        assert False, "Update the docopt"


def validate_state(state):
    """
    Validates state using schema if it is found in the meta data of the state.
    """

    meta = get_meta(state)

    if meta.schema:
        if os.path.exists(meta.schema):
            with open(meta.schema) as f:
                schema = yaml.safe_load(f.read())
        elif has_schema(*split_collection_name(meta.schema)):
            schema = load_schema(*split_collection_name(meta.schema))
        else:
            schema = {}
        validate(state, schema)


def desired_state_from_to(parsed_args):
    """
    Calculates the difference in state from initial-state to new-state
    """

    with open(parsed_args["<initial-state.yml>"]) as f:
        initial_state = yaml.safe_load(f.read())

    with open(parsed_args["<new-state.yml>"]) as f:
        new_state = yaml.safe_load(f.read())

    actions = convert_to_actions(UnorderedDeepDiff(initial_state, new_state), initial_state, new_state)

    print(yaml.dump([action._asdict() for action in actions]))
    return 0


def desired_state_validate(parsed_args):
    """
    Validates a state using the schema and prints a list of errors in the state.
    """

    with open(parsed_args["<state.yml>"]) as f:
        state = yaml.safe_load(f.read())

    with open(parsed_args["<schema.yml>"]) as f:
        schema = yaml.safe_load(f.read())

    for error in get_errors(state, schema):
        print(error)
    else:
        return 0
    return 1


if __name__ == "__main__":
    main()

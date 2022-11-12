

"""
Usage:
    desired-state [options] from <initial-state.yml> to <new-state.yml>
    desired-state [options] validate <state.yml> <schema.yml>

Options:
    -h, --help              Show this page
    --debug                 Show debug logging
    --verbose               Show verbose logging
    --project-src=<d>       Copy project files this directory [default: .]
    --inventory=<i>         Inventory to use
    --cwd=<c>               Change working directory on start
"""

from .stream import WebsocketChannel, NullChannel
from .validate import get_errors, validate
from .collection import split_collection_name, has_schema, load_schema
from .types import get_meta
from getpass import getpass
from collections import defaultdict
from docopt import docopt
import yaml
import os
import sys
import logging


FORMAT = "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"
logging.basicConfig(filename='/tmp/desired_state.log', level=logging.DEBUG, format=FORMAT)  # noqa
logging.debug('Logging started')
logging.debug('Loading runner')
logging.debug('Loaded runner')

logger = logging.getLogger('cli')


def main(args=None):
    '''
    Main function for the CLI.
    '''

    if args is None:
        args = sys.argv[1:]
    parsed_args = docopt(__doc__, args)
    if parsed_args['--debug']:
        logging.basicConfig(level=logging.DEBUG)
    elif parsed_args['--verbose']:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARNING)

    if parsed_args['--cwd']:
        os.chdir(parsed_args['--cwd'])

    if parsed_args['from'] and parsed_args['to']:
        return desired_state_from_to(parsed_args)
    elif parsed_args['validate']:
        return desired_state_validate(parsed_args)
    else:
        assert False, 'Update the docopt'


def inventory(parsed_args, state):
    '''
    Loads an inventory
    '''

    meta = get_meta(state)

    if meta.inventory and os.path.exists(meta.inventory):
        print('inventory:', meta.inventory)
        with open(meta.inventory) as f:
            return f.read()
    elif not parsed_args['--inventory']:
        print('inventory:', 'localhost only')
        return "all:\n  hosts:\n    localhost: ansible_connection=local\n"
    else:
        print('inventory:', parsed_args['--inventory'])
        with open(parsed_args['--inventory']) as f:
            return f.read()


def validate_state(state):
    '''
    Validates state using schema if it is found in the meta data of the state.
    '''

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


def parse_options(parsed_args):

    secrets = defaultdict(str)

    if parsed_args['--ask-become-pass'] and not secrets['become']:
        secrets['become'] = getpass()

    if parsed_args['--stream']:
        stream = WebsocketChannel(parsed_args['--stream'])
    else:
        stream = NullChannel()

    project_src = os.path.abspath(
        os.path.expanduser(parsed_args['--project-src']))

    return secrets, project_src, stream


def desired_state_from_to(parsed_args):
    '''
    Calculates the differene in state from initial-state to new-state executes those changes and exits.
    '''

    secrets, project_src, stream = parse_options(parsed_args)

    threads = []

    if stream.thread:
        threads.append(stream.thread)

    with open(parsed_args['<initial-state.yml>']) as f:
        initial_desired_state = yaml.safe_load(f.read())

    validate_state(initial_desired_state)

    with open(parsed_args['<new-state.yml>']) as f:
        new_desired_state = f.read()

    validate_state(yaml.safe_load(new_desired_state))

    return 0


def desired_state_validate(parsed_args):
    '''
    Validates a state using the schema and prints a list of errors in the state.
    '''

    with open(parsed_args['<state.yml>']) as f:
        state = yaml.safe_load(f.read())

    with open(parsed_args['<schema.yml>']) as f:
        schema = yaml.safe_load(f.read())

    for error in get_errors(state, schema):
        print(error)
    else:
        return 0
    return 1


if __name__ == "__main__":
    main()

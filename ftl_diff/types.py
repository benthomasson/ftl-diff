
from collections import namedtuple

Meta = namedtuple('Meta', ['rules', 'schema', 'inventory'])


def get_meta(state):
    '''
    Gets the meta data from a state file.
    '''
    if not state:
        return Meta(None, None, None)
    elif 'meta' in state:
        return Meta(state['meta'].get('rules', None),
                    state['meta'].get('schema', None),
                    state['meta'].get('inventory', None))
    else:
        return Meta(None, None, None)

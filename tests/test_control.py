
from .util import load_system

def test_control_empty_system():
    x = load_system('empty_system', 'A')
    assert x is None

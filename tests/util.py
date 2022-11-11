import os
import yaml

HERE = os.path.abspath(os.path.dirname(__file__))


def load_rule(name):
    with open(os.path.join(HERE, 'rules', f'{name}.yml')) as f:
        return yaml.safe_load(f.read())


def load_state(name, version):
    with open(os.path.join(HERE, 'states', name, f'{version}.yml')) as f:
        return yaml.safe_load(f.read())


def load_system(name, version):
    with open(os.path.join(HERE, 'systems', name, f'{version}.yml')) as f:
        return yaml.safe_load(f.read())

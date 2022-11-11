
import os
import shutil
import subprocess
import yaml
from functools import lru_cache

ANSIBLE_GALAXY = shutil.which('ansible-galaxy')


@lru_cache
def find_collection(name):
    if ANSIBLE_GALAXY is None:
        raise Exception('ansible-galaxy is not installed')
    try:
        output = subprocess.check_output(
            [ANSIBLE_GALAXY, 'collection', 'list', name], stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError:
        return None
    output = output.decode()
    parts = name.split('.')
    for line in output.splitlines():
        if line.startswith('# '):
            location = line[2:]
            location = os.path.join(location, *parts)
            if os.path.exists(location):
                return location
    return None


def has_schema(collection, schema):
    if find_collection(collection) is None:
        return False
    return os.path.exists(os.path.join(find_collection(collection), 'schemas', schema) + ".yml")


def load_schema(collection, schema):
    if find_collection(collection) is None:
        return False
    location = os.path.join(find_collection(
        collection), 'schemas', schema) + ".yml"
    if not os.path.exists(location):
        raise Exception(
            f'Cannot find schema {schema} in {collection} at {location}')
    with open(location) as f:
        print(f'Loading schema from {location}')
        return yaml.safe_load(f.read())


def split_collection_name(collection_resource):
    collection, _, resource = collection_resource.rpartition(".")
    return collection, resource


def has_rules(collection, rules):
    return os.path.exists(os.path.join(find_collection(collection), 'rules', rules) + ".yml")


def load_rules(collection, rules):
    location = os.path.join(find_collection(
        collection), 'rules', rules) + ".yml"
    if not os.path.exists(location):
        raise Exception(
            f'Cannot find rules {rules} in {collection} at {location}')
    with open(location) as f:
        print(f'Loading rules from {location}')
        return yaml.safe_load(f.read())


def has_tasks(collection, tasks):
    return os.path.exists(os.path.join(find_collection(collection), 'tasks', tasks) + ".yml")


def load_tasks(collection, tasks):
    location = os.path.join(find_collection(
        collection), 'tasks', tasks) + ".yml"
    if not os.path.exists(location):
        raise Exception(
            f'Cannot find tasks {tasks} in {collection} at {location}')
    return location


def has_service(collection, service):
    return os.path.exists(os.path.join(find_collection(collection), 'services', service) + ".yml")


def load_service(collection, service):
    location = os.path.join(find_collection(collection),
                            'services', service) + ".yml"
    if not os.path.exists(location):
        raise Exception(
            f'Cannot find service {service} in {collection} at {location}')
    return location

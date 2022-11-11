

import re
from .util import make_matcher, build_rule_selector
from enum import Enum
from deepdiff import extract
from collections import OrderedDict


class Action(Enum):

    CREATE = 'CREATE'
    UPDATE = 'UPDATE'
    RETRIEVE = 'RETRIEVE'
    DELETE = 'DELETE'
    RENAME = 'RENAME'
    VALIDATE = 'VALIDATE'


ACTION_RULES = {Action.CREATE: 'create',
                Action.UPDATE: 'update',
                Action.RETRIEVE: 'retrieve',
                Action.DELETE: 'delete',
                Action.RENAME: 'rename',
                Action.VALIDATE: 'validate'}


def select_rules_recursive(diff, rules, current_desired_state, new_desired_state):

    matching_rules = []
    matchers = [(make_matcher(build_rule_selector(
        rule['rule_selector'])), rule) for rule in rules]

    for key, value in diff.get('values_changed', {}).items():
        for (matcher, rule) in matchers:
            match = re.match(matcher, key)
            if match:
                matching_rules.append(('values_changed', rule, match, value))

    for item in diff.get('dictionary_item_added', []):
        for (matcher, rule) in matchers:
            match = re.match(matcher, item)
            if match:
                matching_rules.append(
                    ('dictionary_item_added', rule, match, None))
            new_subtree = extract(new_desired_state, item)
            select_rules_recursive_helper(
                diff, matchers, matching_rules, item, new_subtree)

    for item in diff.get('dictionary_item_removed', []):
        for (matcher, rule) in matchers:
            match = re.match(matcher, item)
            if match:
                matching_rules.append(
                    ('dictionary_item_removed', rule, match, None))

    for item in diff.get('iterable_item_added', []):
        print(item)
        for (matcher, rule) in matchers:
            match = re.match(matcher, item)
            if match:
                matching_rules.append(
                    ('iterable_item_added', rule, match, None))

    for item in diff.get('iterable_item_removed', []):
        print(item)
        for (matcher, rule) in matchers:
            match = re.match(matcher, item)
            if match:
                matching_rules.append(
                    ('iterable_item_removed', rule, match, None))

    for key, value in diff.get('type_changes', {}).items():
        # Handles case in YAML where an empty list defaults to None type
        if value.get('old_type') == type(None) and value.get('new_type') == list:
            # Add a single new element to the key
            # TODO: this should probably loop over all the new elements in the list not just one
            key += '[0]'
        elif value.get('old_type') == list and value.get('new_type') == type(None):
            # Add a single new element to the key
            key += '[0]'
        for (matcher, rule) in matchers:
            match = re.match(matcher, key)
            if match:
                matching_rules.append(('type_changes', rule, match, None))

        # Handles case in YAML where an empty dict defaults to None type on the old state
        if value.get('old_type') == type(None) and value.get('new_type') == dict:
            # Try the matcher against all the keys in the dict
            for dict_key in value.get('new_value').keys():
                new_key = f"{key}['{dict_key}']"
                for (matcher, rule) in matchers:
                    match = re.match(matcher, new_key)
                    if match:
                        matching_rules.append(
                            ('type_changes', rule, match, None))
            select_rules_recursive_helper(
                diff, matchers, matching_rules, key, value.get('new_value'))

        # Handles case in YAML where an empty dict defaults to None type on the new state
        if value.get('old_type') == dict and value.get('new_type') == type(None):
            # Try the matcher against all the keys in the dict
            for dict_key in value.get('old_value').keys():
                old_key = f"{key}['{dict_key}']"
                for (matcher, rule) in matchers:
                    match = re.match(matcher, old_key)
                    if match:
                        matching_rules.append(
                            ('type_changes', rule, match, None))
            select_rules_recursive_helper(
                diff, matchers, matching_rules, key, value.get('old_value'))

    return matching_rules


def select_rules_recursive_helper(diff, matchers, matching_rules, path, value):

    for (matcher, rule) in matchers:
        match = re.match(matcher, path)
        if match:
            matching_rules.append(('subtree', rule, match, value))

    if type(value) is list:
        for i, item in enumerate(value):
            select_rules_recursive_helper(
                diff, matchers, matching_rules, f"{path}[{i}]", item)

    if type(value) is dict:
        for k, v in value.items():
            select_rules_recursive_helper(
                diff, matchers, matching_rules, f"{path}['{k}']", v)


def select_rules(diff, rules):
    matching_rules = []
    for rule in rules:
        matcher = make_matcher(build_rule_selector(rule['rule_selector']))
        for key, value in diff.get('values_changed', {}).items():
            match = re.match(matcher, key)
            if match:
                matching_rules.append(('values_changed', rule, match, value))
        for item in diff.get('dictionary_item_added', []):
            match = re.match(matcher, item)
            if match:
                matching_rules.append(
                    ('dictionary_item_added', rule, match, None))
        for item in diff.get('dictionary_item_removed', []):
            match = re.match(matcher, item)
            if match:
                matching_rules.append(
                    ('dictionary_item_removed', rule, match, None))
        for item in diff.get('iterable_item_added', []):
            match = re.match(matcher, item)
            if match:
                matching_rules.append(
                    ('iterable_item_added', rule, match, None))
        for item in diff.get('iterable_item_removed', []):
            match = re.match(matcher, item)
            if match:
                matching_rules.append(
                    ('iterable_item_removed', rule, match, None))
        for key, value in diff.get('type_changes', {}).items():
            # Handles case in YAML where an empty list defaults to None type
            if value.get('old_type') == type(None) and value.get('new_type') == list:
                # Add a single new element to the key
                # TODO: this should probably loop over all the new elements in the list not just one
                key += '[0]'
            elif value.get('old_type') == list and value.get('new_type') == type(None):
                # Add a single new element to the key
                key += '[0]'
            match = re.match(matcher, key)
            if match:
                matching_rules.append(('type_changes', rule, match, None))

            # Handles case in YAML where an empty dict defaults to None type
            if value.get('old_type') == type(None) and value.get('new_type') == dict:
                # Try the matcher against all the keys in the dict
                for dict_key in value.get('new_value').keys():
                    new_key = f"{key}['{dict_key}']"
                    match = re.match(matcher, new_key)
                    if match:
                        matching_rules.append(
                            ('type_changes', rule, match, None))

    return matching_rules


def deduplicate_rules(matching_rules):
    # Deduplicate the rules since some rules may match more than once when using recursive rule selection

    dedup_matching_rules = OrderedDict()

    for matching_rule in matching_rules:
        _, _, match, _ = matching_rule
        changed_subtree_path = match.groups()[0]
        if changed_subtree_path not in dedup_matching_rules:
            dedup_matching_rules[changed_subtree_path] = matching_rule

    dedup_matching_rules = list(dedup_matching_rules.values())

    return dedup_matching_rules


def get_rule_action_subtree(matching_rule, current_desired_state, new_desired_state):

    change_type, rule, match, value = matching_rule
    print('change_type', change_type)
    print('rule', rule)
    print('match', match)
    print('value', value)
    changed_subtree_path = match.groups()[0]
    print('changed_subtree_path', changed_subtree_path)
    try:
        new_subtree = extract(new_desired_state, changed_subtree_path)
        new_subtree_missing = False
    except (KeyError, IndexError, TypeError):
        new_subtree_missing = True
    try:
        old_subtree = extract(current_desired_state, changed_subtree_path)
        old_subtree_missing = False
    except (KeyError, IndexError, TypeError):
        old_subtree_missing = True
    print('new_subtree_missing', new_subtree_missing)
    print('old_subtree_missing', old_subtree_missing)

    if change_type == 'iterable_item_added':
        action = Action.CREATE
        subtree = new_subtree
    elif change_type == 'iterable_item_removed':
        action = Action.DELETE
        subtree = old_subtree
    elif new_subtree_missing is False and old_subtree_missing is False:
        action = Action.UPDATE
        subtree = new_subtree
    elif new_subtree_missing and old_subtree_missing is False:
        action = Action.DELETE
        subtree = old_subtree
    elif old_subtree_missing and new_subtree_missing is False:
        action = Action.CREATE
        subtree = new_subtree
    else:
        assert False, "Logic bug"
    print('action', action)

    return action, subtree

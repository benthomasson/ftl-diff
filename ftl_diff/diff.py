import os
import yaml
import tempfile
import shutil
import json
import re
import glob
import ansible_runner
from pprint import pprint
from deepdiff import DeepDiff, extract

from .rule import (
    select_rules_recursive,
    Action,
    ACTION_RULES,
    get_rule_action_subtree,
    deduplicate_rules,
)
from .util import ensure_directory, build_inventory_selector
from .messages import ValidationResult, ValidationTask, now, Stdout
from .collection import split_collection_name, has_tasks, load_tasks


def convert_diff(diff):
    """
    Converts the DeepDiff structure into a YAML serializable data structure.
    """

    print(diff)
    if "dictionary_item_added" in diff:
        diff["dictionary_item_added"] = [str(x) for x in diff["dictionary_item_added"]]
    if "dictionary_item_removed" in diff:
        diff["dictionary_item_removed"] = [
            str(x) for x in diff["dictionary_item_removed"]
        ]
    if "type_changes" in diff:
        diff["type_changes"] = [str(x) for x in diff["type_changes"]]
    diff = dict(diff)
    print(yaml.safe_dump(diff))
    return diff


class PlaybookRunner:

    """
    PlaybookRunner is responsible for setting up and running ansible-runner
    """

    def __init__(
        self,
        message_processor,
        new_desired_state,
        state_diff,
        destructured_vars_list,
        playbook,
        secrets,
        project_src,
        inventory,
    ):
        print("PlaybookRunner")
        self.message_processor = message_processor
        self.inventory = inventory
        self.secrets = secrets
        self.project_src = project_src
        self.new_desired_state = new_desired_state
        self.state_diff = convert_diff(state_diff)
        self.destructured_vars_list = destructured_vars_list
        self.playbook = playbook
        self.runner_thread = None
        self.shutdown_requested = False
        self.shutdown = False

    def run(self):

        self.build_project_directory()
        self.copy_files()
        self.build_project_subdirectories()
        self.write_settings()
        self.write_cmdline()
        self.write_passwords()
        self.write_state_vars()
        self.write_diff_vars()
        self.write_destructred_vars()
        self.write_playbook()
        self.write_inventory()
        self.start_ansible_playbook()
        return self.read_result()

    def build_project_directory(self):
        self.temp_dir = tempfile.mkdtemp(prefix="desired_state_playbook")
        print(self.temp_dir)
        ensure_directory(os.path.join(self.temp_dir, "env"))
        ensure_directory(os.path.join(self.temp_dir, "project"))

    def build_project_subdirectories(self):
        ensure_directory(os.path.join(self.temp_dir, "project", "roles"))

    def copy_files(self):
        src = os.path.abspath(self.project_src)
        dest = os.path.join(self.temp_dir, "project")
        src_files = os.listdir(src)
        for file_name in src_files:
            full_file_name = os.path.join(src, file_name)
            if os.path.isfile(full_file_name):
                shutil.copy(full_file_name, dest)
            if os.path.isdir(full_file_name):
                shutil.copytree(full_file_name, os.path.join(dest, file_name))

    def write_settings(self):
        with open(os.path.join(self.temp_dir, "env", "settings"), "w") as f:
            f.write(json.dumps(dict(idle_timeout=0, job_timeout=0)))

    def write_cmdline(self):
        with open(os.path.join(self.temp_dir, "env", "cmdline"), "w") as f:
            f.write("--ask-become-pass -v")

    def write_passwords(self):
        with open(os.path.join(self.temp_dir, "env", "passwords"), "w") as f:
            f.write(
                """---\n"SUDO password:": "{0}"\nBECOME password: "{0}"\n...""".format(
                    self.secrets["become"]
                )
            )

    def write_playbook(self):
        self.playbook_file = os.path.join(self.temp_dir, "project", "playbook.yml")
        playbook = self.playbook
        with open(self.playbook_file, "w") as f:
            f.write(yaml.safe_dump(playbook, default_flow_style=False))

    def write_state_vars(self):
        state_vars_file = os.path.join(self.temp_dir, "project", "state_vars.yml")
        with open(state_vars_file, "w") as f:
            f.write(yaml.safe_dump(self.new_desired_state, default_flow_style=False))
        for play in self.playbook:
            play["tasks"].insert(
                0,
                {
                    "include_vars": {"file": "state_vars.yml", "name": "state"},
                    "name": "include state_vars",
                },
            )

    def write_diff_vars(self):
        diff_vars_file = os.path.join(self.temp_dir, "project", "diff_vars.yml")
        with open(diff_vars_file, "w") as f:
            f.write(yaml.safe_dump(self.state_diff, default_flow_style=False))
        for play in self.playbook:
            play["tasks"].insert(
                0,
                {
                    "include_vars": {"file": "diff_vars.yml", "name": "diff"},
                    "name": "include diff_vars",
                },
            )

    def write_destructred_vars(self):
        for i, destructured_vars in enumerate(self.destructured_vars_list):
            diff_vars_file = os.path.join(
                self.temp_dir, "project", f"destructured_vars_{i}.yml"
            )
            with open(diff_vars_file, "w") as f:
                f.write(yaml.safe_dump(destructured_vars, default_flow_style=False))
        for i, play in enumerate(self.playbook):
            play["tasks"].insert(
                0,
                {
                    "include_vars": {"file": f"destructured_vars_{i}.yml"},
                    "name": "include destructured_vars",
                },
            )

    def write_inventory(self):
        print("inventory set to %s", self.inventory)
        with open(os.path.join(self.temp_dir, "inventory"), "w") as f:
            f.write(self.inventory)

    def start_ansible_playbook(self):
        # print('start_ansible_playbook')
        ansible_runner.run(
            private_data_dir=self.temp_dir,
            playbook="playbook.yml",
            quiet=True,
            debug=True,
            ignore_logging=True,
            cancel_callback=self.cancel_callback,
            finished_callback=self.finished_callback,
            event_handler=self.runner_process_message,
        )
        # print('finished ansible runner')
        print(self.temp_dir)

    def cancel_callback(self):
        # print('cancel_callback called')
        return self.shutdown_requested

    def finished_callback(self, runner):
        # print('finished_callback called')
        self.shutdown = True

    def runner_process_message(self, data):
        # if data.get('event', '') == 'runner_on_ok':
        # print("runner message:\n{}".format(pformat(data)))
        self.message_processor(data)
        # print(data.get('stdout', ''))

    def read_result(self):
        artifacts = glob.glob(os.path.join(self.temp_dir, "artifacts", "*-*-*-*-*"))
        if len(artifacts) != 1:
            # We cannot determine the result if there is 0 or more than 1 artifact directories
            return None
        artifacts = artifacts[0]
        if not os.path.isdir(artifacts):
            # If artifacts is not a directory we cannot determine the result
            return None
        rc = os.path.join(artifacts, "rc")
        if not os.path.exists(rc):
            # Cannot determine result if rc does not exist
            return None
        with open(rc) as f:
            return 0 == int(f.read())


def find_tasks(file_or_collection):

    if os.path.exists(file_or_collection):
        task_file = file_or_collection
    elif has_tasks(*split_collection_name(file_or_collection)):
        task_file = load_tasks(*split_collection_name(file_or_collection))
    else:
        raise Exception("No tasks found at f{file_or_collection}")
    return task_file


def desired_state_diff(
    monitor,
    secrets,
    project_src,
    current_desired_state,
    new_desired_state,
    rules,
    inventory,
    explain,
):
    """
    desired_state_diff creates playbooks and runs them with ansible-runner to implement the differences
    between two version of state: current_desired_state and new_desired_state.
    """

    # Find the difference between states

    diff = DeepDiff(current_desired_state, new_desired_state, ignore_order=True)
    print(diff)

    # Find matching rules

    matching_rules = select_rules_recursive(
        diff, rules["rules"], current_desired_state, new_desired_state
    )
    if explain:
        print("matching_rules")
        pprint(matching_rules)

    dedup_matching_rules = deduplicate_rules(matching_rules)

    if explain:
        print("dedup_matching_rules:")
        pprint(dedup_matching_rules)

    # Build up the set of ansible-runner executions to implement the changes using the rules

    ran_rules = []

    plays = []

    destructured_vars_list = []

    for matching_rule in dedup_matching_rules:
        change_type, rule, match, value = matching_rule
        changed_subtree_path = match.groups()[0]
        action, subtree = get_rule_action_subtree(
            matching_rule, current_desired_state, new_desired_state
        )
        print("action", action)

        print("rule action", rule.get(ACTION_RULES[action]))

        # Experiment: Build the vars using destructuring

        destructured_vars = {}

        for name, extract_path in rule.get("vars", {}).items():
            destructured_vars[name] = extract(subtree, extract_path)

        # Experiment: Make the subtree available as node
        destructured_vars["node"] = subtree

        print("destructured_vars", destructured_vars)

        # Determine the inventory to run on

        inventory_selector = build_inventory_selector(rule.get("inventory_selector"))
        if inventory_selector:
            try:
                inventory_name = extract(subtree, inventory_selector)
            except KeyError:
                raise Exception(f"Invalid inventory_selector {inventory_selector}")

        print("inventory_name", inventory_name)

        # Build a play using tasks or role from rule

        play = {
            "name": "{0} {1} {2}".format(
                ACTION_RULES[action], changed_subtree_path, inventory_name
            ),
            "hosts": inventory_name,
            "gather_facts": False,
            "tasks": [],
        }

        if "tasks" in rule.get(ACTION_RULES[action], {}):
            play["tasks"].append(
                {
                    "include_tasks": {
                        "file": find_tasks(rule.get(ACTION_RULES[action]).get("tasks"))
                    },
                    "name": "{0} {1}".format(
                        ACTION_RULES[action], changed_subtree_path
                    ),
                }
            )

        if "become" in rule:
            play["become"] = rule["become"]

        if explain:
            print(yaml.dump(play))
        else:

            # Run the action play

            plays.append(play)
            destructured_vars_list.append(destructured_vars)

            ran_rules.append((rule, changed_subtree_path, subtree, inventory_name))

    def runner_process_message(data):
        monitor.stream.put_message(Stdout(0, now(), data.get("stdout", "")))

    PlaybookRunner(
        runner_process_message,
        new_desired_state,
        diff,
        destructured_vars_list,
        plays,
        secrets,
        project_src,
        inventory,
    ).run()

    return ran_rules


def desired_state_discovery(
    monitor,
    secrets,
    project_src,
    current_desired_state,
    new_desired_state,
    ran_rules,
    inventory,
    explain,
):

    # Discovers the state of a subset of a system

    diff = DeepDiff(current_desired_state, new_desired_state, ignore_order=True)

    # deep copy
    new_discovered_state = yaml.safe_load(yaml.safe_dump(new_desired_state))

    plays = []

    destructured_vars_list = []
    discovered_rules = []

    for discovery_id, (
        rule,
        changed_subtree_path,
        subtree,
        inventory_name,
    ) in enumerate(ran_rules):

        # Experiment: Build the vars using destructuring
        destructured_vars = {}

        for name, extract_path in rule.get("vars", {}).items():
            destructured_vars[name] = extract(subtree, extract_path)

        # Experiment: Make the subtree available as node
        destructured_vars["node"] = subtree
        destructured_vars["discovery_id"] = discovery_id

        print("destructured_vars", destructured_vars)

        # Build a play using tasks or role from rule

        play = {
            "name": f"discovery for {inventory_name} discovery_id {discovery_id}",
            "hosts": inventory_name,
            "gather_facts": False,
            "tasks": [],
        }

        if "tasks" in rule.get(ACTION_RULES[Action.RETRIEVE], {}):
            play["tasks"].append(
                {
                    "include_tasks": {
                        "file": find_tasks(
                            rule.get(ACTION_RULES[Action.RETRIEVE]).get("tasks")
                        )
                    },
                    "name": "include retrieve",
                }
            )

            print(play)

            plays.append(play)
            destructured_vars_list.append(destructured_vars)
            discovered_rules.append([discovery_id, changed_subtree_path, subtree])

    if not plays:
        return new_discovered_state

    def runner_process_message(data):
        monitor.stream.put_message(Stdout(0, now(), data.get("stdout", "")))

    runner = PlaybookRunner(
        runner_process_message,
        new_desired_state,
        diff,
        destructured_vars_list,
        plays,
        secrets,
        project_src,
        inventory,
    )
    result = runner.run()

    if result:

        for discovery_id, changed_subtree_path, subtree in discovered_rules:
            update_discovered_state(
                new_discovered_state,
                runner.temp_dir,
                discovery_id,
                changed_subtree_path,
                subtree,
            )

    return new_discovered_state


def update_discovered_state(
    new_discovered_state, temp_dir, discovery_id, changed_subtree_path, subtree
):

    discovered_state_file = os.path.join(
        temp_dir, "project", f"discovered_state_{discovery_id}.yml"
    )
    if os.path.exists(discovered_state_file):
        with open(discovered_state_file) as f:
            discovered_subtree_state = yaml.safe_load(f.read())
            print(changed_subtree_path)
            print(yaml.safe_dump(discovered_subtree_state, default_flow_style=False))
            print(yaml.safe_dump(subtree, default_flow_style=False))

        # List case
        match_list = re.match(r"(.*)\[(\d+)\]$", changed_subtree_path)
        if match_list:
            parent_path = match_list.groups()[0]
            index = int(match_list.groups()[1])
            extract(new_discovered_state, parent_path)[index] = discovered_subtree_state

        # Dict case
        match_dict = re.match(r"(.*)\['(\S+)'\]$", changed_subtree_path)
        if match_dict and not match_list:
            parent_path = match_dict.groups()[0]
            index = match_dict.groups()[1]
            extract(new_discovered_state, parent_path)[index] = discovered_subtree_state

        if not match_dict and not match_list:
            assert (
                False
            ), f"type of changed_subtree_path not supported {changed_subtree_path}"

        print(yaml.safe_dump(new_discovered_state, default_flow_style=False))


def destructure_vars(rule, subtree):

    destructured_vars = {}

    for name, extract_path in rule.get("vars", {}).items():
        destructured_vars[name] = extract(subtree, extract_path)

    return destructured_vars


def desired_state_validation(
    monitor, secrets, project_src, current_state, ran_rules, inventory, explain
):

    plays = []

    destructured_vars_list = []
    validated_rules = []

    for rule, changed_subtree_path, subtree, inventory_name in ran_rules:

        # Experiment: Build the vars using destructuring
        destructured_vars = destructure_vars(rule, subtree)

        # Experiment: Make the subtree available as node
        destructured_vars["node"] = subtree

        print("destructured_vars", destructured_vars)

        # Build a play using tasks or role from rule

        play = {
            "name": f"validation for {inventory_name}",
            "hosts": inventory_name,
            "gather_facts": False,
            "tasks": [],
        }

        if "tasks" in rule.get(ACTION_RULES[Action.VALIDATE], {}):
            play["tasks"].append(
                {
                    "include_tasks": {
                        "file": find_tasks(
                            rule.get(ACTION_RULES[Action.VALIDATE]).get("tasks")
                        )
                    },
                    "name": "include validation",
                }
            )

            print(play)

            plays.append(play)
            destructured_vars_list.append(destructured_vars)
            validated_rules.append([changed_subtree_path, subtree])

    def runner_process_message(data):
        if data.get("event", "") == "runner_on_ok":
            event_data = data.get("event_data", {})
            if event_data.get("task_action", "") not in [
                "include_tasks",
                "include_vars",
            ]:
                monitor.stream.put_message(
                    ValidationTask(
                        0,
                        now(),
                        event_data.get("host"),
                        event_data.get("task_action", ""),
                        "ok",
                    )
                )
        if data.get("event", "") == "playbook_on_stats":
            event_data = data.get("event_data", {})
            for host in event_data.get("ok", {}).keys():
                monitor.stream.put_message(ValidationResult(0, now(), host, "ok"))

        monitor.stream.put_message(Stdout(0, now(), data.get("stdout", "")))

    if not plays:
        return None

    runner = PlaybookRunner(
        runner_process_message,
        current_state,
        {},
        destructured_vars_list,
        plays,
        secrets,
        project_src,
        inventory,
    )
    result = runner.run()

    return result

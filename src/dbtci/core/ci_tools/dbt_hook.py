import os
import sys
import click
import subprocess
import json
from typing import List, Dict
from dbtci.core.ci_tools.utils.utils import *
from dbt.config import PROFILES_DIR


COMPILED_DIR = "compiled"
RUN_DIR = "run"
MANIFEST_FILE = "manifest.json"
DEFAULT_OPEN_COMMAND = "open"

BASE_COMMAND = "dbt {action} --profile {profile} --target {target} --profiles-dir {profiles_dir} {options}"
DBT_LIST_COMMAND = "dbt ls > /dev/null"

RESOURCE_TYPES = ["model", "test", "seed", "snapshot", "source", "analysis"]


class DbtHook:
    """
    Simple wrapper around the dbt CLI. 
    :param profiles_dir: If set, passed as the `--profiles-dir` argument to the `dbt` command
    :type profiles_dir: str
    :param target: If set, passed as the `--target` argument to the `dbt` command
    :type target: str
    :param profile: If set, passed as the `--profile` argument to the `dbt` command
    :type profile: str
    :param vars: If set, passed as the `--vars` argument to the `dbt` command
    """

    def __init__(self, profile, target, project_root, profiles_dir=PROFILES_DIR):
        self.profile = profile
        self.target = target
        self.project_root = project_root or find_project_root()
        self.target_path = os.path.join(self.project_root, "target")
        self.profiles_dir = profiles_dir
        self.manifest_path = os.path.join(self.target_path, "manifest.json")
        self.manifest = None

    def _generate_manifest(self):
        subprocess.call(DBT_LIST_COMMAND, cwd=self.project_root, shell=True)
        try:
            with open(self.manifest_path) as f:
                manifest = json.load(f)
            return manifest
        except IOError:
            raise Exception(f"Could not find manifest file.")

    def fetch_model_data(self, list_of_models: List = [], exclude_tags: List = []):
        if not self.manifest:
            self.manifest = self._generate_manifest()

        model_dicts = {}

        for k, v in self.manifest["nodes"].items():
            model = get_resource_name(k)
            if v["resource_type"] == "model":
                model_dicts[model] = {}
                model_dicts[model]["tags"] = v["tags"]
                model_dicts[model]["columns"] = v["columns"]
                model_dicts[model]["description"] = v["description"]
                model_dicts[model]["tests"] = [
                    test
                    for test in self.manifest["child_map"][k]
                    if test.startswith("test")
                ]

        if list_of_models:
            model_dicts = {k: v for k, v in model_dicts.items() if k in list_of_models}

        if exclude_tags:
            model_dicts = {
                k: v
                for k, v in model_dicts.items()
                if not intersection_is_not_empty(v["tags"], exclude_tags)
            }

        return model_dicts

    def fetch_macro_data(self, list_of_macros: List = [], exclude_tags: List = []):
        if not self.manifest:
            self.manifest = self._generate_manifest()

        macro_dicts = {
            get_resource_name(k): v for k, v in self.manifest.get("macros").items()
        }

        if list_of_macros:
            macro_dicts = {k: v for k, v in macro_dicts.items() if k in list_of_macros}

        if exclude_tags:
            macro_dicts = {
                k: v
                for k, v in macro_dicts.items()
                if not intersection_is_not_empty(v["tags"], exclude_tags)
            }

        return macro_dicts

    def _macro_child_map(self):
        if not self.manifest:
            self.manifest = self._generate_manifest()

        macro_child_map = {}
        for resource, val in self.manifest.get("nodes").items():
            resource_type = val.get("resource_type")
            if val["depends_on"]["macros"]:
                for macro in val["depends_on"]["macros"]:
                    macro_child_map.setdefault(macro, {"models": [], "tests": []})
                    macro_child_map[macro][resouce_type].append(model)
        return macro_child_map

    def _find_macro_children(self, changed_macros):
        remaining_models = set()
        macro_map = self._macro_child_map()
        for macro in changed_macros:
            remaining_models.update(macro_map.get(macro).get("models"))
        return remaining_models

    def _build_options(
        self,
        models: List = None,
        excludes: List = None,
        dbt_vars: Dict = None,
        full_refresh: bool = False,
    ):
        options = ""
        if models:
            options += "--models {}".format(" ".join(models))
        if excludes:
            options += "--exclude {}".format(" ".join(excludes))
        if dbt_vars:
            options += "--vars {}".format(json.dumps(vars))
        if full_refresh:
            options += "--full-refresh"
        return options

    def _execute(self, action: str, options: str = "", debug: bool = False):
        if action in ["run", "test", "compile"]:
            command = BASE_COMMAND.format(
                action=action,
                profile=self.profile,
                target=self.target,
                profiles_dir=self.profiles_dir,
                options=options,
            ).strip()
        if debug:
            return command
        click.secho(f"Executing: {command}")
        return_code = subprocess.call(command, cwd=self.project_root, shell=True)
        if return_code != 0:
            raise Exception(f"DBT command {command} failed")
        return command

    def run(
        self,
        models: List = None,
        excludes: List = None,
        vars: List = None,
        full_refresh: bool = False,
        debug: bool = False,
    ):
        self._execute(
            action="run",
            options=self._build_options(models, excludes, vars, full_refresh),
            debug=debug,
        )

    def test(
        self,
        models: List = None,
        excludes: List = None,
        vars: List = None,
        debug: bool = False,
    ):
        self._execute(
            action="test",
            options=self._build_options(models, excludes, vars),
            debug=debug,
        )

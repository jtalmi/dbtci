import os
import sys
import json
import subprocess
import logging
from typing import List, Dict

import click

from dbtci.core.ci_tools.dbt_hook import DbtHook
from dbtci.core.ci_tools.utils.git_utils import fetch_changed_dbt_objects

RESOURCE_TYPES = ["model", "test", "seed", "snapshot", "source", "analysis"]


class DbtCIManager(DbtHook):
    """ models: tests, tags, upstream, downstream
      tests: depedent models
      seeds: dependent models 
  """

    def __init__(self, compare_branch="master", *args, **kwargs):
        super(DbtCIManager, self).__init__(*args, **kwargs)
        self.compare_branch = compare_branch
        self.changed_objects = None

    def execute_changed(
        self,
        action,
        check_macros=False,
        children=False,
        full_refresh=False,
        test=False,
        debug=False,
    ):
        if not self.changed_objects:
            self.changed_objects = fetch_changed_dbt_objects()
        models = self.changed_objects.get("model")
        if models:
            click.echo("Changed models: {}".format(" ".join(models)))

        if check_macros:
            macros = self.changed_objects.get("macros")
            if macros:
                click.echo("Changed macros: {}".format(" ".join(macros)))
                models += self._find_macro_children(macros)
        if models:
            if children:
                models = [f"{model}+" for model in models]
            if action == "run":
                self.run(models=models, full_refresh=full_refresh, debug=debug)
            if action == "test" or test:
                self.test(models=models, debug=debug)
        else:
            click.secho("Nothing to do.")

    def _parse_manifest(self, resource_type, *args):
        if resource_type == "model":
            return self.fetch_model_data(*args)
        elif resource_type == "macro":
            return self.fetch_macro_data(*args)
        else:
            raise Exception("Unsupported resource type: %s" % resource_type)

    def _check_schema_tests(self, model, data, required):
        coverage = True
        for schema_test in required:
            if not any(schema_test in test for test in data.get("tests")):
                click.secho(
                    "\u274C" + f" No {schema_test} test found for model: {model}"
                )
                coverage = False
        return coverage

    def _check_yaml(self, resource, data, required, **kwargs):
        coverage = True
        for field in required:
            if not data.get(field):
                click.secho("\u274C" + f" No {field} found for resource: {resource}")
                coverage = False
        return coverage

    def lint(
        self,
        resource_type,
        lint_func,
        changed_only=True,
        exclude_tags=["skip-lint"],
        **kwargs,
    ):
        if changed_only:
            if not self.changed_objects:
                self.changed_objects = fetch_changed_dbt_objects()
            resources = self.changed_objects.get(resource_type)
        if not resources:
            return []
        manifest = self._parse_manifest(resource_type, resources, exclude_tags)
        unlinted_resources = set()

        for resource, data in manifest.items():
            if not lint_func(resource, data, **kwargs):
                unlinted_resources.add(resource)
        return unlinted_resources

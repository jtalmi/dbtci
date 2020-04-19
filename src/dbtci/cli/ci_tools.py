import os
import sys
import logging
import warnings

import click

from . import cli
from dbtci.core.ci_tools.dbt_ci_manager  import DbtCIManager
from dbtci.core.ci_tools.utils.utils  import find_project_root

logger = logging.getLogger(__name__)


LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}

DEFAULT_REQUIRED_TESTS = ['unique', 'not_null']
DEFAULT_REQUIRED_YML = ['description', 'columns']

@cli.command()
@click.option("--macros", help="Run and/or test models using changed macros", is_flag=True)
@click.option("--children", help="Run and/or test model children", is_flag=True)
@click.option("--full-refresh", help="Fully refresh models on run", is_flag=True)
@click.option("--test", help="Test changed models after run", is_flag=True)
@click.option("--debug", help="Print commands only", is_flag=True)
def run_changed(check_macros, children, full_refresh, test, debug):
  project_root = find_project_root()
  if not project_root:
    raise Exception(
        "fatal: Not a dbt project (or any of the parent directories). "
        "Missing dbt_project.yml file"
    )
  os.chdir(project_root)  
  dbt_manager = DbtCIManager(
    profile='dbtci_integration_tests',
    target='dbtci_integration_tests',
    project_root=project_root)
  click.secho('Starting run...', blink=True)
  dbt_manager.execute_changed('run', check_macros, children, full_refresh, test, debug)
  click.secho('Finished run.')

@cli.command()
@click.option("--macros", help="Run and/or test models using changed macros", is_flag=True)
@click.option("--children", help="Run and/or test model children", is_flag=True)
@click.option("--debug", help="Print commands only", is_flag=True)
def test_changed(check_macros, children, debug):
  project_root = find_project_root()
  if not project_root:
    raise Exception(
        "fatal: Not a dbt project (or any of the parent directories). "
        "Missing dbt_project.yml file"
    )
  os.chdir(project_root)
  dbt_manager = DbtCIManager()
  click.secho('Starting test...',fg='green', blink=True)
  dbt_manager.execute_changed('test', check_macros, children, debug)
  click.secho('Finished test.', fg='green')

@cli.command()
@click.argument("resource_type")
@click.option("--tests", help="List of required tests for each resource", is_flag=True)
@click.option("--yml", help="List of required yml fields for each resource", is_flag=True)
def lint(resource_type='model', tests=DEFAULT_REQUIRED_TESTS, yml=DEFAULT_REQUIRED_YML ):
  project_root = find_project_root()
  if not project_root:
    raise Exception(
        "fatal: Not a dbt project (or any of the parent directories). "
        "Missing dbt_project.yml file"
    )
  os.chdir(project_root)

  click.secho('Starting lint...', fg='green', blink=True)

  dbt_manager = DbtCIManager()
  models_without_tests = models_without_yml = macros_without_yml = []
  
  if resource_type == 'model':
    models_without_tests = dbt_manager.lint('model', dbt_manager.check_schema_tests, required=tests)
    models_without_yml = dbt_manager.lint('model', dbt_manager.check_yaml, required=yml)
  elif resource_type == 'macro' and 'description' in yml:
    macros_without_yml = dbt_manager.lint('macro', dbt_manager.check_yaml, required=['description'])

  if models_without_tests or models_without_yml or macros_without_yml:
    if models_without_tests:
      msg = ("The following models do not have the required tests: \n   - %s" %
        "\n   - ".join(models_without_tests))
      click.secho(msg, fg='red')
    if models_without_yml:
      msg = ("The following models do not have valid YAML: \n   - %s" %
        "\n   - ".join(models_without_yml))
      click.secho(msg, fg='red')
    if macros_without_yml:
      msg = ("The following macros do not have valid YAML: \n   - %s" %
        "\n   - ".join(macros_without_yml))
      click.secho(msg, fg='red')
    
    sys.exit(1)  # exit with failure
   
  click.secho('Finished run.')

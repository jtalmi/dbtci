'''
Integration tests:

1. Run changed
- changed models run.
- changed models + macro children run 
- changed models + children run
- changed models run + are tested

2. Test changed
- test changed models 




'''

import unittest
from dbt.adapters.factory import get_adapter
from dbt.config import RuntimeConfig
from dbt.main import handle_and_check
import sys
import os
import yaml
from dbtci.core.ci_tools.dbt_hook import DbtHook
import shutil

IS_DOCKER = os.environ.get("AM_I_IN_A_DOCKER_CONTAINER", False)


class TestArgs(object):
    def __init__(self, kwargs):
        self.which = "run"
        self.single_threaded = False
        self.profiles_dir = os.getcwd()
        self.__dict__.update(kwargs)


class DBTIntegrationTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(DBTIntegrationTest, self).__init__(*args, **kwargs)

        self.test_schema_name = "my_test_schema"
        if IS_DOCKER:
            dbt_config_dir = os.path.abspath(
                os.path.expanduser(
                    os.environ.get("DBT_CONFIG_DIR", "/home/dbt_test_user/.dbt")
                )
            )
        else:
            dbt_config_dir = os.getcwd()

        self.dbt_config_dir = dbt_config_dir
        self.dbt_profile = os.path.join(self.dbt_config_dir, "profiles.yml")

    def setUp(self):
        self.use_profile()
        self.use_default_project()
        self.set_packages()
        self.load_config()

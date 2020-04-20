import unittest
import os
import mock
from dbtci.core.ci_tools.dbt_ci_manager import DbtCIManager
import subprocess


class DBTIntegrationTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(DBTIntegrationTest, self).__init__(*args, **kwargs)

    def setUp(self):
        self.dbt_ci_manager = DbtCIManager(
            profile="snaptravel-snowflake",
            target="dev",
            project_root="/Users/jonathantalmi/dev/db-analytics",
            # profile="dbtci_integration_tests",
            # target="postgres",
            # project_root=os.path.dirname(__file__),
        )
        self.mock_changed_objects = mock.patch(
            "dbtci.core.ci_tools.dbt_ci_manager.fetch_changed_dbt_objects"
        ).start()
        self.mock_changed_objects.return_value = {"model": ["test_model"]}
        self.model_path = os.path.join(
            os.path.dirname(__file__), "models/test_model.sql"
        )
        self.child_model_path = os.path.join(
            os.path.dirname(__file__), "models/test_child_model.sql"
        )
        self.macro_path = os.path.join(
            os.path.dirname(__file__), "macros/test_macro.sql"
        )

        with open(self.model_path, "w+") as f:
            f.write("""SELECT 1 AS my_integer_col""")

        with open(self.child_model_path, "w+") as f:
            f.write("""SELECT * FROM {{ ref('test_model') }}""")

        with open(self.macro_path, "w+") as f:
            f.write("""{% macro test_macro() %} SELECT 2 {% endmacro %}""")

    def tearDown(self):
        mock.patch.stopall()
        os.remove(self.model_path)
        os.remove(self.child_model_path)
        os.remove(self.macro_path)

    def drop_model(self, model_name):
        command = """dbt run-operation drop_view --args \'{"model_name": "{}"}\'"""
        subprocess.call(command.format(model_name), shell=True)

    def test_run_changed_models(self):
        self.dbt_ci_manager.execute_changed("run", check_macros=False, children=False)
        self.dbt_ci_manager.test(["test_model"])

    def test_run_changed_models_with_children(self):
        self.mock_changed_objects.return_value = {"model": ["test_model"]}
        self.dbt_ci_manager.execute_changed("run", check_macros=False, children=True)
        self.dbt_ci_manager.test(["test_model", "test_child_model"])

    def test_run_macro_children(self):
        self.mock_changed_objects.return_value = {"macros": ["test_macro"]}
        self.dbt_ci_manager.execute_changed("run", check_macros=True)
        self.dbt_ci_manager.test(["test_macro_model"])

import unittest
import os
import mock
from collections import defaultdict
from dbtci.core.ci_tools.dbt_ci_manager import DbtCIManager
from dbtci.core.ci_tools.utils.git_utils import OBJECT_TYPES
import subprocess

CHANGED_MODELS_TEMPLATE + {obj: [] for obj in OBJECT_TYPES}


class DBTIntegrationTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(DBTIntegrationTest, self).__init__(*args, **kwargs)

    def setUp(self):
        self.dbt_ci_manager = DbtCIManager(
            profile="dbtci_integration_tests",
            target="postgres",
            project_root=os.path.dirname(__file__),
        )
        self.mock_changed_objects = mock.patch(
            "dbtci.core.ci_tools.dbt_ci_manager.fetch_changed_dbt_objects"
        ).start()
        self.macro_path = os.path.join(
            os.path.dirname(__file__), "macros/test_macro.sql"
        )
        self.model_path = os.path.join(
            os.path.dirname(__file__), "models/test_model.sql"
        )
        self.child_model_path = os.path.join(
            os.path.dirname(__file__), "models/test_child_model.sql"
        )

        with open(self.macro_path, "w+") as f:
            f.write("""{% macro test_macro() %} SELECT 1 {% endmacro %}""")

        with open(self.model_path, "w+") as f:
            f.write("""{{ test_macro() }} AS my_integer_col""")

        with open(self.child_model_path, "w+") as f:
            f.write("""SELECT * FROM {{ ref('test_model') }}""")

    def tearDown(self):
        mock.patch.stopall()
        os.remove(self.model_path)
        os.remove(self.child_model_path)
        os.remove(self.macro_path)

    def test_run_changed_models(self):
        self.mock_changed_objects.return_value = CHANGED_MODELS_TEMPLATE
        self.mock_changed_objects.return_value.update(model=["test_model"])
        self.dbt_ci_manager.execute_changed("run", check_macros=False, children=False)
        self.dbt_ci_manager.test(["test_model"])
        self.dbt_ci_manager.drop(["test_model"])

    def test_run_changed_models_with_children(self):
        self.mock_changed_objects.return_value = CHANGED_MODELS_TEMPLATE
        self.mock_changed_objects.return_value.update(model=["test_model"])
        self.dbt_ci_manager.execute_changed("run", check_macros=False, children=True)
        self.dbt_ci_manager.test(["test_model", "test_child_model"])
        self.dbt_ci_manager.drop(["test_model", "test_child_model"])

    def test_run_macro_children(self):
        self.mock_changed_objects.return_value = CHANGED_MODELS_TEMPLATE
        self.mock_changed_objects.return_value.update(macro=["test_macro"])
        self.dbt_ci_manager.execute_changed("run", check_macros=True)
        self.dbt_ci_manager.test(["test_model"])
        self.dbt_ci_manager.drop(["test_model"])

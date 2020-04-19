import unittest
import os
import mock
from dbtci.core.ci_tools.dbt_ci_manager import DbtCIManager


class DBTIntegrationTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(DBTIntegrationTest, self).__init__(*args, **kwargs)

    def setUp(self):
        self.dbt_ci_manager = DbtCIManager(
            profile="integration_tests",
            target="test",
            project_root="tests/integration/",
        )
        self.mock_changed_objects = mock.patch(
            "dbtci.core.ci_tools.utls.git_utils.fetch_changed_dbt_objects"
        ).start()
        self.mock_changed_objects.return_value = {"model": ["test_model"]}

    def test_run_changed_models(self):
        with open("models/test_model.sql", "w") as f:
            f.write("""SELECT 1 AS my_integer_col""")
        self.execute_changed("run", check_macros=False, children=False)
        self.test(["test_model"])
        os.remove("models/test_model.sql")

    def test_run_changed_models_with_children(self):
        with open("models/test_model.sql", "w") as f:
            f.write("""SELECT 1 AS my_integer_col""")
        with open("models/test_model_child.sql", "w") as f:
            f.write("""SELECT * FROM {{ ref('test_model' }}""")
        self.mock_changed_models.return_value = {"model": ["test_model"]}
        self.execute_changed("run", check_macros=False, children=True)
        self.test(["test_model", "test_model_child"])
        os.remove("models/test_model.sql")
        os.remove("models/test_model_child.sql")

    def test_run_macro_children(self):
        macro = """{% macro test_macro() %} select 2 {% endmacro %}"""
        with open("macro/test_macro.sql", "w") as f:
            f.write(macro)
        with open("models/test_macro_model.sql", "w") as f:
            f.write("""{{ test_macro() }} AS my_integer_col""")
        self.mock_changed_objects.return_value = {"macros": ["test_macro"]}
        self.execute_changed("run", check_macros=True, children=False)
        self.test(["test_macro_model"])
        os.remove("models/test_macro_model.sql")

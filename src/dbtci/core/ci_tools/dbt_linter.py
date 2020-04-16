import os
import sys
import json
import subprocess
import logging
from typing import List, Dict

RESOURCE_TYPES = [
  'model',
  'test',
  'seed',
  'snapshot',
  'source',
  'analysis'
]

MANDATORY_SCHEMA_TESTS = ['unique', 'not_null']
RELATIVE_MANIFEST_PATH = "target/manifest.json"

class DbtLinter:
  ''' models: tests, tags, upstream, downstream
      tests: depedent models
      seeds: dependent models 
  '''
  def __init__(self, compare_branch='master', *args):
    self.args = args
    self.project_dir = '/Users/jonathantalmi/dev/db-analytics/'
    self.manifest = self.fetch_manifest()

  def check_descriptions(self, list_of_models=[]):
    models_without_descriptions = []
    models = [k for k, v in self.manifest['nodes'] if k in list_of_models]
    for model in models:
      if not self.manifest[model]['description']:
        logging.error('No description for model: %s', model)
        models_without_descriptions.append(model)
    return models_without_descriptions

  def check_columns(self, list_of_models=[]):
    models_without_columns = []
    models = [k for k, v in self.manifest['nodes'] if k in list_of_models]
    for model in models:
      if not self.manifest[model]['columns']:
        logging.error('No columns for model: %s', model)
        models_without_columns.append(model)
    return models_without_columns

  def check_test_coverage(self, list_of_models=[], mandatory_tests=MANDATORY_SCHEMA_TESTS):
    models_without_tests = []
    tests_per_model = tests_per_model2(list_of_models)
    for model, tests in tests_per_model:
      for schema_test in mandatory_tests:
        if not any(schema_test in test for test in tests):
          logging.error(f'No {schema_test} test found for model: %s', model)
          models_without_tests.append(model)
    return models_without_tests
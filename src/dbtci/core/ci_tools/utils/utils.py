import os
from os import path
from typing import List

def intersection_is_not_empty(a: List, b: List) -> bool:
  return len(set(a) & set(b)) > 0

def get_resource_name(full_resource_name: str) -> str:
  return full_resource_name.split('.')[-1]

def find_project_root(filedir=os.getcwd(): str):
  '''
  Credit:https://github.com/mikekaminsky/dbt-helper/
  '''
  root_path = os.path.abspath(os.sep)

  while filedir != root_path:
      project_file = os.path.join(cwd, "dbt_project.yml")
      if os.path.exists(project_file):
          return filedir
      filedir = os.path.dirname(filedir)

  return None
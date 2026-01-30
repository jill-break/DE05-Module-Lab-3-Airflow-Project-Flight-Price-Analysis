import sys
import os
import pytest

# Add airflow/dags to sys.path so we can import etl_modules
# This assumes tests/ is at project root.
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dags_folder = os.path.join(project_root, 'airflow', 'dags')

if dags_folder not in sys.path:
    sys.path.insert(0, dags_folder)

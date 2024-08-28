import os

from airflow import DAG
from airflow.configuration import conf as airflow_conf
from dagfactory import load_yaml_dags


os.environ["DBT_ROOT_PATH"] = os.path.join(airflow_conf.get("core", "dags_folder"), "git_odt-analytics-dbt")
load_yaml_dags(globals_dict=globals(),
               suffix=['dag.yaml'])

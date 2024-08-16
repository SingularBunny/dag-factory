from typing import Callable, Dict, Any

from airflow import DAG
from airflow.utils.module_loading import import_string
from airflow.utils.task_group import TaskGroup
from cosmos.airflow.graph import generate_task_or_group
from cosmos.dbt.graph import DbtNode

from build.lib.dagfactory.dbtdagbuilder import DagBuilder
from dagfactory.exceptions import DagFactoryException


def make_converter(resource_type: str, converter_rules: Dict[str, Any]) -> Callable:
    def convert_source(dag: DAG, task_group: TaskGroup, node: DbtNode, **kwargs):
        rule = converter_rules.get(node.name, None)
        if rule:
            operator_class = rule.pop("class")
            rule["dag"] = dag
            rule["task_group"] = task_group
            rule["task_id"] = f"{node.name}_{resource_type}"
            return DagBuilder.make_task(operator_class, rule)
        else:
            return generate_task_or_group(dag, task_group, node, **kwargs)

    return convert_source

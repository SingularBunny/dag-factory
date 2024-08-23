"""Module contains code for generating tasks and constructing a DAG"""

# pylint: disable=ungrouped-imports
import os
from datetime import timedelta
from pathlib import Path
from typing import List, Dict, Union, Any, Callable

from airflow import DAG, configuration
from airflow.models import BaseOperator, MappedOperator
from airflow.utils.module_loading import import_string
from cosmos import DbtDag, ProfileConfig, ProjectConfig, ExecutionConfig, RenderConfig, DbtTaskGroup
from cosmos.constants import DbtResourceType
from cosmos.converter import specific_kwargs, airflow_kwargs
from cosmos.profiles import PostgresUserPasswordProfileMapping, BaseProfileMapping
from packaging import version

from dagfactory.dbt_converters import make_converter
from dagfactory.exceptions import DagFactoryException, DagFactoryConfigException

try:
    from airflow.version import version as AIRFLOW_VERSION
except ImportError:
    from airflow import __version__ as AIRFLOW_VERSION


from dagfactory import utils
from dagfactory.dagbuilder import DagBuilder

# pylint: disable=ungrouped-imports,invalid-name
# Disabling pylint's ungrouped-imports warning because this is a
# conditional import and cannot be done within the import group above
# TaskGroup is introduced in Airflow 2.0.0
if version.parse(AIRFLOW_VERSION) >= version.parse("2.0.0"):
    from airflow.utils.task_group import TaskGroup
else:
    TaskGroup = None
# pylint: disable=ungrouped-imports,invalid-name

if version.parse(AIRFLOW_VERSION) >= version.parse("2.4.0"):
    from airflow.datasets import Dataset
else:
    Dataset = None


# these are params only used in the DAG factory, not in the tasks
SYSTEM_PARAMS: List[str] = ["operator", "dependencies", "task_group_name"]

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"

class DbtDagBuilder(DagBuilder):

    def __init__(self,
                 dag_name: str,
                 dag_config: Dict[str, Any],
                 default_config: Dict[str, Any],
                 user_defined_macros: Dict[str, Any] = {}) -> None:
        super().__init__(dag_name, dag_config, default_config)
        self.dbt_root_path = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
        self.user_defined_macros: Dict[str, Any] = user_defined_macros

    def parse_dbt_params(self, shared_execution_config: ExecutionConfig, params: Dict[str, Any]):
        dbt_specific_kwargs = specific_kwargs(**params)
        profile_config_kwargs = dbt_specific_kwargs.get("profile_config", {}).copy()
        profile_mapping_kwargs = profile_config_kwargs.pop("profile_mapping", {}).copy()
        profile_mapping_class = profile_mapping_kwargs.pop("mapping_class", None)
        try:
            profile_mapping_obj: Callable[..., BaseProfileMapping] = import_string(profile_mapping_class)
        except Exception as err:
            raise DagFactoryException(f"Failed to import Profile Mapping: {profile_mapping_class}") from err
        dbt_specific_kwargs["profile_config"] = ProfileConfig(
            **profile_config_kwargs,
            profile_mapping=profile_mapping_obj(**profile_mapping_kwargs),
        )
        dbt_specific_kwargs["project_config"] = ProjectConfig(
            self.dbt_root_path / dbt_specific_kwargs.get("project_config", ""))

        render_config_kwargs = dbt_specific_kwargs.get("render_config", None)
        if render_config_kwargs:
            node_converters = render_config_kwargs.get("node_converters", {})
            node_converters_dict = {}
            for resource_type, converter_rules in node_converters.items():
                node_converters_dict[DbtResourceType(resource_type)] = make_converter(resource_type, converter_rules, self)
            if node_converters_dict:
                render_config_kwargs["node_converters"] = node_converters_dict

            dbt_specific_kwargs["render_config"] = RenderConfig(**render_config_kwargs)

        execution_config_kwargs = dbt_specific_kwargs.get("execution_config", None)
        if execution_config_kwargs:
            dbt_specific_kwargs["execution_config"] = RenderConfig(**execution_config_kwargs)
        else:
            dbt_specific_kwargs["execution_config"] = shared_execution_config

        return dbt_specific_kwargs

    def make_task_groups(
        self,
        shared_execution_config: ExecutionConfig,
        task_groups: Dict[str, Any], dag: DAG
    ) -> Dict[str, "TaskGroup"]:
        """Takes a DAG and task group configurations. Creates TaskGroup instances.

        :param task_groups: Task group configuration from the YAML configuration file.
        :param dag: DAG instance that task groups to be added.
        """
        task_groups_dict: Dict[str, "TaskGroup"] = {}
        if version.parse(AIRFLOW_VERSION) >= version.parse("2.0.0"):
            for task_group_name, task_group_conf in task_groups.items():
                task_group_conf["group_id"] = task_group_name
                task_group_conf["dag"] = dag
                task_group_str: str = task_group_conf.pop("task_group_class", "airflow.utils.task_group.TaskGroup")
                try:
                    # class is a Callable https://stackoverflow.com/a/34578836/3679900
                    task_group_obj: Callable[..., TaskGroup] = import_string(task_group_str)
                except Exception as err:
                    raise DagFactoryException(f"Failed to import DAG: {task_group_str}") from err
                if issubclass(task_group_obj, DbtTaskGroup):
                    task_group_conf.update(self.parse_dbt_params(shared_execution_config, task_group_conf))

                task_group = task_group_obj(
                    **{
                        k: v
                        for k, v in task_group_conf.items()
                        if k not in SYSTEM_PARAMS
                    }
                )
                task_groups_dict[task_group.group_id] = task_group
        return task_groups_dict

    # pylint: disable=too-many-locals
    def build(self) -> Dict[str, Union[str, DAG]]:
        """
        Generates a DAG from the DAG parameters.

        :returns: dict with dag_id and DAG object
        :type: Dict[str, Union[str, DAG]]
        """
        dag_params: Dict[str, Any] = self.get_dag_params()

        dag_kwargs: Dict[str, Any] = {}

        dag_kwargs["dag_id"] = dag_params["dag_id"]

        if not dag_params.get("timetable") and not utils.check_dict_key(
            dag_params, "schedule"
        ):
            dag_kwargs["schedule_interval"] = dag_params.get(
                "schedule_interval", timedelta(days=1)
            )

        if version.parse(AIRFLOW_VERSION) >= version.parse("1.10.11"):
            dag_kwargs["description"] = dag_params.get("description", None)
        else:
            dag_kwargs["description"] = dag_params.get("description", "")

        if version.parse(AIRFLOW_VERSION) >= version.parse("2.2.0"):
            dag_kwargs["max_active_tasks"] = dag_params.get(
                "max_active_tasks",
                configuration.conf.getint("core", "max_active_tasks_per_dag"),
            )

            if dag_params.get("timetable"):
                timetable_args = dag_params.get("timetable")
                dag_kwargs["timetable"] = DagBuilder.make_timetable(
                    timetable_args.get("callable"), timetable_args.get("params")
                )
        else:
            dag_kwargs["concurrency"] = dag_params.get(
                "concurrency", configuration.conf.getint("core", "dag_concurrency")
            )

        dag_kwargs["catchup"] = dag_params.get(
            "catchup", configuration.conf.getboolean("scheduler", "catchup_by_default")
        )

        dag_kwargs["max_active_runs"] = dag_params.get(
            "max_active_runs",
            configuration.conf.getint("core", "max_active_runs_per_dag"),
        )

        dag_kwargs["dagrun_timeout"] = dag_params.get("dagrun_timeout", None)

        dag_kwargs["default_view"] = dag_params.get(
            "default_view", configuration.conf.get("webserver", "dag_default_view")
        )

        dag_kwargs["orientation"] = dag_params.get(
            "orientation", configuration.conf.get("webserver", "dag_orientation")
        )

        dag_kwargs["template_searchpath"] = dag_params.get("template_searchpath", None)

        # Jinja NativeEnvironment support has been added in Airflow 2.1.0
        if version.parse(AIRFLOW_VERSION) >= version.parse("2.1.0"):
            dag_kwargs["render_template_as_native_obj"] = dag_params.get(
                "render_template_as_native_obj", False
            )

        dag_kwargs["sla_miss_callback"] = dag_params.get("sla_miss_callback", None)

        dag_kwargs["on_success_callback"] = dag_params.get("on_success_callback", None)

        dag_kwargs["on_failure_callback"] = dag_params.get("on_failure_callback", None)

        dag_kwargs["default_args"] = dag_params.get("default_args", None)

        dag_kwargs["doc_md"] = dag_params.get("doc_md", None)

        dag_kwargs["access_control"] = dag_params.get("access_control", None)

        dag_kwargs["is_paused_upon_creation"] = dag_params.get(
            "is_paused_upon_creation", None
        )

        if (
            utils.check_dict_key(dag_params, "schedule")
            and not utils.check_dict_key(dag_params, "schedule_interval")
            and version.parse(AIRFLOW_VERSION) >= version.parse("2.4.0")
        ):
            if utils.check_dict_key(
                dag_params["schedule"], "file"
            ) and utils.check_dict_key(dag_params["schedule"], "datasets"):
                file = dag_params["schedule"]["file"]
                datasets_filter = dag_params["schedule"]["datasets"]
                datasets_uri = utils.get_datasets_uri_yaml_file(file, datasets_filter)

                del dag_params["schedule"]["file"]
                del dag_params["schedule"]["datasets"]
            else:
                datasets_uri = dag_params["schedule"]

            dag_kwargs["schedule"] = [Dataset(uri) for uri in datasets_uri]

        dag_kwargs["params"] = dag_params.get("params", None)
        dag_kwargs["user_defined_macros"] = self.user_defined_macros

        dag_str: str = dag_params.pop("dag_class", "airflow.DAG")

        shared_execution_config = ExecutionConfig()
        try:
            # class is a Callable https://stackoverflow.com/a/34578836/3679900
            dag_obj: Callable[..., DAG] = import_string(dag_str)
        except Exception as err:
            raise DagFactoryException(f"Failed to import DAG: {dag_str}") from err
        if issubclass(dag_obj, DbtDag):
            dag_kwargs.update(self.parse_dbt_params(shared_execution_config, dag_params))
        dag: DAG = dag_obj(**dag_kwargs)

        if dag_params.get("doc_md_file_path"):
            if not os.path.isabs(dag_params.get("doc_md_file_path")):
                raise DagFactoryException("`doc_md_file_path` must be absolute path")

            with open(
                dag_params.get("doc_md_file_path"), "r", encoding="utf-8"
            ) as file:
                dag.doc_md = file.read()

        if dag_params.get("doc_md_python_callable_file") and dag_params.get(
            "doc_md_python_callable_name"
        ):
            doc_md_callable = utils.get_python_callable(
                dag_params.get("doc_md_python_callable_name"),
                dag_params.get("doc_md_python_callable_file"),
            )
            dag.doc_md = doc_md_callable(
                **dag_params.get("doc_md_python_arguments", {})
            )

        # tags parameter introduced in Airflow 1.10.8
        if version.parse(AIRFLOW_VERSION) >= version.parse("1.10.8"):
            dag.tags = dag_params.get("tags", None)

        tasks: Dict[str, Dict[str, Any]] = dag_params.get("tasks", {})

        # add a property to mark this dag as an auto-generated on
        dag.is_dagfactory_auto_generated = True

        # create dictionary of task groups
        task_groups_dict: Dict[str, "TaskGroup"] = self.make_task_groups(
            shared_execution_config,
            dag_params.get("task_groups", {}), dag
        )

        # create dictionary to track tasks and set dependencies
        tasks_dict: Dict[str, BaseOperator] = {}
        for task_name, task_conf in tasks.items():
            task_conf["task_id"]: str = task_name
            operator: str = task_conf["operator"]
            task_conf["dag"]: DAG = dag
            # add task to task_group
            if task_groups_dict and task_conf.get("task_group_name"):
                task_conf["task_group"] = task_groups_dict[
                    task_conf.get("task_group_name")
                ]
            # Dynamic task mapping available only in Airflow >= 2.3.0
            if (task_conf.get("expand") or task_conf.get("partial")) and version.parse(
                AIRFLOW_VERSION
            ) < version.parse("2.3.0"):
                raise DagFactoryConfigException(
                    "Dynamic task mapping available only in Airflow >= 2.3.0"
                )

            # replace 'task_id.output' or 'XComArg(task_id)' with XComArg(task_instance) object
            if task_conf.get("expand") and version.parse(
                AIRFLOW_VERSION
            ) >= version.parse("2.3.0"):
                task_conf = self.replace_expand_values(task_conf, tasks_dict)
            params: Dict[str, Any] = {
                k: v for k, v in task_conf.items() if k not in SYSTEM_PARAMS
            }
            task: Union[BaseOperator, MappedOperator] = DagBuilder.make_task(
                operator=operator, task_params=params
            )
            tasks_dict[task.task_id]: BaseOperator = task

        if isinstance(dag, DbtDag):
            for task_id in dag.task_ids:
                if task_id not in tasks_dict:
                    task = dag.task_dict[task_id]
                    tasks_dict[task_id] = dag.task_dict[task_id]

        # set task dependencies after creating tasks
        self.set_dependencies(
            tasks, tasks_dict, dag_params.get("task_groups", {}), task_groups_dict
        )

        return {"dag_id": dag_params["dag_id"], "dag": dag}

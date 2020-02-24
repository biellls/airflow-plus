import textwrap
from enum import Enum

import jinja2
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Union, Optional, Dict, Sequence

import pendulum
from airflow import AirflowException, settings
from airflow.hooks.base_hook import BaseHook
from airflow.models import Connection
from airflow.operators.python_operator import PythonOperator
from pendulum import Pendulum
from pydantic import BaseModel
from typing_extensions import Protocol, runtime_checkable

from airflow_plus.airflow_testing import AirflowDb


class DAGContext(BaseModel):
    execution_date: Pendulum
    ds: str
    tomorrow_ds: str
    yesterday_ds: str
    ts: str


@runtime_checkable
class Operator(Protocol):
    template_fields: Sequence[str] = ()
    template_ext: Sequence[str] = ()
    task_id: str
    operator_config: 'OperatorConfig'

    def execute(self, dag_context: DAGContext):
        ...

    def _resolve_hooks(self):
        for k, v in self.__annotations__.items():
            if (issubclass(v, BaseHook) or CustomBaseHook in v.__bases__) and isinstance(getattr(self, k), str):
                setattr(self, k, ConnectionHelper(getattr(self, k)).hook)

    @property
    def airflow_operator(self) -> PythonOperator:
        if hasattr(self, '_operator'):
            return self._operator

        def _execute_func(**context):
            for template_field in self.template_fields:
                val = context.pop(template_field)
                setattr(self, template_field, val)
            self._resolve_hooks()
            self.execute(DAGContext.parse_obj(context))

        class PythonOperatorPlus(PythonOperator):
            template_fields = ('templates_dict', 'op_kwargs')
            template_ext = self.template_ext

        python_operator = PythonOperatorPlus(
            task_id=self.task_id,
            python_callable=_execute_func,
            provide_context=True,
            op_kwargs={x: getattr(self, x) for x in self.template_fields},
            **self.operator_config.make_kwargs()
        )
        # noinspection PyAttributeOutsideInit
        self._operator = python_operator
        return python_operator

    # noinspection PyProtectedMember
    def __rshift__(self, other: Union['Operator', 'SubDAG']) -> 'SubDAG':
        if isinstance(other, Operator):
            return SubDAG()._add_edge(self, other)
        else:
            sub_dag = other.copy()
            for source in other.sources:
                sub_dag._add_edge(self, source)
            return sub_dag

    def test(self, execution_date: Optional[Union[Pendulum, datetime]] = None, airflow_db: Optional[AirflowDb] = None):
        import airflow
        from airflow_plus.airflow_testing import mock_airflow_db
        if not execution_date:
            execution_date = pendulum.now()
        elif isinstance(execution_date, datetime):
            execution_date = pendulum.instance(execution_date)
        if not airflow_db:
            with mock_airflow_db():
                with airflow.DAG(dag_id='test_dag', schedule_interval='@once', start_date=datetime.min) as dag:
                    airflow_op = self.airflow_operator
                    dag >> airflow_op
                    airflow.models.TaskInstance(airflow_op, execution_date=execution_date).run(
                        test_mode=True,
                        ignore_task_deps=True,
                        ignore_ti_state=True,
                    )
        else:
            with airflow.DAG(dag_id='test_dag', schedule_interval='@once', start_date=datetime.min) as dag:
                airflow_op = self.airflow_operator
                dag >> airflow_op
                airflow.models.TaskInstance(airflow_op, execution_date=execution_date).run(
                    test_mode=True,
                    ignore_task_deps=True,
                    ignore_ti_state=True,
                )


class TriggerRule(Enum):
    ALL_SUCCESS = 'all_success'
    ALL_FAILED = 'all_failed'
    ALL_DONE = 'all_done'
    ONE_SUCCESS = 'one_success'
    ONE_FAILED = 'one_failed'
    NONE_FAILED = 'none_failed'
    NONE_SKIPPED = 'none_skipped'
    DUMMY = 'dummy'


@dataclass(eq=True, frozen=True)
class OperatorConfig:
    owner: str = None
    params: dict = None
    retries: int = None
    depends_on_past: bool = None
    trigger_rule: TriggerRule = None

    def make_kwargs(self) -> dict:
        return {
            k: getattr(self, k).value if isinstance(getattr(self, k), Enum) else getattr(self, k)
            for k, v in self.__annotations__.items()
            if getattr(self, k) is not None
        }


SubDAGEdges = List[Tuple[
        Union[Operator, 'SubDAG'],
        Union[Operator, 'SubDAG'],
    ]]


class SubDAG:
    edges: SubDAGEdges
    operators: Dict[str, Operator]

    def __init__(self):
        self.edges = []
        self.operators = {}

    def _add_edge(self, source: Operator, destination: Operator):
        self.edges.append((source, destination))
        self.operators[source.task_id] = source
        self.operators[destination.task_id] = destination
        return self

    def copy(self) -> 'SubDAG':
        sub_dag = SubDAG()
        sub_dag.edges = self.edges.copy()
        sub_dag.operators = self.operators.copy()
        return sub_dag

    @property
    def sources(self) -> List[Operator]:
        all_x = {x.task_id for x, y in self.edges}
        all_y = {y.task_id for x, y in self.edges}
        return [self.operators[task_id] for task_id in all_x.difference(all_y)]

    @property
    def sinks(self) -> List[Operator]:
        all_x = {x.task_id for x, y in self.edges}
        all_y = {y.task_id for x, y in self.edges}
        return [self.operators[task_id] for task_id in all_y.difference(all_x)]

    def __rshift__(self, other: Union[Operator, 'SubDAG', 'Component']) -> 'SubDAG':
        sub_dag = self.copy()
        if isinstance(other, Operator):
            for sink in self.sinks:
                sub_dag._add_edge(sink, other)
        else:
            if isinstance(other, Component):
                other = other.sub_dag
            for sink in self.sinks:
                for source in other.sources:
                    sub_dag._add_edge(sink, source)
            for edge in other.edges:
                sub_dag._add_edge(*edge)
        return sub_dag

    def __getitem__(self, item: str) -> 'OperatorInSubDAG':
        return OperatorInSubDAG(sub_dag=self, operator=self.operators[item])


@dataclass
class OperatorInSubDAG:
    sub_dag: SubDAG
    operator: Operator

    # noinspection PyProtectedMember
    def __rshift__(self, other: Union[Operator, 'SubDAG', 'Component']) -> 'SubDAG':
        sub_dag = self.sub_dag.copy()
        if isinstance(other, Operator):
            sub_dag._add_edge(self.operator, other)
        else:
            if isinstance(other, Component):
                other = other.sub_dag
            for source in other.sources:
                sub_dag._add_edge(self.operator, source)
            sub_dag.edges += other.edges
        return sub_dag

    def test(self, execution_date: Optional[Union[Pendulum, datetime]] = None, airflow_db: Optional[AirflowDb] = None):
        self.operator.test(execution_date=execution_date, airflow_db=airflow_db)


class DAG:
    def __init__(self, dag_id: str, schedule_interval: str, start_date: datetime):
        self.dag_id = dag_id
        self.schedule_interval = schedule_interval
        self.start_date = start_date
        self.sub_dag = SubDAG()

    def __rshift__(self, other: Union['SubDAG', 'Component']):
        self.sub_dag = self.sub_dag >> other
        return self

    # noinspection PyStatementEffect
    def make_airflow_dag(self):
        import airflow
        with airflow.DAG(
                dag_id=self.dag_id,
                schedule_interval=self.schedule_interval,
                start_date=self.start_date,
        ) as dag:
            for source in self.sub_dag.sources:
                dag >> source.airflow_operator
            for x, y in self.sub_dag.edges:
                x.airflow_operator >> y.airflow_operator
        return dag

    def __enter__(self) -> 'DAG':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Voodoo magic to insert DAG into the calling module's globals so the DAG bag picks it up
        import inspect
        inspect.stack()
        frm = inspect.stack()[1]
        mod = inspect.getmodule(frm[0])
        mod.dag_id = self.make_airflow_dag()

    def __getitem__(self, item: str) -> Operator:
        return self.sub_dag[item].operator


@dataclass
class ConnectionHelper:
    conn_id: str

    @staticmethod
    def custom_hook_classes() -> List[Union['CustomBaseHook', 'BaseHook']]:
        import inspect
        import importlib.util

        hook_classes = []
        for plugin_path in [settings.PLUGINS_FOLDER, str(Path(__file__).parent/'contrib/hooks')]:
            for python_file in Path(plugin_path).rglob('*.py'):
                relative_path = python_file.relative_to(Path(plugin_path))
                parts = list(relative_path.parts)
                parts[-1] = parts[-1].replace('.py', '')
                module_name = '.'.join(parts)
                spec = importlib.util.spec_from_file_location(name=module_name, location=str(python_file))
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                classes = inspect.getmembers(mod, inspect.isclass)
                hook_classes += [
                    cls
                    for cls_name, cls in classes
                    if (BaseHook in cls.__bases__) or isinstance(cls, CustomBaseHook)
                ]
        return hook_classes

    def get_custom_hook(self) -> Union[BaseHook, 'CustomBaseHook']:
        conn_type = BaseHook.get_connection(self.conn_id).conn_type

        for cls in self.custom_hook_classes():
            hook_name = getattr(cls, 'conn_type', None)
            if hook_name == conn_type:
                return cls.from_conn_id(conn_id=self.conn_id)

        raise ValueError(f'No hook found with connection type {conn_type}')

    @property
    def hook(self) -> BaseHook:
        try:
            return BaseHook.get_hook(self.conn_id)
        except AirflowException as e:
            if 'Unknown hook type' not in str(e):
                raise
            return self.get_custom_hook()


@runtime_checkable
class CustomBaseHook(Protocol):
    conn_type: str
    conn_type_long: str

    def __init__(self, conn_params: Connection):
        ...

    @classmethod
    def from_conn_id(cls, conn_id: str) -> 'CustomBaseHook':
        conn_params = BaseHook.get_connection(conn_id)
        return cls(conn_params)


@runtime_checkable
class Component(Protocol):
    sub_dag: SubDAG

    def __rshift__(self, other: Union[Operator, SubDAG, 'Component']) -> SubDAG:
        if isinstance(other, Component):
            other = other.sub_dag
        return self.sub_dag >> other


@runtime_checkable
class Templated(Protocol):
    template: str

    @property
    def context(self):
        return {
            k: getattr(self, k).rendered if isinstance(getattr(self, k), Templated) else getattr(self, k)
            for k, v in self.__annotations__.items() if k != 'template'
        }

    def render(self) -> str:
        return jinja2.Template(textwrap.dedent(self.template).strip()).render(self.context)

    @property
    def rendered(self) -> str:
        return self.render()

    def __str__(self) -> str:
        return self.rendered

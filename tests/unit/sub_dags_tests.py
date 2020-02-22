from dataclasses import dataclass
from datetime import datetime

from airflow_plus.models import DAGContext, Operator


@dataclass(eq=True, frozen=True)
class MyOperator(Operator):
    task_id: str

    def execute(self, dag_context: DAGContext):
        print(self.task_id)
        print(dag_context.ds)


def test_create_sub_dag_from_operators():
    op1 = MyOperator(task_id='op1')
    op2 = MyOperator(task_id='op2')

    sd = op1 >> op2

    assert len(sd.edges) == 1
    assert sd.edges[0][0] is op1
    assert sd.edges[0][1] is op2
    assert sd['op1'].operator is op1
    assert sd['op2'].operator is op2


def test_create_sub_dag_from_sub_dag():
    op1 = MyOperator(task_id='op1')
    op2 = MyOperator(task_id='op2')
    op3 = MyOperator(task_id='op3')
    op4 = MyOperator(task_id='op4')

    sub_dag_combinations = [
        (op1 >> op2) >> (op3 >> op4),
        (op1 >> op2) >> op3 >> op4,
        op1 >> op2 >> (op3 >> op4),
        op1 >> (op2 >> op3) >> op4,
    ]
    for sub_dag in sub_dag_combinations:
        assert len(sub_dag.edges) == 3
        assert sub_dag.sources == [op1]
        assert sub_dag.sinks == [op4]
        assert (op2, op3) in sub_dag.edges
        assert (op2, op4) not in sub_dag.edges


def test_create_branching_sub_dag():
    op1 = MyOperator(task_id='op1')
    op2 = MyOperator(task_id='op2')
    op3 = MyOperator(task_id='op3')
    op4 = MyOperator(task_id='op4')

    sd1 = op1 >> op2 >> op3
    sub_dag = sd1['op2'] >> op4

    assert len(sub_dag.edges) == 3
    assert (op1, op2) in sub_dag.edges
    assert (op2, op3) in sub_dag.edges
    assert (op2, op4) in sub_dag.edges
    assert sub_dag.sources == [op1]
    assert set(sub_dag.sinks) == {op3, op4}


def test_run_airflow_operator_from_sub_dag(capsys):
    op1 = MyOperator(task_id='test_op1')
    op2 = MyOperator(task_id='test_op2')

    sub_dag = op1 >> op2

    sub_dag['test_op1'].test()
    captured = capsys.readouterr()
    assert 'test_op1' in captured.out

    sub_dag['test_op2'].test(execution_date=datetime(2019, 1, 2))
    captured = capsys.readouterr()
    assert 'test_op2' in captured.out
    assert '2019-01-02' in captured.out

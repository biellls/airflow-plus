from dataclasses import dataclass

from airflow_plus.models import Templated


@dataclass
class MyTemplate(Templated):
    template = """
    {{ x }} = {{ y }}
    """
    x: str
    y: int


def test_simple_templated():
    assert MyTemplate(x='x', y=5).rendered == 'x = 5'


@dataclass
class MyTemplateWithWhen(Templated):
    template = """
    {% when x %}
    {% when y %}y = {{ y }}{% endwhen %}
    """
    x: str = None
    y: int = None


def test_template_with_when():
    assert MyTemplateWithWhen(x='x', y=5).rendered.strip() == 'x\ny = 5'
    assert MyTemplateWithWhen(x='x').rendered.strip() == 'x'
    assert MyTemplateWithWhen(y=5).rendered.strip() == 'y = 5'


@dataclass
class MyTemplateArgs(Templated):
    template = """
    {% for k, v in args.items() %}
    {{ k | upper }} = {{ v }}
    {% endfor %}
    """
    x: str = None
    y: int = None


def test_render_args():
    assert MyTemplateArgs(x='x', y=5).rendered.strip() == "X = x\nY = 5"
    assert MyTemplateArgs(y=5).rendered.strip() == "Y = 5"

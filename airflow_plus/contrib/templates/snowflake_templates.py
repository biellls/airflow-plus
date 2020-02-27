from enum import Enum
from typing import Tuple, Union, List, Generic, TypeVar

from dataclasses import dataclass

from airflow_plus.contrib.metadata import FieldMetadata
from airflow_plus.models import Templated


@dataclass
class CopyTemplate(Templated):
    template = """
    COPY INTO {{ table }} from (
        SELECT $1 FROM {{ s3_path }}
        {% for field in fields %}
            ${{ loop.index }} as {{ field.name }}{{ ',' if not loop.last else '' }}
        {% endfor %}
        FROM {{ s3_path }}
    )
    FILE_FORMAT = {{ file_format }}
    {% when copy_options %}
    ;
    """
    table: str
    s3_path: str
    fields: List[FieldMetadata]
    file_format: Union['NamedFileFormatTemplate', 'CustomFileFormatTemplate']
    copy_options: 'CopyOptionsTemplate' = None


@dataclass
class NamedFileFormatTemplate(Templated):
    template = """
    (format_name = {{ format_name }})
    """
    format_name: str


class FileFormatType(Enum):
    CSV = 'CSV'
    JSON = 'JSON'
    AVRO = 'AVRO'
    ORC = 'ORC'
    PARQUET = 'PARQUET'
    XML = 'XML'


class FileFormatCompression(Enum):
    AUTO = 'AUTO'
    GZIP = 'GZIP'
    BZ2 = 'BZ2'
    BROTLI = 'BROTLI'
    ZSTD = 'ZSTD'
    DEFLATE = 'DEFLATE'
    RAW_DEFLATE = 'RAW_DEFLATE'
    NONE = 'NONE'


T = TypeVar('T')


class Quoted(Generic[T]):
    def __init__(self, x: T):
        self.x = x

    def __str__(self) -> str:
        return f"'{self.x}'"


@dataclass
class CustomFileFormatTemplate(Templated):
    template = """
    (
        {% for k, v in args.items() %}
        {% when k %}{{ k }} = {{ v }}{% endwhen %}
        {% endfor %}
    )
    """
    type: FileFormatType = None
    field_delimiter: Quoted[str] = None
    null_if: Tuple[str, ...] = None
    compression: FileFormatCompression = None


@dataclass
class CopyOptionsTemplate(Templated):
    template = """
    {% if force is not none %}FORCE = {{ force | str | upper }}{% endif %}
    """
    force: bool = None


if __name__ == '__main__':
    rendered_copy = CopyTemplate(
        table='foo',
        s3_path='bar',
        fields=[FieldMetadata(name='a', type='varchar'), FieldMetadata(name='b', type='integer')],
        file_format=CustomFileFormatTemplate(
            type=FileFormatType.CSV,
            field_delimiter=Quoted(','),
            null_if=('null', 'NULL'),
            compression=FileFormatCompression.AUTO,
        )
    ).rendered
    print(rendered_copy)

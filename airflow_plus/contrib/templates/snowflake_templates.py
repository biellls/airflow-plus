from enum import Enum
from typing import Tuple, Union

from dataclasses import dataclass

from airflow_plus.models import Templated


@dataclass
class CopyTemplate(Templated):
    template = """
    COPY INTO {{ table }}
    SELECT $1 FROM {{ s3_path }}
    FILE_FORMAT = {{ file_format }}
    {% if copy_options is not none %}{{ copy_options }}{% endif %}
    """
    table: str
    s3_path: str
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


@dataclass
class CustomFileFormatTemplate(Templated):
    template = """
    (
        {% if type is not none %}type = {{ type }}{% endif %}
        {% if field_delimiter is not none %}field_delimiter = '{{ field_delimiter }}'{% endif %}
        {% if null_if is not none %}null_if = {{ null_if }}{% endif %}
        {% if compression is not none %}compression = {{ compression }}{% endif %}
    )
    """
    type: FileFormatType = None
    field_delimiter: str = None
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
        file_format=CustomFileFormatTemplate(
            type=FileFormatType.CSV,
            field_delimiter=',',
            null_if=('null', 'NULL'),
            compression=FileFormatCompression.AUTO,
        )
    )

    print(rendered_copy)

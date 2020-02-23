"""Functions to perform data transformations"""
import gzip
import zlib
from enum import Enum
from io import BytesIO
from typing import Union

from pandas import DataFrame


class FileFormat(str, Enum):
    CSV = 'csv'
    CSV_SEMICOLON = 'csv_semicolon'
    TSV = 'tsv'
    JSON = 'json'

    def convert_from_dataframe(self, df: DataFrame) -> bytes:
        if self in [FileFormat.CSV, FileFormat.CSV_SEMICOLON, FileFormat.TSV]:
            separator = ',' if FileFormat.CSV else ';' if FileFormat.CSV_SEMICOLON else '/t'
            data = df.to_csv(index=False, sep=separator)
        elif self == FileFormat.JSON:
            data = df.to_json(index=False, orient='records')
        else:
            assert False, f'Format case is not exhaustive. Missing "{self}"'
        return data

    @property
    def extension(self) -> str:
        if self == FileFormat.CSV_SEMICOLON:
            return 'csv'
        return self.value


class Compression(str, Enum):
    NONE = 'none'
    GZIP = 'gzip'
    ZLIB = 'zlib'

    def compress(self, data: Union[str, bytes, BytesIO]) -> BytesIO:
        if isinstance(data, str):
            data = data.encode()
        elif isinstance(data, BytesIO):
            data = data.getvalue()

        if self == Compression.NONE:
            return BytesIO(data)
        elif self == Compression.GZIP:
            out = BytesIO()
            with gzip.GzipFile(fileobj=out, mode="wb") as f:
                f.write(data)
        elif self == Compression.ZLIB:
            out = BytesIO(zlib.compress(data))
        else:
            assert False, f'Compress case is not exhaustive. Missing "{self}"'

        return out

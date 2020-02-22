"""Functions to perform data transformations"""
import gzip
from io import BytesIO
from typing import Union


def gzip_data(data: Union[str, bytes]) -> BytesIO:
    out = BytesIO()
    with gzip.GzipFile(fileobj=out, mode="wb") as f:
        f.write(data)
    return out

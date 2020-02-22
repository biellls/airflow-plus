from io import BytesIO
from pathlib import Path
from typing import List, Union

from airflow.models import Connection

from airflow_plus.models import CustomBaseHook


class FileSystemHook(CustomBaseHook):
    def list_path(self, path: str, recursive: bool = False) -> List[str]:
        ...

    def write_data(self, path: str, data: Union[str, bytes, BytesIO]):
        ...

    def read_data(self, path: str) -> BytesIO:
        ...


class S3FSHook(FileSystemHook):
    conn_type = 's3_filesystem_hook'
    conn_type_long = 'S3 FileSystem'

    def __init__(self, conn_params: Connection):
        from airflow.hooks.S3_hook import S3Hook
        self.conn_id = conn_params.conn_id
        self.af_s3_hook = S3Hook(aws_conn_id=self.conn_id)
        self.bucket = conn_params.extra_dejson['bucket']

    def list_path(self, path: str, recursive: bool = False) -> List[str]:
        if recursive:
            return self.af_s3_hook.list_keys(self.bucket, prefix=path)
        else:
            return self.af_s3_hook.list_prefixes(self.bucket, prefix=path)

    def write_data(self, path: str, data: Union[str, bytes, BytesIO]):
        if isinstance(data, str):
            data = data.encode()
        if isinstance(data, bytes):
            data = BytesIO(data)
        # noinspection PyProtectedMember
        self.af_s3_hook._upload_file_obj(data, bucket_name=self.bucket, key=path)

    def read_data(self, path: str) -> BytesIO:
        obj = self.af_s3_hook.get_key(path, self.bucket)
        return BytesIO(obj.get()['Body'].read())


class LocalFSHook(FileSystemHook):
    conn_type = 'local_filesystem_hook'
    conn_type_long = 'Local Filesystem'

    def __init__(self, conn_params: Connection):
        self.conn_id = conn_params.conn_id
        self.base_path = Path(conn_params.extra_dejson('base_path', '/'))

    def list_path(self, path: str, recursive: bool = False) -> List[str]:
        if recursive:
            return [str(x) for x in (self.base_path / path).rglob('*')]
        else:
            return [str(x) for x in (self.base_path / path).iterdir()]

    def write_data(self, path: str, data: Union[str, bytes, BytesIO]):
        if isinstance(data, str):
            data = data.encode()
        elif isinstance(data, BytesIO):
            data = data.getvalue()
        (self.base_path / path).write_bytes(data)

    def read_data(self, path: str) -> BytesIO:
        return BytesIO((self.base_path / path).read_bytes())


class FTPFSHook(FileSystemHook):
    conn_type = 'ftp_filesystem'
    conn_type_long = 'FTP FileSystem'

    def __init__(self, conn_params: Connection):
        from airflow.contrib.hooks.ftp_hook import FTPHook
        self.conn_id = conn_params.conn_id
        self.af_ftp_hook = FTPHook(ftp_conn_id=self.conn_id)
        self.base_path = Path(conn_params.extra_dejson('base_path', '/'))

    def list_path(self, path: str, recursive: bool = False) -> List[str]:
        if recursive:
            raise NotImplementedError('Recursive list not implemented for FTP')
        else:
            return self.af_ftp_hook.list_directory(str(self.base_path / path))

    def write_data(self, path: str, data: Union[str, bytes, BytesIO]):
        if isinstance(data, str):
            data = data.encode()
        if isinstance(data, bytes):
            data = BytesIO(data)
        self.af_ftp_hook.store_file(str(self.base_path / path), data)

    def read_data(self, path: str) -> BytesIO:
        result = BytesIO()
        self.af_ftp_hook.retrieve_file(str(self.base_path / path), result)
        return result

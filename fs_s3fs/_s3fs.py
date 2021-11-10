from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

__all__ = ["S3FS"]

import contextlib
from datetime import datetime, timedelta
import io
import itertools
import os
import tempfile
import threading
import mimetypes

import minio
from minio import S3Error
from minio.datatypes import (
    Object as minioObject
)

import six
from six import text_type

from fs import ResourceType
from fs.base import FS
from fs.info import Info
from fs import errors
from fs.mode import Mode
from fs.path import basename, dirname, forcedir, join, normpath, relpath
from fs.time import datetime_to_epoch


def _make_repr(class_name, *args, **kwargs):
    """
    Generate a repr string.

    Positional arguments should be the positional arguments used to
    construct the class. Keyword arguments should consist of tuples of
    the attribute value and default. If the value is the default, then
    it won't be rendered in the output.

    Here's an example::

        def __repr__(self):
            return make_repr('MyClass', 'foo', name=(self.name, None))

    The output of this would be something line ``MyClass('foo',
    name='Will')``.

    """
    arguments = [repr(arg) for arg in args]
    arguments.extend(
        "{}={!r}".format(name, value)
        for name, (value, default) in sorted(kwargs.items())
        if value != default
    )
    return "{}({})".format(class_name, ", ".join(arguments))


class S3File(io.IOBase):
    """Proxy for a S3 file."""

    @classmethod
    def factory(cls, filename, mode, on_close):
        """Create a S3File backed with a temporary file."""
        _temp_file = tempfile.TemporaryFile()
        proxy = cls(_temp_file, filename, mode, on_close=on_close)
        return proxy

    def __repr__(self):
        return _make_repr(
            self.__class__.__name__, self.__filename, text_type(self.__mode)
        )

    def __init__(self, f, filename, mode, on_close=None):
        self._f = f
        self.__filename = filename
        self.__mode = mode
        self._on_close = on_close

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @property
    def raw(self):
        return self._f

    def close(self):
        if self._on_close is not None:
            self._on_close(self)

    @property
    def closed(self):
        return self._f.closed

    def fileno(self):
        return self._f.fileno()

    def flush(self):
        return self._f.flush()

    def isatty(self):
        return self._f.asatty()

    def readable(self):
        return self.__mode.reading

    def readline(self, limit=-1):
        return self._f.readline(limit)

    def readlines(self, hint=-1):
        if hint == -1:
            return self._f.readlines(hint)
        else:
            size = 0
            lines = []
            for line in iter(self._f.readline, b""):
                lines.append(line)
                size += len(line)
                if size > hint:
                    break
            return lines

    def seek(self, offset, whence=os.SEEK_SET):
        if whence not in (os.SEEK_CUR, os.SEEK_END, os.SEEK_SET):
            raise ValueError("invalid value for 'whence'")
        self._f.seek(offset, whence)
        return self._f.tell()

    def seekable(self):
        return True

    def tell(self):
        return self._f.tell()

    def writable(self):
        return self.__mode.writing

    def writelines(self, lines):
        return self._f.writelines(lines)

    def read(self, n=-1):
        if not self.__mode.reading:
            raise IOError("not open for reading")
        return self._f.read(n)

    def readall(self):
        return self._f.readall()

    def readinto(self, b):
        return self._f.readinto(b)

    def write(self, b):
        if not self.__mode.writing:
            raise IOError("not open for reading")
        self._f.write(b)
        return len(b)

    def truncate(self, size=None):
        if size is None:
            size = self._f.tell()
        self._f.truncate(size)
        return size

    @property
    def length(self):
        current_offset = self.tell()
        self.seek(0, os.SEEK_END)
        length = self.tell()
        self.seek(current_offset, os.SEEK_SET)
        return length

    @property
    def mode(self):
        return self.__mode


@contextlib.contextmanager
def minioerrors(path):
    """Translate S3 errors to FSErrors."""
    try:
        yield
    except S3Error as error:
        error_code = error.code
        error_msg = error.message
        http_status = error.response.status

        if error_code == "NoSuchBucket":
            raise errors.ResourceError(path, exc=error, msg=error_msg)
        if http_status == 404:
            raise errors.ResourceNotFound(path)
        elif http_status == 403:
            raise errors.PermissionDenied(path=path, msg=error_msg)
        else:
            raise errors.OperationFailed(path=path, exc=error)


@six.python_2_unicode_compatible
class S3FS(FS):
    """
    Construct an Amazon S3 filesystem for
    `PyFilesystem <https://pyfilesystem.org>`_

    :param str bucket_name: The S3 bucket name.
    :param str dir_path: The root directory within the S3 Bucket.
        Defaults to ``"/"``
    :param str aws_access_key_id: The access key, or ``None`` to read
        the key from standard configuration files.
    :param str aws_secret_access_key: The secret key, or ``None`` to
        read the key from standard configuration files.
    :param str endpoint_url: Alternative endpoint url (``None`` to use
        default).
    :param str aws_session_token:
    :param str region: Optional S3 region.
    :param str delimiter: The delimiter to separate folders, defaults to
        a forward slash.
    :param bool strict: When ``True`` (default) S3FS will follow the
        PyFilesystem specification exactly. Set to ``False`` to disable
        validation of destination paths which may speed up uploads /
        downloads.
    :param str cache_control: Sets the 'Cache-Control' header for uploads.
    :param str acl: Sets the Access Control List header for uploads.
    :param dict upload_args: A dictionary for additional upload arguments.
        See https://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Object.put
        for details.
    :param dict download_args: Dictionary of extra arguments passed to
        the S3 client.

    """

    _meta = {
        "case_insensitive": False,
        "invalid_path_chars": "\0",
        "network": True,
        "read_only": False,
        "thread_safe": True,
        "unicode_paths": True,
        "virtual": False,
    }

    _object_attributes = [
        # all public attrs in minio.datatypes.Object
        "bucket_name",
        "content_type",
        "etag",
        "is_delete_marker",
        "is_dir",
        "is_latest",
        "last_modified",
        "metadata",
        "object_name",
        "owner_id",
        "owner_name",
        "size",
        "storage_class",
        "version_id"
    ]

    _dir_mark = ".pyfs.isdir"

    def __init__(
            self,
            bucket_name,
            dir_path="/",
            endpoint=None,
            access_key=None,
            secret_key=None,
            secure=False,
            region=None,
            http_client=None,
            delimiter="/",
            strict=True,
            upload_args=None,
            download_args=None,
    ):
        if download_args is None:
            self._download_args = {"request_headers": None}
        if upload_args is None:
            self._upload_args = {"content_type": None, "metadata": None}

        _creds = (access_key, secret_key)
        if any(_creds) and not all(_creds):
            raise ValueError(
                "access_key and secret_key "
                "must be set together if specified"
            )
        self._bucket_name = bucket_name
        self.bucket_name = bucket_name
        self.dir_path = dir_path
        self._prefix = relpath(normpath(dir_path)).rstrip("/")

        # minioClient required
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self.region = region
        self.http_client = http_client

        self.delimiter = delimiter
        self.strict = strict
        self._tlocal = threading.local()

        super(S3FS, self).__init__()

    def __repr__(self):
        return _make_repr(
            self.__class__.__name__,
            self._bucket_name,
            dir_path=(self.dir_path, "/"),
            region=(self.region, None),
            delimiter=(self.delimiter, "/"),
        )

    def __str__(self):
        return "<miniofs '{}'>".format(join(self._bucket_name, relpath(self.dir_path)))

    def _path_to_key(self, path) -> str:
        """Converts an fs path to a s3 key."""
        _path = relpath(normpath(path))
        _key = (
            "{}/{}".format(self._prefix, _path).lstrip("/").replace("/", self.delimiter)
        )
        return _key

    def _path_to_dir_key(self, path) -> str:
        """Converts an fs path to a s3 key."""
        _path = relpath(normpath(path))
        _key = (
            forcedir("{}/{}".format(self._prefix, _path))
                .lstrip("/")
                .replace("/", self.delimiter)
        )
        return _key

    def _key_to_path(self, key) -> str:
        return key.replace(self.delimiter, "/")

    def _minio_stat_object(self, path, key) -> minioObject:
        _key = key.rstrip(self.delimiter)
        with minioerrors(path):
            try:
                return self.minio.stat_object(
                    self.bucket_name,
                    _key,
                )
            except S3Error:
                current_dir = list(filter(lambda x: x.object_name.rstrip(self.delimiter) == _key, self.minio.list_objects(self.bucket_name, _key)))
                if not current_dir:
                    raise errors.ResourceNotFound(path)
                return current_dir[0]


    def _get_upload_args(self, key) -> dict:
        upload_args = self._upload_args.copy()

        if not upload_args.get("content_type"):
            mime_type, _encoding = mimetypes.guess_type(key)
            upload_args["content_type"] = mime_type or "binary/octet-stream"

        return upload_args

    @property
    def minio(self) -> minio.Minio:
        if not hasattr(self._tlocal, "minio"):
            self._tlocal.minio = minio.Minio(
                self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure,
                http_client=self.http_client,
            )
        return self._tlocal.minio

    def _extract_info_from_minio_object(self, obj: minioObject, namespaces) -> dict:
        """Make an info dict from an s3 Object."""
        key = obj.object_name  # 'a/b/c/d'
        path = self._key_to_path(key)  # noting happen
        name = basename(path.rstrip("/"))  # 'd'
        is_dir = obj.is_dir
        info = {"basic": {"name": name, "is_dir": is_dir}}

        if "details" in namespaces:
            _type = int(ResourceType.directory if is_dir else ResourceType.file)
            info["details"] = {
                "accessed": None,
                "modified": datetime_to_epoch(obj.last_modified) if obj.last_modified else None,
                "size": obj.size,
                "type": _type,
            }

        if "s3" in namespaces:
            s3info = info["s3"] = {}
            for name in self._object_attributes:
                value = getattr(obj, name, None)
                if isinstance(value, datetime):
                    value = datetime_to_epoch(value)
                s3info[name] = value

        return info

    def getinfo(self, path, namespaces=None) -> Info:
        namespaces = namespaces or ()
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)

        if _path == "/":
            return Info(
                {
                    "basic": {"name": "", "is_dir": True},
                    "details": {"type": int(ResourceType.directory)},
                }
            )

        obj = self._minio_stat_object(path, _key)
        info = self._extract_info_from_minio_object(obj, namespaces)
        return Info(info)

    def listdir(self, path) -> list:
        _path = self.validatepath(path)
        _s3_key = self._path_to_dir_key(_path)

        dirs = []
        with minioerrors(path):
            objects_list = list(self.minio.list_objects(
                self.bucket_name,
                prefix=_s3_key,
                recursive=False,
            ))
            if not objects_list and _s3_key != "":
                if self.getinfo(path).is_file:
                    raise errors.DirectoryExpected(path)
                raise errors.ResourceNotFound(path)
            for obj in objects_list:
                if obj.object_name.endswith(self._dir_mark):
                    continue
                key = basename(obj.object_name.rstrip("/"))

                dirs.append(key)

        if len(dirs) == 1 and not self.getinfo(path).is_dir:
            raise NotADirectoryError(path)

        return dirs

    def makedir(self, path, permissions=None, recreate=False):
        self.check()
        _path = self.validatepath(path)
        _key = self._path_to_dir_key(_path)

        # 检查父文件夹是否存在
        if not self.isdir(dirname(_path)):
            raise errors.ResourceNotFound(path)

        file_mark = join(_key, self._dir_mark)  # 在目录下创建一个 mark 文件，表示目录被生成

        # 标记文件已存在
        if not recreate and (self.exists(file_mark) or self.exists(path)):
            raise errors.DirectoryExists(path)

        else:
            with minioerrors(path):
                self.minio.put_object(
                    self.bucket_name,
                    file_mark,
                    io.BytesIO(b""),
                    length=0,
                )

        return self.opendir(path)

    def openbin(self, path, mode="r", buffering=-1, **options):
        mode = mode if "b" in mode else mode + "b"
        _mode = Mode(mode)
        _mode.validate_bin()
        self.check()
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)
        #
        # if _mode.create:
        #     # check ancestor exist
        #     _ancestor_path = dirname(_path)
        #     if _ancestor_path != "/":
        #         _ancestor_key = self._path_to_dir_key(_ancestor_path)
        #         self._minio_stat_object(_ancestor_path, _ancestor_key)
        #
        #     # only do if file exist
        #     try:
        #         info = self.getinfo(path)
        #     except errors.ResourceNotFound:
        #         pass
        #     else:
        #         # check file exist if in exclusive mode
        #         if _mode.exclusive:
        #             raise errors.FileExists(path)
        #
        #         # check already have same name dir
        #         if info.isdir:
        #             raise errors.FileExists(path)

        if _mode.create:

            def on_close_create(s3file):
                """Called when the S3 file closes, to upload data."""
                try:
                    s3file.raw.seek(0)
                    with minioerrors(path):
                        self.minio.put_object(
                            self._bucket_name,
                            _key,
                            s3file.raw,
                            s3file.length,
                            **self._get_upload_args(_key)
                        )
                finally:
                    s3file.raw.close()

            try:
                dir_path = dirname(_path)
                if dir_path != "/":
                    _dir_key = self._path_to_dir_key(dir_path)
                    self._minio_stat_object(dir_path, _dir_key)
            except errors.ResourceNotFound:
                raise errors.ResourceNotFound(path)

            try:
                info = self.getinfo(path)
            except errors.ResourceNotFound:
                pass
            else:
                if _mode.exclusive:
                    raise errors.FileExists(path)
                if info.is_dir:
                    raise errors.FileExpected(path)

            s3file = S3File.factory(path, _mode, on_close=on_close_create)
            if _mode.appending:
                try:
                    with minioerrors(path):
                        s3file.raw.write(self.minio.get_object(
                            self._bucket_name,
                            _key,
                            **self._download_args
                        ).read())
                except errors.ResourceNotFound:
                    pass
                else:
                    s3file.seek(0, os.SEEK_END)

            return s3file

        if self.strict:
            info = self.getinfo(path)
            if info.is_dir:
                raise errors.FileExpected(path)

        def on_close(s3file):
            """Called when the S3 file closes, to upload the data."""
            try:
                if _mode.writing:
                    s3file.raw.seek(0, os.SEEK_SET)
                    with minioerrors(path):
                        self.minio.put_object(
                            self._bucket_name,
                            _key,
                            s3file.raw,
                            s3file.length,
                            **self._get_upload_args(_key)
                        )
            finally:
                s3file.raw.close()

        s3file = S3File.factory(path, _mode, on_close=on_close)
        with minioerrors(path):
            s3file.raw.write(self.minio.get_object(
                self._bucket_name,
                _key,
                **self._download_args
            ).read())
        s3file.seek(0, os.SEEK_SET)
        return s3file

    def remove(self, path):
        self.check()
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)
        if self.strict:
            info = self.getinfo(path)
            if info.is_dir:
                raise errors.FileExpected(path)
        self.minio.remove_object(self._bucket_name, _key)

    def isempty(self, path):
        self.check()
        _path = self.validatepath(path)
        _key = self._path_to_dir_key(_path)
        return self.listdir(path) == []

    def removedir(self, path):
        self.check()
        _path = self.validatepath(path)
        if _path == "/":
            raise errors.RemoveRootError()
        info = self.getinfo(_path)
        if not info.is_dir:
            raise errors.DirectoryExpected(path)
        if not self.isempty(path):
            raise errors.DirectoryNotEmpty(path)
        _key = self._path_to_dir_key(_path)
        self.minio.remove_object(self._bucket_name, join(_key, self._dir_mark))

    def setinfo(self, path, info):
        self.getinfo(path)

    def readbytes(self, path):
        self.check()
        if self.strict:
            info = self.getinfo(path)
            if not info.is_file:
                raise errors.FileExpected(path)
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)
        bytes_file = io.BytesIO()
        with minioerrors(path):
            bytes_file.write(self.minio.get_object(
                self._bucket_name,
                _key,
                **self._download_args
            ).read())
        return bytes_file.getvalue()

    def download(self, path, file, chunk_size=None, **options):
        self.check()
        if self.strict:
            info = self.getinfo(path)
            if not info.is_file:
                raise errors.FileExpected(path)
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)
        with minioerrors(path):
            file.write(self.minio.get_object(
                self._bucket_name,
                _key,
                **self._download_args
            ).read())

    def exists(self, path):
        self.check()
        _path = self.validatepath(path)
        if _path == "/":
            return True
        _key = self._path_to_dir_key(_path)
        try:
            self._minio_stat_object(path, _key)
        except errors.ResourceNotFound:
            return False
        else:
            return True

    def scandir(self, path, namespaces=None, page=None):
        _path = self.validatepath(path)
        namespaces = namespaces or ()
        _s3_key = self._path_to_dir_key(_path)

        info = self.getinfo(path)
        if not info.is_dir:
            raise errors.DirectoryExpected(path)

        def gen_info():
            with minioerrors(path):
                for obj in self.minio.list_objects(
                        self.bucket_name,
                        prefix=_s3_key,
                        recursive=False,
                ):
                    if obj.object_name.endswith(self._dir_mark):
                        continue
                    yield Info(self._extract_info_from_minio_object(obj, namespaces))

        iter_info = iter(gen_info())
        if page is not None:
            start, end = page
            iter_info = itertools.islice(iter_info, start, end)

        for info in iter_info:
            yield info

    def writebytes(self, path, contents):
        if not isinstance(contents, bytes):
            raise TypeError("contents must be bytes")

        _path = self.validatepath(path)
        _key = self._path_to_key(_path)
        if self.strict:
            if not self.isdir(dirname(path)):
                raise errors.ResourceNotFound(path)
            try:
                info = self.getinfo(path)
                if info.is_dir:
                    raise errors.FileExpected(path)
            except errors.ResourceNotFound:
                pass

        bytes_file = io.BytesIO(contents)
        with minioerrors(path):
            self.minio.put_object(
                self._bucket_name,
                _key,
                bytes_file,
                len(contents),
                **self._get_upload_args(_key)
            )

    # def upload(self, path, file, chunk_size=None, **options):
    #     _path = self.validatepath(path)
    #     _key = self._path_to_key(_path)
    #
    #     if self.strict:
    #         if not self.isdir(dirname(path)):
    #             raise errors.ResourceNotFound(path)
    #         try:
    #             info = self._getinfo(path)
    #             if info.is_dir:
    #                 raise errors.FileExpected(path)
    #         except errors.ResourceNotFound:
    #             pass
    #
    #     with minioerrors(path):
    #         self.minio.put_object(
    #             self._bucket_name,
    #             _key,
    #             file,
    #             file.
    #             **self._get_upload_args(_key)
    #         )

    # def copy(self, src_path, dst_path, overwrite=False):
    #     if not overwrite and self.exists(dst_path):
    #         raise errors.DestinationExists(dst_path)
    #     _src_path = self.validatepath(src_path)
    #     _dst_path = self.validatepath(dst_path)
    #     if self.strict:
    #         if not self.isdir(dirname(_dst_path)):
    #             raise errors.ResourceNotFound(dst_path)
    #     _src_key = self._path_to_key(_src_path)
    #     _dst_key = self._path_to_key(_dst_path)
    #     try:
    #         with s3errors(src_path):
    #             self.client.copy_object(
    #                 Bucket=self._bucket_name,
    #                 Key=_dst_key,
    #                 CopySource={"Bucket": self._bucket_name, "Key": _src_key},
    #             )
    #     except errors.ResourceNotFound:
    #         if self.exists(src_path):
    #             raise errors.FileExpected(src_path)
    #         raise

    # def move(self, src_path, dst_path, overwrite=False):
    #     self.copy(src_path, dst_path, overwrite=overwrite)
    #     self.remove(src_path)

    def geturl(self, path, purpose="download", expires=1):
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)
        if _path == "/":
            raise errors.NoURL(path, purpose)
        if purpose == "download":
            url = self.minio.presigned_get_object(
                self._bucket_name,
                _key,
                expires=timedelta(days=1)
            )
            return url
        else:
            raise errors.NoURL(path, purpose)

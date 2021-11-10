from __future__ import unicode_literals

import unittest

import minio
from nose.plugins.attrib import attr

from fs.test import FSTestCases
from fs_s3fs_minio import S3FS


class TestS3FS(FSTestCases, unittest.TestCase):
    """Test S3FS implementation from dir_path."""

    bucket_name = "fs-minio-test"
    client = minio.Minio(
        endpoint="localhost:9000",
        region="us-west-2",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )

    def make_fs(self):
        self._delete_bucket_contents()
        return S3FS(
            self.bucket_name,
            region="us-west-2",
            access_key="minio",
            secret_key="minio123",
            endpoint="localhost:9000",
        )

    def _delete_bucket_contents(self):
        for exist_object in self.client.list_objects(self.bucket_name, recursive=True):
            self.client.remove_object(exist_object.bucket_name, exist_object.object_name)


@attr("slow")
class TestS3FSSubDir(FSTestCases, unittest.TestCase):
    """Test S3FS implementation from dir_path."""

    bucket_name = "fs-minio-test"
    client = minio.Minio(
        endpoint="localhost:9000",
        region="us-west-2",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    s3 = S3FS(
        bucket_name,
        region="us-west-2",
        access_key="minio",
        secret_key="minio123",
        endpoint="localhost:9000",
    )

    def make_fs(self):
        self._delete_bucket_contents()
        self.s3.makedir("subdirectory", recreate=True)
        return S3FS(
            self.bucket_name, dir_path="subdirectory",
            region="us-west-2",
            access_key="minio",
            secret_key="minio123",
            endpoint="localhost:9000",
        )

    def _delete_bucket_contents(self):
        for exist_object in self.client.list_objects(self.bucket_name, recursive=True):
            self.client.remove_object(exist_object.bucket_name, exist_object.object_name)


class TestS3FSHelpers(unittest.TestCase):
    def test_path_to_key(self):
        s3 = S3FS("foo")
        self.assertEqual(s3._path_to_key("foo.bar"), "foo.bar")
        self.assertEqual(s3._path_to_key("foo/bar"), "foo/bar")

    def test_path_to_key_subdir(self):
        s3 = S3FS("foo", "/dir")
        self.assertEqual(s3._path_to_key("foo.bar"), "dir/foo.bar")
        self.assertEqual(s3._path_to_key("foo/bar"), "dir/foo/bar")

    def test_upload_args(self):
        s3 = S3FS("foo", acl="acl", cache_control="cc")
        self.assertDictEqual(
            s3._get_upload_args("test.jpg"),
            {"ACL": "acl", "CacheControl": "cc", "ContentType": "image/jpeg"},
        )
        self.assertDictEqual(
            s3._get_upload_args("test.mp3"),
            {"ACL": "acl", "CacheControl": "cc", "ContentType": "audio/mpeg"},
        )
        self.assertDictEqual(
            s3._get_upload_args("test.json"),
            {"ACL": "acl", "CacheControl": "cc", "ContentType": "application/json"},
        )
        self.assertDictEqual(
            s3._get_upload_args("unknown.unknown"),
            {"ACL": "acl", "CacheControl": "cc", "ContentType": "binary/octet-stream"},
        )

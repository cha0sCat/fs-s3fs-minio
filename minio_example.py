from io import BytesIO

from minio import Minio
from minio.error import S3Error

minioClient = Minio('play.min.io',
                    access_key='Q3AM3UQ867SPQQA43P2F',
                    secret_key='zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG',
                    secure=True)

bucket_name = "function-test"
try:
    minioClient.make_bucket(bucket_name, location="us-west-2")
except S3Error as err:
    if err.code == "BucketAlreadyOwnedByYou":
        pass
    else:
        raise err


minioClient.put_object(bucket_name, "a/b/c/d", BytesIO(b"Hello World!"), length=len("Hello World!".encode()))
# minioClient.put_object(bucket_name, "a/b/c", BytesIO(b"Hello World!"), length=len("Hello World!".encode()))
# minioClient.stat_object(bucket_name, "a/b/c/d/e")
# a = list(minioClient.list_objects(bucket_name, recursive=True))
b = list(minioClient.list_objects(bucket_name, prefix="a/b/c/d", recursive=True))
# print(a)

#!/usr/bin/env python

from setuptools import setup, find_packages

with open("fs_s3fs_minio/_version.py") as f:
    exec(f.read())

CLASSIFIERS = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Operating System :: OS Independent",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Topic :: System :: Filesystems",
]

with open("README.rst", "rt") as f:
    DESCRIPTION = f.read()

REQUIREMENTS = ["boto3~=1.9", "fs~=2.4", "six~=1.10"]

setup(
    name="fs-s3fs-minio",
    author="cha0sCat",
    author_email="no@mail.com",
    classifiers=CLASSIFIERS,
    description="Amazon S3 filesystem for PyFilesystem2",
    install_requires=REQUIREMENTS,
    license="MIT",
    long_description=DESCRIPTION,
    packages=find_packages(),
    keywords=["pyfilesystem", "Amazon", "s3"],
    platforms=["any"],
    test_suite="nose.collector",
    url="https://github.com/cha0sCat/fs-s3fs-minio",
    version=__version__,
    entry_points={"fs.opener": ["s3 = fs_s3fs_minio.opener:S3FSOpener"]},
)

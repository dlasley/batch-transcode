#!/usr/bin/env python
from setuptools import setup
from setuptools import find_packages

setup(
    name = 'transcode',
    version = '3.0.0',
    description = 'Python mkv transcoding library. Uses mkvtoolnix, mediainfo, and ffmpeg',
    url = 'https://github.com/dlasley/py-transcode',
    packages=find_packages(exclude=('tests',)),
    test_suite="transcode.test.tests",
)
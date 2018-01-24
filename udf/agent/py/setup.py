from setuptools import setup
from os import path

import sys
setup_dir = path.dirname(__file__)
sys.path.insert(0, setup_dir)

from kapacitor.udf import VERSION

setup(name='kapacitor_udf',
    version=VERSION,
    packages=[
        'kapacitor',
        'kapacitor.udf',
    ],
    install_requires=[
        "protobuf==3.0.0",
    ],
    maintainer_email="support@influxdb.com",
    license="MIT",
    url="github.com/masami10/kapacitor",
    description="Kapacitor UDF Agent library",
)

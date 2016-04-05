# -*- coding: utf-8 -*-


"""setup.py: setuptools control."""


import re
from setuptools import setup


version = re.search(
    '^__version__\s*=\s*"(.*)"',
    open('deep_data_bench/deep_data_bench.py').read(),
    re.M
    ).group(1)


with open("README.rst", "rb") as f:
    long_descr = f.read().decode("utf-8")


setup(
    name = "deep_data_bench",
    packages = ["deep_data_bench"],
    install_requires=[
        "MySQL-python",
        "prettytable",
    ],
    entry_points = {
        "console_scripts": ['deep_data_bench = deep_data_bench.deep_data_bench:main',
                            'get_meta_data = deep_data_bench.metadata:main',
                            'report_viewer = deep_data_bench.ReportViewer:main',
                            'query_generator = deep_data_bench.QueryGenerator:main'
                            ]
        },
    version = version,
    description = "Python command line application for deep_data_bench.",
    long_description = long_descr,
    author = "Vincent S. King",
    author_email = "vincentsking@gmail.com",
    url = "https://github.com/DeepInfoSci/deep_data_bench",
    )

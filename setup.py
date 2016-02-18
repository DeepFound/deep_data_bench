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
    name = "cmdline-deep_data_bench",
    packages = ["deep_data_bench"],
    entry_points = {
        "console_scripts": ['deep_data_bench = deep_data_bench.deep_data_bench:main']
        },
    version = version,
    description = "Python command line application for deep_data_bench.",
    long_description = long_descr,
    author = "Vincent S. King",
    author_email = "vincentsking@gmail.com",
    url = "https://github.com/DeepInfoSci/deep_data_bench",
    )

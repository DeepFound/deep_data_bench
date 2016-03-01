# deep_data_bench
Deep Data Bench

How to install:

Ubuntu:
$ virtualenv --python=python2.7 ~/.venv/deep_data_bench; source ~/.venv/deep_data_bench/bin/activate
(deep_data_bench)$ pip install -e git+https://github.com/DeepInfoSci/deep_data_bench.git#egg=deep_data_bench

CentOS:
$ sudo yum install centos-release-SCL
$ sudo yum install python27
$ scl enable python27 bash
$ virtualenv --python=python2.7 ~/.venv/deep_data_bench; source ~/.venv/deep_data_bench/bin/activate
(deep_data_bench)$ pip install -e git+https://github.com/DeepInfoSci/deep_data_bench.git#egg=deep_data_bench


What is Deep Data Bench?

Deep Data Bench is a versatile MySQL benchmarking tool that runs with your data. 
Rather than using a consistent schema and transaction profile, Deep Data Bench 
uses special meta data files and configuration files to define and run its tests. These 
files are highly customizable and generated from a schema you specify.  
Deep Data Bench allows you to define tests that run various CRUD profiles in 
parallel, serial, and with different numbers of threads. It allows for a fine level of 
control over what those queries will look like and how long or frequently they are 
run.


Installed Programs:

deep_data_bench  -  main benchmark app

get_meta_data - for obtaining meta data json file for making very custom workloads/profiles

report_viewer -  user to view various reports from a deep_data_bench report file/object





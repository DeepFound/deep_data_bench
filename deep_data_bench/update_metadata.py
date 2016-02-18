#!/usr/bin/env python2.7
from metadata import MetaData
import datetime
import json
import argparse
import math

def update_table_size(update_meta,size_file):
	'''This will take in a meta object and a space seperated file describing
		table_name row_count
	   It should then return an updated meta object
	'''
	tables = {}
	with open(size_file) as size:
		all_table = (line.split(" ") for line in size)
		tables = { table[0] : table[1] for table in all_table}
	total_rows = 0;
	for table in tables:
		total_rows = float(total_rows) + float(tables[table])
	for table in tables:
		update_meta.table_size[table] = float(tables[table])/float(total_rows)
	update_meta.table_size['BDB_TOTAL_ROWS']['total_row_count'] = total_rows

def update_uniqueness(update_meta,index_file):
	'''This will take in a meta object and a space seperated file describing
		table_name index_column_name uniqueness
	   It should then return an updated meta object
	'''
	tables = {}
	with open(index_file) as indexes:
		all_indexes = (line.split(" ") for line in indexes)
		for table in all_indexes:
			tables[table[0]] = {}
			tables[table[0]][table[1]] = table[2]
	for table in tables:
		for index in tables[table]:	
			for col_index in range(len(update_meta.meta_data[table])):
				if update_meta.meta_data[table][col_index]['column_name'] == index:
					update_meta.meta_data[table][col_index]['uniqueness'] = int(tables[table][index])
					break

def update(update_meta,size_file,index_file):
	if size_file:
		update_table_size(update_meta,size_file)
	if index_file:
		update_uniqueness(update_meta,index_file)
	source = update_meta.meta_data
	for table in source.keys():
		for column in range(len(source[table])):
			item = lambda col_name: source[table][column][col_name]
			min, max = item('min'), item('max') #Leave defaults if no cardinality was recorded
			if item('uniqueness') == -1: 
				continue
			if item('datatype') in update_meta.text_types: #TODO:
				if item('uniqueness') == 0:
					max = 0
				else:
					max = int(math.log(int(item('uniqueness')), 26)) #We are using the lowercase asci letters for random varchar, so varchar(X) is a cardinality of 26^X
				min = 0
			elif item('datatype') in update_meta.int_types:
					min = 0
					max = min + item('uniqueness')
			elif item('datatype') in update_meta.date_types:
				if item('datatype') == 'date': #TODO:
					min = datetime.datetime.now() - datetime.timedelta(days=3650)
					max = min + datetime.timedelta(seconds=item('uniqueness'))
					min = min.strftime("%Y-%m-%d")
					max = max.strftime("%Y-%m-%d")
				elif item('datatype') == 'datetime' or item('datatype') == 'timestamp':
					min = datetime.datetime.now() - datetime.timedelta(days=7365)
					max = min + datetime.timedelta(seconds=item('uniqueness'))
					min = min.strftime("%Y-%m-%d %H:%M:%S")
					max = max.strftime("%Y-%m-%d %H:%M:%S")
				elif item('datatype') == 'time':
					min = "00:00:00"
					max = str(datetime.timedelta(seconds=item('uniqueness'))) #XXX: Might be 0:00:00
				elif item('datatype') == 'year':
					min = datetime.datetime.now() - datetime.timedelta(days=3650)
					max = min + datetime.timedelta(seconds=item('uniqueness'))
					min = min.strftime("%Y")
					max = max.strftime("%Y")
			elif item('datatype') in update_meta.float_types:
					min = 0.0
					max = min + item('uniqueness')
			else:
				min = 0
				max = 128
			source[table][column]['min'], source[table][column]['max'] = min, max
if __name__ == '__main__':
	config_items = {
					'meta_file':			{'value':None}, \
					'size_file':			{'value':None,'default':None}, \
					'index_file':			{'value':None, 'default':None}, \
					}
	parser = argparse.ArgumentParser()
	for k, v in sorted(config_items.iteritems()):
			parser.add_argument("--" + k)
	args = parser.parse_args()

	for k, v in config_items.iteritems():
		if eval(str("args."+k)) == None:
			if 'default' in config_items[k].keys():
				config_items[k]['value'] = config_items[k]['default']
			else: 
				config_items[k]['value'] = raw_input("Enter " + k + ":")
		else:
			config_items[k]['value'] = eval(str("args."+k))
	json_file = config_items['meta_file']['value']
	m = MetaData()
	m.import_from_file(json_file)
	update(m,config_items['size_file']['value'],config_items['index_file']['value'])
	m.export_to_file(json_file + ".updated.json")
	m.export_to_stdout()

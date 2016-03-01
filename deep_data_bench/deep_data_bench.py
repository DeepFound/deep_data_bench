#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

"""deep_data_bench.deep_data_bench: provides entry point main()."""
__version__ = "0.1.0"
import sys
import os
import argparse
import ConfigParser
from DBAppConductor import DBAppConductor
from PillarReport import PillarReport
from metadata import MetaData
from collections import OrderedDict
import pickle
import tempfile
import ast

def main():
	print("Welcome to Deep Data Bench. version: %s" % __version__)
	config_items = OrderedDict()

	config_items['source_mysql_user'] 			= {'section':'MySQL',	'value':None, 'default' : None} 
	config_items['source_mysql_password'] 		= {'section':'MySQL',	'value':None, 'default' : None}
	config_items['source_mysql_socket'] 		= {'section':'MySQL',	'value':None, 'default' : None}
	config_items['source_mysql_host'] 			= {'section':'MySQL',	'value':None, 'default' : None}
	config_items['source_mysql_port'] 			= {'section':'MySQL',	'value':None, 'default' : 3306}
	config_items['source_database'] 			= {'section':'MySQL',	'value':None, 'default' : None}
	config_items['destination_mysql_user'] 		= {'section':'MySQL',	'value':None}
	config_items['destination_mysql_password'] 	= {'section':'MySQL',	'value':None}
	config_items['destination_mysql_socket'] 	= {'section':'MySQL',	'value':None, 'default' : None}
	config_items['destination_mysql_host'] 		= {'section':'MySQL',	'value':None}
	config_items['destination_mysql_port'] 		= {'section':'MySQL',	'value':None, 'default' : 3306}
	config_items['destination_database'] 		= {'section':'MySQL',	'value':None}
	config_items['pillar_durations'] 			= {'section':'Pillar',  'value':None, 'default' : "60s,60s,60s"}
	config_items['pillars'] 					= {'section':'Pillar',  'value':None, 'default' : "PureLoad,EvenCRUD,Analytics"}
	config_items['num_clients'] 				= {'section':'Pillar',  'value':None, 'default' : "8,8,8"}
	config_items['show_stats_frequency'] 		= {'section':'Pillar',  'value':0, 'default':0}
	config_items['tables'] 						= {'section':'MySQL',   'value':None, 'default':'*'}
	config_items['collect_stats'] 				= {'section':'MySQL',   'value':None, 'default':False} #TODO: Make this a flag and cleanup metadata.py
	config_items['retain_destination_database'] = {'section':'MySQL',   'value':None, 'default':False} #TODO: Make this a flag
	config_items['write_sql_to_disk'] 			= {'section':'Pillar',   'value':None, 'default':False} #TODO: Make this a flag
	config_items['repeat_x_times'] 				= {'section':'Pillar',  'value':1, 'default':1}
	config_items['report_name'] 				= {'section':'Pillar',  'value':None, 'default':None}
	config_items['destination_mysql_engine'] 	= {'section':'MySQL',   'value':None, 'default':None}
	
	parser = argparse.ArgumentParser()
	config = ConfigParser.ConfigParser()
	parser.add_argument("--config",help="config containing all arguments")
	for k, v in sorted(config_items.iteritems()):
			parser.add_argument("--" + k)
	args = parser.parse_args()
	if (args.config != None):
		if os.path.isfile(args.config) and os.access(args.config, os.R_OK):
			#print "config " + args.config + " exists and is readable"	
			config.read(args.config)
			for k, v in config_items.iteritems():
				if not(config.has_section(v['section']) and config.has_option(v['section'], k)):
					if eval(str("args."+k)) != None:
						config_items[k]['value'] = eval(str("args."+k))
					elif 'default' in config_items[k].keys():
						#print "The config does not contain " + k + " in section " + v['section'] + ". Using default: " + str(config_items[k]['default'])
						config_items[k]['value'] = config_items[k]['default']
					else:	
						print "The config does not contain " + k + " in section " + v['section']
						sys.exit(1)
				else:
					#make command line override config file
					if eval(str("args."+k)) != None:
						config_items[k]['value'] = eval(str("args."+k))
					else:
						config_items[k]['value'] = config.get(v['section'], k)
		else:
			print args.config + " is either missing or is not readable."
			sys.exit(1)
	else:
		for k, v in config_items.iteritems():
			if eval(str("args."+k)) == None:
				if 'default' in config_items[k].keys():
					config_items[k]['value'] = config_items[k]['default']
				else:
					config_items[k]['value'] = raw_input("Enter " + k + ":")
			else:
				config_items[k]['value'] = eval(str("args."+k))
			if not config.has_section(config_items[k]['section']):
				config.add_section(config_items[k]['section'])		
		
		for k, v in config_items.iteritems():
			if config_items[k]['value'] is not None:
				config.set(v['section'], k, config_items[k]['value'])
		# Writing our configuration file to
		with open('my.ini', 'wb') as configfile:
		    config.write(configfile)
	for k, v in config_items.iteritems():
		print k + ":" + str(v['value'])	

	profiles_to_run = config_items['pillars']['value'].split(",")
	pillar_durations = config_items['pillar_durations']['value'].split(",")

	if config_items['source_database']['value'] is not None:
		source_databases = config_items['source_database']['value'].split(",")
	else:
		source_databases = ['placeholder']
	
	destination_databases = config_items['destination_database']['value'].split(",")
	num_clients = config_items['num_clients']['value'].split(",")

	if config_items['source_database']['value'] is not None:
		if not (len(source_databases) == len(destination_databases)):
			print "the source and destination databases must have the same number of comma separated items."
			sys.exit(1)

	if not (len(profiles_to_run) == len(pillar_durations) == len(num_clients)):
		print "pillars, pillar_durations and num_clients must have the same number of comma separated items."
		sys.exit(1)		

	#if len(profiles_to_run) < len(destination_databases):
	#	destination_databases = []
	#	for d in destination_databases:
	#		destination_databases.append(config_items['destination_database']['value'])

	todo = zip(profiles_to_run, pillar_durations,num_clients)

	for k in range(int(config_items['repeat_x_times']['value'])):

		print "starting iteration " + str(k) 
		for index, source_database in enumerate(source_databases):

			print "Source DB: " + source_database + ", Destination DB: " + destination_databases[index]

			meta = MetaData(profiles_to_run[0])
			
			if os.path.isfile(profiles_to_run[0]):
				meta.import_from_file(profiles_to_run[0])
				print "imported " + str(profiles_to_run[0])
			else:
				meta.load(config_items['source_mysql_user']['value'], config_items['source_mysql_password']['value'], config_items['source_mysql_host']['value'], int(config_items['source_mysql_port']['value']), config_items['source_mysql_socket']['value'], source_database, False)

			if not config_items['retain_destination_database']['value']:
				print "dumping into " + destination_databases[index]
				meta.dump(config_items['destination_mysql_user']['value'], config_items['destination_mysql_password']['value'], config_items['destination_mysql_host']['value'], int(config_items['destination_mysql_port']['value']), config_items['destination_mysql_socket']['value'], destination_databases[index],config_items['destination_mysql_engine']['value'])

			dict_of_profiles_to_run = {}
			
			final_report = PillarReport()
			final_report.num_tables = meta.getNumberOfTables()
			final_report.num_columns = meta.getTotalNumberOfColumns()
			final_report.num_indexes = meta.getTotalNumberOfIndexes()
			final_report.mysql_variables = meta.mysql_variables
			final_report.create_tables = meta.create_tables
			
			for pillar in todo:

				profile_file_list = []
				for profile in pillar[0].split(" "):
					m = MetaData(profile)
					if os.path.isfile(profile):
						m.import_from_file(profile)
						profile_file_list.append(profile)
					else:
						if profile == "PureLoad":
							m.load(config_items['source_mysql_user']['value'], config_items['source_mysql_password']['value'], config_items['source_mysql_host']['value'], int(config_items['source_mysql_port']['value']), config_items['source_mysql_socket']['value'], source_database, config_items['collect_stats']['value'])
						else:
							m.load(config_items['source_mysql_user']['value'], config_items['source_mysql_password']['value'], config_items['source_mysql_host']['value'], int(config_items['source_mysql_port']['value']), config_items['source_mysql_socket']['value'], source_database,False)
						f = os.path.dirname(os.path.realpath(__file__)) + "/meta_data/" + profile + ".json"  #TODO  add tables somehow to the name?  maybe just random name
						m.export_to_file(f)
						profile_file_list.append(f)
					#check for no tables
					if len(m.meta_data.keys()) == 0:
					 	print "profile: " + profile + " has no tables?  exiting..."
					 	sys.exit(1)	

				pillar_name = pillar[0].replace(" ","_")
				
				dict_of_profiles_to_run[pillar_name] = DBAppConductor(pillar[2].split(" "), 
												pillar[1],
												config_items['destination_mysql_user']['value'],
												config_items['destination_mysql_password']['value'],
												config_items['destination_mysql_host']['value'],
												int(config_items['destination_mysql_port']['value']),
												config_items['destination_mysql_socket']['value'],
												destination_databases[index],
												profile_file_list,
												config_items['tables']['value'].split(" ")
												)
				print "Running Profile: " + pillar_name + " on database " + destination_databases[index] + " ..."
				dict_of_profiles_to_run[pillar_name].setShowStatsFrequency(config_items['show_stats_frequency']['value'])
				dict_of_profiles_to_run[pillar_name].write_sql_to_disk = config_items['write_sql_to_disk']['value']
				dict_of_profiles_to_run[pillar_name].go()
				final_report.feedClientStats(pillar_name,dict_of_profiles_to_run[pillar_name].getClientStats())

			final_report.printFullReport()
			if config_items['report_name']['value'] == None:
				#f = tempfile.NamedTemporaryFile(mode='wb', delete=False, dir=os.path.dirname(os.path.realpath(__file__)) + "/reports/", suffix='.dump', prefix=source_database+'_report')
				f = tempfile.NamedTemporaryFile(mode='wb', delete=False, dir=os.getcwd(), suffix='.dump', prefix=source_database+'_report')
			else:
				#f = open(os.path.dirname(os.path.realpath(__file__)) + "/reports/" + config_items['report_name']['value'] + "_" + str(k) + ".dump",'wb')
				f = open(os.getcwd() + "/" + config_items['report_name']['value'] + "_" + str(k) + ".dump",'wb')
			pickle.dump(final_report, f)
			f.close()
			#report.dumpClientTrendDataPerProfile()
if __name__=="__main__":
	main()


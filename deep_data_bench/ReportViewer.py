#!/usr/bin/env python2.7
import pickle
import sys
from deep_data_bench import PillarReport
import argparse
from collections import OrderedDict
#import pprint
import os

def printSlowQueryDifferences(dict_of_report_objects):

	full_list_of_query_info = {}

	for key,report in dict_of_report_objects.iteritems():
		for item in report.getSlowestQueries():
			if not key in full_list_of_query_info.keys():
				full_list_of_query_info[key] = []
			full_list_of_query_info[key].append(item)


	for key, value_list in full_list_of_query_info.iteritems():
		for value in value_list:
			for key2, value_list2 in full_list_of_query_info.iteritems():
				for value2 in value_list2:
					if not key == key2:
						if value['sql'] == value2['sql']:
							print "-" * 30
							print value['sql']
							print key + ": " + str(value['execution_time'])
							print key2 + ": " + str(value2['execution_time'])
							for i, explain_row in enumerate(value['explain']):
								for k,v in explain_row.iteritems():
									if not value2['explain'][i][k] == v:
										print key + " -> explain row:" + str(i) + " explain item: " + str(k) + "  value: " + str(v)
										print key2 + " -> explain row:" + str(i) + " explain item: " + str(k) + "  value: " + str(value2['explain'][i][k]) 
		#print "key:" + key
		#for data in value:
		#	print "===================================================="
		#	print data

def printDatabaseDifferences(dict_of_report_objects):
	print ''
	max_column_length = len(os.path.basename(max(dict_of_report_objects.keys(), key=len))[:-5])
	print '.' * ((max_column_length * (len(dict_of_report_objects.keys()) + 1)+2) + ((len(dict_of_report_objects.keys()) + 1) * 5) -4 )
	print " ".ljust(max_column_length), ' | ',
	for report_name in dict_of_report_objects.keys():
		print os.path.basename(report_name).ljust(max_column_length)[:-5], ' | ',
	print ''
	print '.' * ((max_column_length * (len(dict_of_report_objects.keys()) + 1)+2) + ((len(dict_of_report_objects.keys()) + 1) * 5) -4 )
	print "Num Tables".ljust(max_column_length), ' | ',
	for report_name in dict_of_report_objects.keys():
		print str(dict_of_report_objects[report_name].num_tables).ljust(max_column_length), ' | ',
	print ''
	print "Num Columns".ljust(max_column_length), ' | ',
	for report_name in dict_of_report_objects.keys():
		print str(dict_of_report_objects[report_name].num_columns).ljust(max_column_length), ' | ',
	print ""
	print "Num Indexes".ljust(max_column_length), ' | ',
	for report_name in dict_of_report_objects.keys():
		print str(dict_of_report_objects[report_name].num_indexes).ljust(max_column_length), ' | ',
	print ""
	print '.' * ((max_column_length * (len(dict_of_report_objects.keys()) + 1)+2) + ((len(dict_of_report_objects.keys()) + 1) * 5) -4 )


def printScores(dict_of_report_objects):
	print ''
	print 'Pillar Scores:'
	max_column_length = len(os.path.basename(max(dict_of_report_objects.keys(), key=len))[:-5])
	print '.' * ((max_column_length * (len(dict_of_report_objects.keys()) + 1)+2) + ((len(dict_of_report_objects.keys()) + 1) * 5) -4 )
	print " ".ljust(max_column_length), ' | ',
	for report_name in dict_of_report_objects.keys():
		print os.path.basename(report_name).ljust(max_column_length)[:-5], ' | ',
	print ''
	print '.' * ((max_column_length * (len(dict_of_report_objects.keys()) + 1)+2) + ((len(dict_of_report_objects.keys()) + 1) * 5) -4 )

	for pillar in dict_of_report_objects[report_name].totals_per_profile.keys():
		print pillar.ljust(max_column_length), ' | ',
		for report_name in dict_of_report_objects.keys():
			
			print "{a:,.0f}".format(a=dict_of_report_objects[report_name].getProfileScore(pillar)).ljust(max_column_length), ' | ',
		print ''
	print '.' * ((max_column_length * (len(dict_of_report_objects.keys()) + 1)+2) + ((len(dict_of_report_objects.keys()) + 1) * 5) -4 )
	print "Total".ljust(max_column_length), ' | ',
	for report_name in dict_of_report_objects.keys():
		print "{a:,.0f}".format(a=dict_of_report_objects[report_name].getTotalScore()).ljust(max_column_length), ' | ',
	print ''
	print '.' * ((max_column_length * (len(dict_of_report_objects.keys()) + 1)+2) + ((len(dict_of_report_objects.keys()) + 1) * 5) -4 )


	#for report_name in dict_of_report_objects.keys():
	#	for pillar in dict_of_report_objects[report_name].totals_per_profile.keys():
	#		print os.path.basename(report_name) + " : " + pillar + " score: " + "{a:,.0f}".format(a=dict_of_report_objects[report_name].getProfileScore(pillar))
	#	
	#	print os.path.basename(report_name) + " : Total Score: " + "{a:,.0f}".format(a=dict_of_report_objects[report_name].getTotalScore())
	#	print " "

def printSummaryDifferences(dict_of_report_objects):
	max_column_length = len(os.path.basename(max(dict_of_report_objects.keys(), key=len))[:-5])
	reports = dict_of_report_objects.keys()
	for report_name in reports:
		t = 1

	for pillar in dict_of_report_objects[reports[0]].totals_per_profile.keys():
		for operation in dict_of_report_objects[report_name].totals_per_profile[pillar].query_stats.keys():
			if dict_of_report_objects[reports[0]].totals_per_profile[pillar].query_stats[operation]['count'] != 0:
				title = pillar + ' (' + operation + ')'
				print '.' * ((max_column_length * (len(dict_of_report_objects.keys()) + 1)+2) + ((len(dict_of_report_objects.keys()) + 1) * 5) -4 )

				print title.ljust(max_column_length), ' | ',
				for report_name in reports:
					print os.path.basename(report_name).ljust(max_column_length)[:-5], ' | ',
				print ''

				print '.' * ((max_column_length * (len(dict_of_report_objects.keys()) + 1)+2) + ((len(dict_of_report_objects.keys()) + 1) * 5) -4 )
				for stat_item in dict_of_report_objects[reports[0]].items_to_show:
					print stat_item[1].ljust(max_column_length), ' | ',
					for report_name in reports:
						if stat_item[0] == "count":
							v = "{a:,d}".format(a=dict_of_report_objects[report_name].totals_per_profile[pillar].query_stats[operation][stat_item[0]])
						elif stat_item[0] == "operations per second":
							#v = "{a:,.0f}".format(a=dict_of_report_objects[report_name].totals_per_profile[pillar].query_stats[operation][stat_item[0]])
							v = "{a:,.0f}".format(a=dict_of_report_objects[report_name].getAllClientsOperationRatePerProfile(pillar,operation))
						else:
							v = "{a:.4f}".format(a=dict_of_report_objects[report_name].totals_per_profile[pillar].query_stats[operation][stat_item[0]])
						print str(v).center(max_column_length), ' | ',
					print ''
				print "Average".ljust(max_column_length), ' | ',
				for report_name in reports: 
					v = "{a:.4f}".format(a=dict_of_report_objects[report_name].totals_per_profile[pillar].getAveQueryTime(operation))
					print str(v).center(max_column_length), ' | ',
				print ''	
	print '.' * ((max_column_length * (len(dict_of_report_objects.keys()) + 1)+2) + ((len(dict_of_report_objects.keys()) + 1) * 5) -4 )

def printMySQLErrorDifferences(dict_of_report_objects):
	print "TODO"


def printVariableDifferences(dict_of_report_objects):
	dict_of_variables = OrderedDict()
	for key, report in dict_of_report_objects.iteritems():
		dict_of_variables[key] = OrderedDict(sorted(report.mysql_variables.items(), key=lambda t: t[0]))
	report_files = dict_of_variables.keys()

	for report in report_files:
		for var, value in dict_of_variables[report].iteritems():
			rest_of_the_reports = list(report_files)
			if report in rest_of_the_reports: rest_of_the_reports.remove(report)
			for other_report in rest_of_the_reports:
				if var not in dict_of_variables[other_report].keys():
					dict_of_variables[other_report][var] = "--"
	#resort
	for report in report_files:
		dict_of_variables[report] = OrderedDict(sorted(dict_of_variables[report].items(), key=lambda t: t[0]))

	max_column_length = len(os.path.basename(max(dict_of_variables.keys(), key=len)))
	for var, value in dict_of_variables[report_files[1]].iteritems():
		values = [];
		for file_name in report_files:
			values.append(dict_of_variables[file_name][var])
		if values[1:] != values[:-1]:
			if len(var) > max_column_length:
				max_column_length = len(var)
	max_column_length = max_column_length + 2

	print "MySQL Variable differences:"
	print '.' * (max_column_length * (len(dict_of_variables.keys()) + 1) + ((len(dict_of_variables.keys()) + 1) * 5) -2 )
	print "var name".ljust(max_column_length), ' | ',
	for r in report_files:
		print os.path.basename(r).ljust(max_column_length), ' | ',
	print ''
	print '.' * (max_column_length * (len(dict_of_variables.keys()) + 1) + ((len(dict_of_variables.keys()) + 1) * 5) -2 )
	for var, value in dict_of_variables[report_files[1]].iteritems():
		values = []; 
		for file_name in report_files:
			values.append(dict_of_variables[file_name][var])
		if values[1:] != values[:-1]:
			print var.ljust(max_column_length), ' | ',
			for file_name in report_files:
				print dict_of_variables[file_name][var].ljust(max_column_length), ' | ',
			print " "
	print '.' * (max_column_length * (len(dict_of_variables.keys()) + 1) + ((len(dict_of_variables.keys()) + 1) * 5) -2 )

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("report", help="report file on disk")
	parser.add_argument("--summary", action="store_true", help="print summary report")
	parser.add_argument("--json_summary", action="store_true", help="print summary report in json format")
	parser.add_argument("--vars", action="store_true", help="print mysql variables")
	parser.add_argument("--db_info", action="store_true", help="print mysql database information")
	parser.add_argument("--errors", action="store_true", help="print mysql errors")
	parser.add_argument("--warnings", action="store_true", help="print mysql warnings")
	parser.add_argument("--slowest_queries", action="store_true", help="print mysql warnings")
	parser.add_argument("--trend", action="store_true", help="print query trend for graphing")
	args = parser.parse_args()

	list_of_report_files = args.report.split(",")

	if len(list_of_report_files) == 1:
		with open(args.report, "rb") as f:
			print os.getcwd()
			obj = pickle.load(f)
		if args.summary:
			obj.printFullReport()
		if args.json_summary:
			obj.printJSONSummaryReport()			
		if args.vars:
			obj.printMySQLvariables()
		if args.db_info:
			obj.printCreateDBStatements()
			obj.printDatabaseInformation()	
		if args.errors:
			obj.printAllErrors()
		if args.warnings:
			obj.printAllWarnings()
		if args.slowest_queries:
			obj.printSlowestQueries()
		if args.trend:
			obj.PrintTrendDataPerProfile()
	else:
		dict_of_report_objects = {}
		for file_name in list_of_report_files:
			with open(file_name, "rb") as f:
				dict_of_report_objects[file_name] = pickle.load(f)
		
		dict_of_report_objects = OrderedDict(sorted(dict_of_report_objects.items(), key=lambda t: t[1].getTotalScore(), reverse=True))

		if args.vars:
			printVariableDifferences(dict_of_report_objects)

		if args.errors:
			printMySQLErrorDifferences(dict_of_report_objects)

		if args.summary:
			printSummaryDifferences(dict_of_report_objects)
			printScores(dict_of_report_objects)

		if args.db_info:
			printDatabaseDifferences(dict_of_report_objects)

		if args.slowest_queries:
			printSlowQueryDifferences(dict_of_report_objects)

if __name__ == '__main__':
	main()


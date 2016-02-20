#!/usr/bin/env python2.7
from ClientQueryStats import ClientQueryStats
from collections import OrderedDict
import pickle
import sys
import json

class PillarReport(object):	
	num_tables 		= 0
	num_columns 	= 0
	num_indexes 	= 0
	db_complexity  	= 0
	mysql_variables = {}
	items_to_show 	= [('count','Count'), ('operations per second','Rate (per sec)'), ('rows_affected','rows_affected'),('min_execution_time','Min') ,('max_execution_time','Max'), ('total_execution_time','Total')]
	create_tables  	= {}

	def __init__(self):
		self.__client_stats_per_profile = OrderedDict()
		self.totals_per_profile = OrderedDict()

	def printCreateDBStatements(self):
		for table, create_text in self.create_tables.iteritems():
			print create_text
			print ''

	def feedClientStats(self,profile,stat_list):
		self.__client_stats_per_profile[profile] = list(stat_list)
		for s in stat_list:
			if profile in self.totals_per_profile.keys():
				for sql_operation in self.totals_per_profile[profile].query_stats.keys():
					for stat_item in self.totals_per_profile[profile].query_stats[sql_operation].keys():
						if stat_item.startswith('min'):
							if s.query_stats[sql_operation][stat_item] < self.totals_per_profile[profile].query_stats[sql_operation][stat_item]:
								self.totals_per_profile[profile].query_stats[sql_operation][stat_item] = s.query_stats[sql_operation][stat_item]
						elif stat_item.startswith('max'):
							if s.query_stats[sql_operation][stat_item] > self.totals_per_profile[profile].query_stats[sql_operation][stat_item]:
								self.totals_per_profile[profile].query_stats[sql_operation][stat_item] = s.query_stats[sql_operation][stat_item]
						else:
							self.totals_per_profile[profile].query_stats[sql_operation][stat_item] += s.query_stats[sql_operation][stat_item]
			else:
				self.totals_per_profile[profile] = s			

	def getProfileScore(self, profile):
		score = 0
		for sql_operation in self.totals_per_profile[profile].query_stats.keys():
			score += self.totals_per_profile[profile].query_stats[sql_operation]['count'] / self.getMaxClientDuration(profile)
		return score

	def getTotalScore(self):
		total = 0
		for profile in self.totals_per_profile.keys():
			total += self.getProfileScore(profile)
		return total

	def getNumberOfClients(self,profile):
		if profile in self.__client_stats_per_profile.keys():
			return len(self.__client_stats_per_profile[profile])
		else:
			return 0

	def getAverageClientDuration(self,profile):
		t = 0.0
		if profile in self.__client_stats_per_profile.keys():
			for d in self.__client_stats_per_profile[profile]:
				t += d.getDuration()
			return t / len(self.__client_stats_per_profile[profile])
		else:
			return t

	def getMaxClientDuration(self,profile):
		if profile in self.__client_stats_per_profile.keys():
			max_duration = 0
			for s in self.__client_stats_per_profile[profile]:
				if s.getDuration() > max_duration:
					max_duration = s.getDuration()
			return round(max_duration,4)
		else:
			return 0.0

	def getAllClientsOperationRatePerProfile(self,profile,operation):
		if profile in self.__client_stats_per_profile.keys() and self.getMaxClientDuration(profile) > 0:
 			return self.totals_per_profile[profile].query_stats[operation]['count'] / self.getMaxClientDuration(profile)
		else:
			return 0.0

	def printAllErrors(self):
		for profile in self.__client_stats_per_profile.keys():
			print "Profile: " + profile
			self.printMysqlErrors(profile)	

	def printMysqlErrors(self,profile):
		for s in self.__client_stats_per_profile[profile]:
			for k,v in s.mysql_errors.iteritems():
				print s.client_name + ": " + str(k) + " - " + v['message'] + " --count---> " + str(v['count']);

	def printAllWarnings(self):
		for profile in self.__client_stats_per_profile.keys():
			print "Profile: " + profile
			self.printMysqlWarnings(profile)		


	def getTransactionCount(self,profile):
		total = 0
		for s in self.__client_stats_per_profile[profile]:
			total += s.num_transactions
		return total

	def getAveTransactionSize(self,profile):
		total_transactions = 0
		sum_of_transaction_sizes = 0
		for s in self.__client_stats_per_profile[profile]:
			total_transactions += s.num_transactions
			sum_of_transaction_sizes += s.total_transaction_size
		if total_transactions == 0:
			return 0
		else:
			return sum_of_transaction_sizes / total_transactions

	def printSlowestQueries(self):
		slowest_queries = []
		for profile in self.__client_stats_per_profile.keys():
			slowest_queries = []
			for s in self.__client_stats_per_profile[profile]:
				slowest_queries.extend(s.slowest_queries)
			slowest_queries.sort(key=lambda dic: dic['execution_time'], reverse=False)
			print "Profile: " + profile
			for q in slowest_queries:
				print "----------------START--------------------"
				print str(q['execution_time']) + " seconds  "
				print q['sql']
				if 'explain' in q.keys():
					for item in q['explain']:
						for i,v in item.iteritems():
							print str(i) + ": " + str(v)

	def getSlowestQueries(self):
		slowest_queries = []
		for profile in self.__client_stats_per_profile.keys():
			slowest_queries = []
			for s in self.__client_stats_per_profile[profile]:
				slowest_queries.extend(s.slowest_queries)
		slowest_queries.sort(key=lambda dic: dic['execution_time'], reverse=False)
		return slowest_queries

	def getTransactionsPerSec(self,profile):
		if self.getMaxClientDuration(profile) <= 0:
			return 0
		return self.getTransactionCount(profile) / self.getMaxClientDuration(profile)

	def printMysqlWarnings(self,profile):
		for s in self.__client_stats_per_profile[profile]:
			for k,v in s.mysql_warnings.iteritems():
				print s.client_name + ": " + str(k) + " - " + v['message'] + " --count---> " + str(v['count']);

	def PrintTrendDataPerProfile(self):
		for profile,client_stats_list in self.__client_stats_per_profile.iteritems():
			print "Profile: " + profile
			trend_data = []
			for client_stats in client_stats_list:
				for item in client_stats.trend_data:
					trend_data.append(item)
			trend_data = sorted(trend_data,key=lambda x: x[1])
			for operation,elapsed_time,execution_time in trend_data:	
				print operation + "," + str(elapsed_time) + "," + str(execution_time)

	def printMySQLvariables(self):
		#print "MySQL Variables: "
		for key, value in self.mysql_variables.iteritems():
			print str(key) + "," + str(value)
	
	def printDatabaseInformation(self):
		print "Database Information:"
		print "Total Tables: " + str(self.num_tables)
		print "Total Columns: " + str(self.num_columns)
		print "Total Indexes: " + str(self.num_indexes)
		#self.db_complexity = self.num_tables + self.num_columns + self.num_indexes
		#print "Database Complexity: " + str(self.db_complexity)		

	def printStatColumns(self):
		print '{:>100}'.format("Query Execution Time (seconds)")
		print '{:<21}'.format("Operation"),
		for stat_item in self.items_to_show:
			print '{:<14}'.format(stat_item[1]),
		print '{:<14}'.format("Average")
				
	def printReportHeader(self, profile):
		print "------------------------------------------------------------------------------------------"
		print "Pillar: " + profile
		print "number of clients: " + str(self.getNumberOfClients(profile))
		print "average client duration: " + '{:.4f}'.format(self.getAverageClientDuration(profile)) + " (sec)"
		print "test duration: " + '{:.4f}'.format(self.getMaxClientDuration(profile)) + " (sec)"  
		self.printStatColumns()

	def printProfileStats(self,profile):
		for sql_operation in self.totals_per_profile[profile].query_stats.keys():
			if self.totals_per_profile[profile].query_stats[sql_operation]['count'] == 0:
				continue
			print '{:<21}'.format(sql_operation),
			for stat_item in self.items_to_show:
				if stat_item[0] == 'count' or stat_item[0] == 'rows_affected':
					print '{:<14,d}'.format(self.totals_per_profile[profile].query_stats[sql_operation][stat_item[0]]),
				elif stat_item[0] == 'operations per second':
					#print '{:<15,.0f}'.format(self.totals_per_profile[profile].query_stats[sql_operation][stat_item[0]]),
					print '{:<14,.2f}'.format(self.getAllClientsOperationRatePerProfile(profile,sql_operation)),
				else:
					print '{:<14.4f}'.format(self.totals_per_profile[profile].query_stats[sql_operation][stat_item[0]]),
			print '{:<14.4f}'.format(self.totals_per_profile[profile].getAveQueryTime(sql_operation))

	def printFullReport(self):
		print "Deep Data Bench Report"
		col_len = 15
		#show = zip(self.__client_stats_per_profile.keys(),self.__client_stats_per_profile.keys())
		for profile in self.__client_stats_per_profile.keys():
			self.printReportHeader(profile)
			self.printProfileStats(profile)
			print "Num Transactions: " + str(self.getTransactionCount(profile))
			print "Ave Transaction Size: " + str(self.getAveTransactionSize(profile))
			print "Transactions Per Second: " + str(self.getTransactionsPerSec(profile))
		print ""

	def printJSONSummaryReport(self):
		data = {}
		for profile in self.__client_stats_per_profile.keys():
			data[profile] = {}
			data[profile]['NumberOfClients'] 		= self.getNumberOfClients(profile)
			data[profile]['AverageClientDuration'] 	= self.getAverageClientDuration(profile)
			data[profile]['MaxClientDuration']		= self.getMaxClientDuration(profile)
			data[profile]['NumberOfTransactions']	= self.getTransactionCount(profile)
			data[profile]['AverageTransactionSize']	= self.getAveTransactionSize(profile)
			data[profile]['TransactionsPerSecond']	= self.getTransactionsPerSec(profile)
			data[profile]['Sql_Operation_Stats']	= {}
			for sql_operation in self.totals_per_profile[profile].query_stats.keys():
				if self.totals_per_profile[profile].query_stats[sql_operation]['count'] == 0:
					continue
				if sql_operation not in data[profile].keys():
					data[profile]['Sql_Operation_Stats'][sql_operation] = {}
				for stat_item in self.items_to_show:
					if stat_item[0] == 'operations per second':
						data[profile]['Sql_Operation_Stats'][sql_operation][stat_item[0]] = self.getAllClientsOperationRatePerProfile(profile,sql_operation)
					else:
						data[profile]['Sql_Operation_Stats'][sql_operation][stat_item[0]] = self.totals_per_profile[profile].query_stats[sql_operation][stat_item[0]]
				data[profile]['Sql_Operation_Stats'][sql_operation]['ave query time'] = self.totals_per_profile[profile].getAveQueryTime(sql_operation)

		print json.dumps(data, sort_keys=True,indent=4, separators=(',', ': '))



if __name__ == '__main__':
	print "Report " + sys.argv[1]
	with open(sys.argv[1], "rb") as input:
		obj = pickle.load(input)
	obj.printFullReport()







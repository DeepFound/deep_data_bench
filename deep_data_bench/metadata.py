#!/usr/bin/env python2.7
import MySQLdb
import warnings
import datetime
import random
import argparse
import os
#from ParseSlowQuery import ParseSlowQueryFile
from random import choice
from string import ascii_lowercase
import json
from json import encoder
encoder.FLOAT_REPR = lambda o: format(o, '.2f')
from decimal import Decimal
import sys

# workaround for json not encoding Decimal  bug: http://bugs.python.org/issue16535
# http://stackoverflow.com/questions/1960516/python-json-serialize-a-decimal-object
class DecimalEncoder(json.JSONEncoder):
	def default(self, o):
		if type(o) is datetime.date:
			return o.strftime('%Y-%m-%d')
		if type(o) is datetime.datetime:
			return o.strftime('%Y-%m-%d %H:%M:%S.%f')        
		if isinstance(o, Decimal):
			return float(o)
		return super(DecimalEncoder, self).default(o)

class MetaData(object):
 
	int_types 			= ['tinyint','smallint','mediumint','int','bigint']
	int_type_size 		= {'tinyint' : 1, 'smallint' : 2, 'mediumint' : 3, 'int' : 4, 'bigint' : 8}
	float_types			= ['float','double','decimal']
	date_types			= ['date','datetime','timestamp','time','year']
	date_formats		= {'date' : '%Y-%m-%d', 'datetime' : '%Y-%m-%d %H:%M:%S.%f', 'timestamp' : '%Y-%m-%d %H:%M:%S.%f', 'time' : '%H:%M:%S.%f', 'year' : '%Y'}
	text_types			= ['char','varchar','tinytext','text','blob','mediumtext','mediumblob','longtext','longblob','enum','set','binary']
	fixed_data			= {'table1.column2' : ['gg', 'ff', '*']}
	count 				= 0
	
	def __init__(self, profile = "Random"):
		self.use_slow_query_data = False
		self.profile 			= profile
		self.meta_data 			= {}
		self.what_to_do 		= {}
		self.index_info 		= {}
		self.foreign_key_info	= {}
		self.create_tables  	= {}
		self.create_views  		= {}
		self.mysql_variables 	= {}
		self.pattern_position 	= 0

		self.global_options = {}
		self.global_options['update_primary_key_columns'] 			= False
		self.global_options['extended_insert_size'] 				= 1
		self.global_options['min_transaction_size'] 				= 1
		self.global_options['max_transaction_size'] 				= 1
		self.global_options['rollback_chance'] 						= 0
		self.global_options['create_savepoint_chance'] 				= 0
		self.global_options['rollback_to_savepoint_chance']			= 0
		self.global_options['release_savepoint_chance']				= 0
		self.global_options['commit_early_chance']					= 0
		self.global_options['rollback_early_chance']				= 0
		self.global_options['max_tables_in_a_transaction'] 			= 30
		self.global_options['select_lock_in_share_mode_chance'] 	= 0
		self.global_options['select_order_by_chance'] 				= 3
		self.global_options['select_limit_size'] 					= 1000   # no limit if 0
		self.global_options['update_limit_size'] 					= 10   # no limit if 0
		self.global_options['delete_limit_size'] 					= 10   # no limit if 0
 		self.global_options['where_clause_range_chance'] 			= 8
 		self.global_options['where_clause_early_termination_chance'] = 3
		self.global_options['range_max_int'] 						= 1000
		self.global_options['range_max_float'] 						= 1000
		self.global_options['range_max_date_days'] 					= 1
		self.global_options['range_max_time_minutes'] 				= 4
		self.global_options['range_max_year'] 						= 1
		self.global_options['pre_generate_data'] 					= False
		self.global_options['scale_factor'] 						= 10
		self.global_options['select_group_by_chance'] 				= 8
		self.global_options['select_join_chance']					= 2
		self.global_options['insert_ignore_chance']					= 6
		self.global_options['insert_on_duplicate_key_update_chance']= 6

		if self.profile == "PureLoad":
			self.global_options['pre_generate_data']					= True 
			self.global_options['extended_insert_size'] 				= 800 
			self.global_options['min_transaction_size'] 				= 1
			self.global_options['max_transaction_size'] 				= 1
			self.global_options['insert_ignore_chance']					= 1
			self.global_options['insert_on_duplicate_key_update_chance']= 0
		elif self.profile == "JustInserts":
			self.global_options['pre_generate_data']					= True
			self.global_options['extended_insert_size'] 				= 1
			self.global_options['min_transaction_size'] 				= 1
			self.global_options['max_transaction_size'] 				= 1
			self.global_options['insert_ignore_chance']					= 0
			self.global_options['insert_on_duplicate_key_update_chance']= 6
		elif self.profile == "InsertOnDuplicateKeyUpdate":
			self.global_options['pre_generate_data']					= True
			self.global_options['extended_insert_size'] 				= 200
			self.global_options['min_transaction_size'] 				= 1
			self.global_options['max_transaction_size'] 				= 1
			self.global_options['insert_ignore_chance']					= 0
			self.global_options['insert_on_duplicate_key_update_chance']= 1
		elif self.profile == "JustUpdates":
			self.global_options['pre_generate_data']				= True 
			self.global_options['extended_insert_size'] 			= 1
			self.global_options['min_transaction_size'] 			= 1
			self.global_options['max_transaction_size'] 			= 1
			self.global_options['where_clause_range_chance'] 		= 0	
		elif self.profile == "JustDeletes":
			self.global_options['pre_generate_data']				= True
			self.global_options['extended_insert_size'] 			= 1
			self.global_options['min_transaction_size'] 			= 1
			self.global_options['max_transaction_size'] 			= 1
			#self.global_options['where_clause_range_chance'] 		= 0	
		elif self.profile == "JustReplaces":
			self.global_options['pre_generate_data']				= True
			self.global_options['extended_insert_size'] 			= 1
			self.global_options['min_transaction_size'] 			= 1
			self.global_options['max_transaction_size'] 			= 1
			#self.global_options['where_clause_range_chance'] 		= 0	
		elif self.profile == "sysbench":
			self.global_options['pre_generate_data']				= True
			self.global_options['extended_insert_size']				= 1
			self.global_options['min_transaction_size'] 			= 19
			self.global_options['max_transaction_size'] 			= 19
			self.global_options['rollback_chance'] 					= 0
			self.global_options['select_lock_in_share_mode_chance'] = 0
			self.global_options['where_clause_range_chance'] 		= 0
		elif self.profile == "TRX_CRUD":
			self.global_options['pre_generate_data'] 				= True
			self.global_options['extended_insert_size'] 			= 200
			self.global_options['min_transaction_size'] 			= 5
			self.global_options['max_transaction_size'] 			= 40
			self.global_options['rollback_chance'] 					= 20
			self.global_options['create_savepoint_chance'] 			= 20
			self.global_options['rollback_to_savepoint_chance']		= 20
			self.global_options['release_savepoint_chance']			= 20
			self.global_options['commit_early_chance']				= 20
			self.global_options['rollback_early_chance']			= 30
			self.global_options['select_lock_in_share_mode_chance'] = 30
		elif self.profile == "EvenCRUD":
			self.global_options['pre_generate_data']				= True
			self.global_options['extended_insert_size'] 			= 1
		elif self.profile == "Analytics":
			self.global_options['pre_generate_data']				= True
		elif self.profile == "Random" or self.profile == "RandomWithLoad":
			self.global_options['update_primary_key_columns'] 			= True
			self.global_options['extended_insert_size'] 				= random.randint(1,100)
			self.global_options['min_transaction_size'] 				= random.randint(1,10)
			self.global_options['max_transaction_size'] 				= random.randint(11,300)
			self.global_options['rollback_chance'] 						= random.randint(0,40)
			self.global_options['create_savepoint_chance'] 				= random.randint(0,40)
			self.global_options['rollback_to_savepoint_chance']			= random.randint(0,40)
			self.global_options['release_savepoint_chance']				= random.randint(0,40)
			self.global_options['commit_early_chance']					= random.randint(0,40)
			self.global_options['rollback_early_chance']				= random.randint(0,40)			
			self.global_options['max_tables_in_a_transaction'] 			= random.randint(3,500)
			self.global_options['select_lock_in_share_mode_chance'] 	= random.randint(0,100)
			self.global_options['where_clause_range_chance'] 			= random.randint(0,50)
			self.global_options['range_max_int'] 						= random.randint(10,1000)
			self.global_options['range_max_float'] 						= random.randint(10,1000)
			self.global_options['range_max_date_days'] 					= random.randint(10,100)
			self.global_options['range_max_time_minutes'] 				= random.randint(1,10)
			self.global_options['range_max_year'] 						= random.randint(1,3)
			self.global_options['pre_generate_data'] 					= False
		else:
			self.global_options['update_primary_key_columns'] 			= True
			self.global_options['extended_insert_size'] 				= random.randint(1,100)
			self.global_options['min_transaction_size'] 				= random.randint(1,10)
			self.global_options['max_transaction_size'] 				= random.randint(11,300)
			self.global_options['rollback_chance'] 						= random.randint(0,40)
			self.global_options['create_savepoint_chance'] 				= random.randint(0,40)
			self.global_options['rollback_to_savepoint_chance']			= random.randint(0,40)
			self.global_options['release_savepoint_chance']				= random.randint(0,40)
			self.global_options['commit_early_chance']					= random.randint(0,40)
			self.global_options['rollback_early_chance']				= random.randint(0,40)			
			self.global_options['max_tables_in_a_transaction'] 			= random.randint(3,500)
			self.global_options['select_lock_in_share_mode_chance'] 	= random.randint(0,100)
			self.global_options['where_clause_range_chance'] 			= random.randint(0,50)
			self.global_options['range_max_int'] 						= random.randint(10,1000)
			self.global_options['range_max_float'] 						= random.randint(10,1000)
			self.global_options['range_max_date_days'] 					= random.randint(10,100)
			self.global_options['range_max_time_minutes'] 				= random.randint(1,10)
			self.global_options['range_max_year'] 						= random.randint(1,3)
			self.global_options['pre_generate_data'] 					= False

	def useSlowQueryData(self,file_name):
		from ParseSlowQuery import ParseSlowQueryFile
		if os.path.isfile(file_name) and os.access(file_name, os.R_OK):
			self.slow_query_data = ParseSlowQueryFile(file_name)
			#print "loaded slow query file..."
			self.use_slow_query_data = True
		else:
			print "slow query file, " + file_name + " either does not exist or is not readable."
			self.use_slow_query_data = False

	def getColumnDataType(self, table, column):
		for v in self.meta_data[table]:
			if v['column_name'] == column:
				return v['datatype']
		return "int"

	def getColumnMinMax(self, table, column):
		for v in self.meta_data[table]:
			if v['column_name'] == column:
				return (v['min'],v['max'])

	def __get_what_to_do(self):
		if self.profile == "PureLoad":
			return { 'CRUD' : {'INSERT' : 1000, 'SELECT' : 0, 'UPDATE' : 0, 'DELETE' : 0, 'REPLACE' : 0}, 'SLEEP' : {'MIN' : 0, 'MAX' : 0}}
		if self.profile == "JustInserts":
			return { 'CRUD' : {'INSERT' : 1000, 'SELECT' : 0, 'UPDATE' : 0, 'DELETE' : 0, 'REPLACE' : 0}, 'SLEEP' : {'MIN' : 0, 'MAX' : 0}}		
		if self.profile == "InsertOnDuplicateKeyUpdate":
			return { 'CRUD' : {'INSERT' : 1000, 'SELECT' : 0, 'UPDATE' : 0, 'DELETE' : 0, 'REPLACE' : 0}, 'SLEEP' : {'MIN' : 0, 'MAX' : 0}}		
		if self.profile == "JustUpdates":
			return { 'CRUD' : {'INSERT' : 0, 'SELECT' : 0, 'UPDATE' : 1000, 'DELETE' : 0, 'REPLACE' : 0}, 'SLEEP' : {'MIN' : 0, 'MAX' : 0}}	
		if self.profile == "JustDeletes":
			return { 'CRUD' : {'INSERT' : 0, 'SELECT' : 0, 'UPDATE' : 0, 'DELETE' : 1000, 'REPLACE' : 0}, 'SLEEP' : {'MIN' : 0, 'MAX' : 0}}	
		if self.profile == "JustReplaces":
			return { 'CRUD' : {'INSERT' : 0, 'SELECT' : 0, 'UPDATE' : 0, 'DELETE' : 0, 'REPLACE' : 1000}, 'SLEEP' : {'MIN' : 0, 'MAX' : 0}}	
		elif self.profile == "MimicLoad":
			return { 'CRUD' : {'INSERT' : 1000, 'SELECT' : 0, 'UPDATE' : 0, 'DELETE' : 0, 'REPLACE' : 0}, 'SLEEP' : {'MIN' : 0, 'MAX' : 0}  }
		elif self.profile == "EvenCRUD":
			return { 'CRUD' : {'INSERT' : 200, 'SELECT' : 100, 'UPDATE' : 100, 'DELETE' : 100, 'REPLACE' : 100}, 'SLEEP' : {'MIN' : 0, 'MAX' : 0}  } 
		elif self.profile == "TRX_CRUD":
			return { 'CRUD' : {'INSERT' : 200, 'SELECT' : 100, 'UPDATE' : 100, 'DELETE' : 100, 'REPLACE' : 100}, 'SLEEP' : {'MIN' : 0, 'MAX' : 0}  }
		elif self.profile == "sysbench":
			return { 'CRUD' : {'INSERT' : 5, 'SELECT' : 100, 'UPDATE' : 10, 'DELETE' : 5, 'REPLACE' : 0}, 'SLEEP' : {'MIN' : 0, 'MAX' : 0}  
					,'PATTERN2' : [('S',10),('U',3),('I',1),('D',1)]
					,'PATTERN' : ['SELECT','SELECT','SELECT','SELECT','SELECT','SELECT','SELECT','SELECT','SELECT','SELECT','SELECT','SELECT','SELECT','SELECT','UPDATE','UPDATE','UPDATE','DELETE','INSERT']
					}
		elif self.profile == "Analytics":
			return { 'CRUD' : {'INSERT' : 0, 'SELECT' : 1000, 'UPDATE' : 0, 'DELETE' : 0, 'REPLACE' : 0}, 'SLEEP' : {'MIN' : 0, 'MAX' : 0}  }   
		elif self.profile == "Random":
			return { 'CRUD' : {'INSERT' : random.randint(0,1000), 'SELECT' : random.randint(0,1000), 'UPDATE' : random.randint(0,1000), 'DELETE' : random.randint(0,1000), 'REPLACE' : random.randint(0,1000)}, 'SLEEP' : {'MIN' : 0, 'MAX' : 0}  }   
		elif self.profile == "RandomWithLoad":
			return { 'CRUD' : {'INSERT' : 1000, 'SELECT' : random.randint(0,1000), 'UPDATE' : random.randint(0,1000), 'DELETE' : random.randint(0,1000), 'REPLACE' : random.randint(0,1000)}, 'SLEEP' : {'MIN' : 0, 'MAX' : 0}  }   
		else:
			return { 'CRUD' : {'INSERT' : random.randint(0,1000), 'SELECT' : random.randint(0,1000), 'UPDATE' : random.randint(0,1000), 'DELETE' : random.randint(0,1000), 'REPLACE' : random.randint(0,1000)}, 'SLEEP' : {'MIN' : 0, 'MAX' : 0}  }
	
	def add_foreign_key_relationship(self, pk_table, pk_column, fk_table, fk_column):
		for i,v in enumerate(self.meta_data[pk_table]):
			if v['column_name'] == pk_column:
				self.meta_data[pk_table][i]['foreign_keys'].append(fk_table + "." + fk_column)

	def randomly_create_foreign_key_relationships(self):
		for table,value in self.meta_data.iteritems():
			print table

	def getNumberOfTables(self):
		return len(self.create_tables)

	def getNumberOfViews(self):
		return len(self.create_views)

	def getNumberOfColumns(self,table):
		return len(self.meta_data[table])

	def getTotalNumberOfColumns(self):
		total = 0
		for table in self.create_tables.keys():
			total += self.getNumberOfColumns(table)
		return total

	def getNumberOfIndexes(self,table):
		return len(self.index_info[table])

	def getTotalNumberOfIndexes(self):
		total = 0
		for table in self.index_info.keys():
			total += len(self.index_info[table])
		return total

	def columnInAnIndex(self,table,column):
		for indx in self.index_info[table]:
			if column == self.index_info[table][indx][0]:
				#print "col: " + column + "  table: " + table + " is in an index "
				return True
		#print "col: " + column + "  table: " + table + " is not in an index ..."
		return False

	def columnInPrimary(self,table,column):
		if 'PRIMARY' in self.index_info[table].keys():
			if column in self.index_info[table]['PRIMARY']:
				return True
		return False

	def findSomethingToDo(self,table):
		#let's psudo randomly pick something to do based on the CRUD wieghts
		todo = None
		if 'PATTERN' in self.what_to_do[table].keys():
			todo = self.what_to_do[table]['PATTERN'][self.pattern_position]
			self.pattern_position += 1
			if self.pattern_position >= len(self.what_to_do[table]['PATTERN']):
				self.pattern_position = 0
			return todo
		elif 'CRUD' in self.what_to_do[table].keys(): 
			if sum(self.what_to_do[table]['CRUD'].values()) == 0:
				return None
			for key in self.what_to_do[table]['CRUD'].keys():
				if self.what_to_do[table]['CRUD'][key] == sum(self.what_to_do[table]['CRUD'].values()):
					return key
			if todo == None:
				weights		= self.what_to_do[table]['CRUD'].values()
				strings		= self.what_to_do[table]['CRUD'].keys()
				return strings[self.__grabWeightedIndex(weights)]

	def chooseATable(self,tables):

		if self.count < 10000:
			self.count = self.count + 1
			return random.choice(tables)

		weights = []
		strings = []
		for t in tables:
			strings.append(t)
			weights.append(self.what_to_do[t]['SIZE'])
		if sum(weights) == 0:
			return random.choice(tables)
		return strings[self.__grabWeightedIndex(weights)]


	def __grabWeightedIndex(self,values):
		rand 		= ( random.randint(1, 1000) / 1000.00 ) * sum(values)
		currTotal 	= 0
		index 		= 0
		for amount in values:
			currTotal += amount
			if rand > currTotal:
				index += 1
			else:
				break
		return index

	def export_to_file(self,json_file):
		f = open(json_file,'w')
		f.write(json.dumps([self.meta_data,self.what_to_do,self.index_info,self.create_tables,self.create_views,self.global_options,self.fixed_data], sort_keys=True,indent=4, separators=(',', ': '), cls=DecimalEncoder))
		f.close()

	def export_to_stdout(self):
		print json.dumps([self.meta_data,self.what_to_do,self.index_info,self.create_tables,self.create_views,self.global_options,self.fixed_data], sort_keys=True,indent=4, separators=(',', ': '), cls=DecimalEncoder)

	def import_from_file(self,json_file):
		with open(json_file) as data_file:    
			data = json.load(data_file)
		self.meta_data 		= data[0]
		self.what_to_do 	= data[1]
		self.index_info 	= data[2]
		self.create_tables 	= data[3]
		self.create_views 	= data[4]
		self.global_options = data[5]
		self.fixed_data 	= data[6]

	def dump(self, mysqluser, mysqlpass, host, port, socket, database, engine):
		try:
			if socket == None:
				self.__db = MySQLdb.connect(host=host, port=port, user=mysqluser, passwd=mysqlpass)
			else:
				self.__db = MySQLdb.connect(unix_socket=socket, user=mysqluser, passwd=mysqlpass)
			self.__db.autocommit(1)
			self.__cur = self.__db.cursor()
			self.__cur.execute("drop database if exists " + database)
			self.__cur.execute("create database IF NOT EXISTS " + database)
			self.__cur.execute("use " + database)
			self.__cur.execute("SET FOREIGN_KEY_CHECKS=0")
			for table, create_statement in self.create_tables.iteritems():
				print "Creating table " + table
				#print create_statement
				self.__cur.execute(create_statement)
				if engine != None:
					print "Altering table " + table + " to ENGINE=" + str(engine)
					self.__cur.execute("ALTER table " + table + " ENGINE=" + str(engine))

		except MySQLdb.Error, e:
			print "An Error occured in " + self.__class__.__name__ + " %s" %e
			print "Exiting..."
			sys.exit(1)

		views_not_created = self.create_views.values()

		#hackery to create views potentialy dependent on other views.
		while len(views_not_created) > 0:
			for i,create_statement in enumerate(views_not_created):
				print "Creating view " + str(i) + " - " + str(create_statement[:40])
				if self.__execute_query(create_statement):
					views_not_created.pop(i)
			random.shuffle(views_not_created)

	def __execute_query(self, sql):
		with warnings.catch_warnings():
			warnings.simplefilter('error', MySQLdb.Warning)
			try:
				self.__cur.execute(sql)
				return True
			except MySQLdb.Error, e:
				print "An Error occured running query. %s" %e
				#print sql;
				return False
			except MySQLdb.Warning, e:
				print "An Warning occured running query. %s" %e
				return True
			except MySQLdb.ProgrammingError, e:
				print "A ProgrammingError occured running query. %s" %e
				exit(1)
				return False


	def __generateRandomString(self,length=10):
		#return self.random_worker.grabAstring(length)
		return ''.join(choice(list(ascii_lowercase)) for _ in xrange(length))

	def load(self, mysqluser, mysqlpass, host, port, socket, database, collect_stats = True):
		self.__mysqluser 	= mysqluser
		self.__mysqlpass 	= mysqlpass
		self.__host			= host
		self.__port			= port
		self.__socket		= socket
		self.__database		= database

		try:
			if socket == None:
				self.__db = MySQLdb.connect(host=self.__host, port=self.__port, user=self.__mysqluser, passwd=self.__mysqlpass, db=self.__database)
			else:
				self.__db = MySQLdb.connect(unix_socket=self.__socket, user=self.__mysqluser, passwd=self.__mysqlpass, db=self.__database)
			self.__db.autocommit(1)
			self.__cur = self.__db.cursor()
		except MySQLdb.Error, e:
			#print "An Error occured running. %s" %e
			print "An Error occured in " + self.__class__.__name__ + " %s" %e
			exit()

		self.__cur.execute("show variables")
		rows = self.__cur.fetchall()
		for row in rows:
			self.mysql_variables[row[0]] = row[1]

		#obtain foreign key info
		self.__cur.execute("select concat(referenced_table_name, '.', referenced_column_name) as 'primary_key', concat(table_name, '.', column_name) as 'foreign_key', constraint_name as 'name' from information_schema.key_column_usage where referenced_table_name is not null and table_schema = '" + self.__database + "' order by primary_key;")
		rows = self.__cur.fetchall()
		for row in rows:
			if row[0] not in self.foreign_key_info.keys():
				self.foreign_key_info[row[0]] = []
				self.foreign_key_info[row[0]].append(row[1])
			else:
				self.foreign_key_info[row[0]].append(row[1])

		self.__cur.execute("SELECT TABLE_NAME,TABLE_TYPE FROM information_schema.tables WHERE TABLE_SCHEMA='" + self.__database + "'")
		rows = self.__cur.fetchall()
		for row in rows:

			table = row[0]
			table_type = row[1]

			self.index_info[table] = {}
			self.meta_data[table] = []
			self.what_to_do[table] = {}

			if self.use_slow_query_data:
				self.what_to_do[table] = { 'CRUD' : {'INSERT' : self.slow_query_data.getQuerySpecificQueryCountForATable('INSERT',table), 
													 'SELECT' : self.slow_query_data.getQuerySpecificQueryCountForATable('SELECT',table), 
													 'UPDATE' : self.slow_query_data.getQuerySpecificQueryCountForATable('UPDATE',table), 
													 'DELETE' : self.slow_query_data.getQuerySpecificQueryCountForATable('DELETE',table), 
													 'REPLACE' : self.slow_query_data.getQuerySpecificQueryCountForATable('REPLACE',table)}, 
										   'SLEEP' : {'MIN' : 0, 'MAX' : 0}}
			else:
				self.what_to_do[table] = self.__get_what_to_do()
 			

 			self.what_to_do[table]['TABLE_TYPE'] = table_type

			#obtain index info
			self.__cur.execute("SHOW INDEX FROM " + table + " FROM " + self.__database)
			name_to_index = dict((d[0], i) for i, d in enumerate(self.__cur.description))
			rows2 = self.__cur.fetchall()
			for row2 in rows2:
				item = lambda col_name: row2[name_to_index[col_name]]
				if item('Key_name') in self.index_info[table].keys():
					self.index_info[table][item('Key_name')].append(item('Column_name'))
				else:
					self.index_info[table][item('Key_name')] = [item('Column_name')]

			if self.use_slow_query_data:
				self.what_to_do[table]['SIZE'] = self.slow_query_data.getOverallQueryCountForATable(table)
			else:
				self.__cur.execute("SELECT TABLE_ROWS FROM information_schema.tables WHERE TABLE_NAME='" + table + "' AND TABLE_SCHEMA='" + self.__database + "'")
				table_count = self.__cur.fetchone()[0]
				if table_count == None:
					self.what_to_do[table]['SIZE'] = 0
				else:
					self.what_to_do[table]['SIZE'] = int(int(table_count) * self.global_options['scale_factor'])

			# get create table info
			self.__cur.execute("SHOW CREATE TABLE " + table + ";")
			create_info = self.__cur.fetchone()
			if table_type == "BASE TABLE":
				self.create_tables[create_info[0]] = create_info[1]
			elif table_type == "VIEW":
				self.create_views[create_info[0]] = create_info[1]

			self.__cur.execute("SELECT * FROM information_schema.columns WHERE TABLE_NAME='" + table + "' AND TABLE_SCHEMA='" + self.__database + "'")
			name_to_index = dict((d[0], i) for i, d in enumerate(self.__cur.description))
			rows2 = self.__cur.fetchall()

			for row2 in rows2:
				item = lambda col_name: row2[name_to_index[col_name]]

				uniqueness = 100
				if collect_stats and table_type != 'VIEW' and self.columnInAnIndex(table,item('COLUMN_NAME')):
					#self.__cur.execute("SELECT count(DISTINCT(`" + item('COLUMN_NAME') + "`)) FROM " + table)
					self.__cur.execute("SELECT CARDINALITY FROM information_schema.statistics where TABLE_SCHEMA='" + self.__database + "' AND TABLE_NAME='" + table + "' AND COLUMN_NAME='" + item('COLUMN_NAME') + "'")
					result = self.__cur.fetchone()[0]
					if result != None:
						uniqueness = int(result * self.global_options['scale_factor']) #Grab the uniqueness
				if item('DATA_TYPE') in self.text_types:
					max = item('CHARACTER_MAXIMUM_LENGTH')
					if max > 1024:
						max = 200
					min = int(max / 10)
					if min == 0:
						min = 1
					max = int(min * 2)
					if max > item('CHARACTER_MAXIMUM_LENGTH'):
						max = item('CHARACTER_MAXIMUM_LENGTH')

					the_item = table + "." + item('COLUMN_NAME')

					if item('DATA_TYPE') == "enum" or item('DATA_TYPE') == "set":
						self.__cur.execute("SELECT COLUMN_TYPE FROM information_schema.columns WHERE table_name = '" + table + "' AND column_name = '" + item('COLUMN_NAME') + "' AND TABLE_SCHEMA='" + self.__database + "'")
						self.fixed_data[the_item] = []
						self.fixed_data[the_item].extend(self.__cur.fetchone()[0][5:-1].replace("'","").split(','))

					if collect_stats and table_type != 'VIEW':
						if uniqueness != self.what_to_do[table]['SIZE'] and the_item not in self.fixed_data.keys():
							self.fixed_data[the_item] = []
							if float(uniqueness) / float(self.what_to_do[table]['SIZE']+1) > 0.9:
								#print "highly unique but not totally."
								self.fixed_data[the_item].append(self.__generateRandomString(max))
								self.fixed_data[the_item].append('*')
								self.fixed_data[the_item].append('*')
								self.fixed_data[the_item].append('*')
								self.fixed_data[the_item].append('*')
							elif float(uniqueness) / float(self.what_to_do[table]['SIZE']+1) > 0.8:
								self.fixed_data[the_item].append(self.__generateRandomString(max))
								self.fixed_data[the_item].append('*')
								self.fixed_data[the_item].append('*')
								self.fixed_data[the_item].append('*')								
							elif float(uniqueness) / float(self.what_to_do[table]['SIZE']+1) > 0.7:
								self.fixed_data[the_item].append(self.__generateRandomString(max))
								self.fixed_data[the_item].append('*')
								self.fixed_data[the_item].append('*')								
							elif float(uniqueness) / float(self.what_to_do[table]['SIZE']+1) > 0.5:
								self.fixed_data[the_item].append(self.__generateRandomString(max))
								self.fixed_data[the_item].append('*')							
							elif float(uniqueness) / float(self.what_to_do[table]['SIZE']+1) > 0.3:
								self.fixed_data[the_item].append(self.__generateRandomString(max))
								self.fixed_data[the_item].append(self.__generateRandomString(max))
								self.fixed_data[the_item].append('*')								
							else:
								#print "making fixed data, uniqueness: " + str(uniqueness)
								for i in range(0,uniqueness):
									self.fixed_data[the_item].append(self.__generateRandomString(max))

				elif item('DATA_TYPE') in self.int_types:
					if 'unsigned' in item('COLUMN_TYPE'):
						min = 0
						max = (2 ** (self.int_type_size[item('DATA_TYPE')] * 8)) - 1;					
					else:
						min = (2 ** ((self.int_type_size[item('DATA_TYPE')] * 8 ) - 1)) * -1
						max = (2 ** ((self.int_type_size[item('DATA_TYPE')] * 8 ) - 1)) - 1
				
					if collect_stats and self.columnInAnIndex(table,item('COLUMN_NAME')):
						self.__cur.execute("SELECT MIN(`" + item('COLUMN_NAME') + "`), MAX(`" + item('COLUMN_NAME') + "`) FROM " + table)
						r = self.__cur.fetchone()
						if r[0] is not None and r[1] is not None:
							min = int(r[0])
							max = int(r[1] * self.global_options['scale_factor'])
							if max > (2 ** (self.int_type_size[item('DATA_TYPE')] * 8)) - 1:
								max = (2 ** (self.int_type_size[item('DATA_TYPE')] * 8)) - 1;

				elif item('DATA_TYPE') in self.date_types:
					if item('DATA_TYPE') == 'date':
						min = (datetime.datetime.now() - datetime.timedelta(days=17365)).strftime(self.date_formats[item('DATA_TYPE')])
						max = datetime.datetime.now().strftime(self.date_formats[item('DATA_TYPE')])
					elif item('DATA_TYPE') == 'datetime':
						min = (datetime.datetime.now() - datetime.timedelta(days=17365)).strftime(self.date_formats[item('DATA_TYPE')])
						max = datetime.datetime.now().strftime(self.date_formats[item('DATA_TYPE')])
					elif item('DATA_TYPE') == 'timestamp':
						min = datetime.datetime(1970, 1, 1).strftime(self.date_formats[item('DATA_TYPE')])
						max = datetime.datetime.now().strftime(self.date_formats[item('DATA_TYPE')])
					elif item('DATA_TYPE') == 'time':
						min = "00:00:00.000000"
						max = "23:59:59.999999"
					elif item('DATA_TYPE') == 'year':
						min = (datetime.datetime.now() - datetime.timedelta(days=18650)).strftime(self.date_formats[item('DATA_TYPE')])
						max = datetime.datetime.now().strftime(self.date_formats[item('DATA_TYPE')])
					
					if collect_stats and self.columnInAnIndex(table,item('COLUMN_NAME')):
						self.__cur.execute("SELECT MIN(`" + item('COLUMN_NAME') + "`), MAX(`" + item('COLUMN_NAME') + "`) FROM " + table)
						r = self.__cur.fetchone()
						if r[0] is not None and r[1] is not None:
							min = r[0]
							max = r[1]

				elif item('DATA_TYPE') in self.float_types:
					if item('DATA_TYPE') == 'double':
						if 'unsigned' in item('COLUMN_TYPE'):
							min = 0.0
						else:
							min = ((2 ** ((self.int_type_size['bigint'] * 8 ) - 1)) * -1.0)/950000.00
						max = ((2 ** ((self.int_type_size['bigint'] * 8 ) - 1)) - 1.0)/950000.00
					else:
						if 'unsigned' in item('COLUMN_TYPE'):
							min = 0.0
						else:
							min = ((2 ** ((self.int_type_size['int'] * 8 ) - 1)) * -1.0)/950000.00
						max = ((2 ** ((self.int_type_size['int'] * 8 ) - 1)) - 1.0)/950000.00

					if collect_stats and table_type != 'VIEW':
						self.__cur.execute("SELECT MIN(`" + item('COLUMN_NAME') + "`), MAX(`" + item('COLUMN_NAME') + "`) FROM " + table)
						r = self.__cur.fetchone()
						if r[0] is not None and r[1] is not None:
							min = r[0]
							max = r[1] * self.global_options['scale_factor']
				else:
					min = 0
					max = 128

				# XXX we need to unify string and boolean types
				if collect_stats == "False" or collect_stats == "false":
					collect_stats = False
				if collect_stats == "True" or collect_stats == "true":
					collect_stats = True

				method = 'random'
				if item('EXTRA') == "auto_increment":
					method = 'ignore'
				if not collect_stats:
					if item('COLUMN_KEY') == 'PRI' or item('COLUMN_KEY') == 'UNI' or item('COLUMN_KEY') == 'MUL':
						method = 'autoinc'
				curr_foreign_key_list = []
				if table + "." + item('COLUMN_NAME') in self.foreign_key_info.keys():
					for fk in self.foreign_key_info[table + "." + item('COLUMN_NAME')]:
						curr_foreign_key_list.append(fk)

				self.meta_data[row[0]].append({'column_name':item('COLUMN_NAME'),
												'datatype':item('DATA_TYPE'),
												'method': method,
												'min': min,
												'max': max,
												'current_min': None,
												'current_max': None,				
												'uniqueness': uniqueness,
												'col_key': item('COLUMN_KEY'),
												'foreign_keys': curr_foreign_key_list
												})
		self.__db.close()

class ParseSlowQueryFile():
	def __init__(self,filename):

		self.query_type_counts = {	"INSERT" : {"all_tables" : 0},
									"SELECT" : {"all_tables" : 0},
									"UPDATE" : {"all_tables" : 0},
									"DELETE" : {"all_tables" : 0},
									"REPLACE": {"all_tables" : 0}
								}
		with open(filename, "r") as f:
			for line in f:
				line.strip()
				if line[0] == "#":
					continue
				query_type = line.split(' ', 1)[0]
				if query_type in self.query_type_counts.keys():
					tables = None
					if query_type == "UPDATE":						
						tables = line.split(' ', 2)[1]
						tables = tables.split(',')
					elif query_type in ["SELECT","DELETE"]:
						if "FROM" in line:
							#print line
							tables = line.split('FROM',1)[1].strip()
							tables = tables.split(' ',1)[0]
							tables = tables.split(',')
						else:
							continue
					elif query_type in ["INSERT","REPLACE"]:
						if "INTO" in line:
							tables = line.split('INTO',1)[1].strip()
							tables = tables.split(' ',1)[0]
							tables = tables.split(',')
						else:
							continue

					if tables != None:
						for table in tables:
							self.query_type_counts[query_type]['all_tables'] += 1
							table = table.strip(";`'")
							if table not in self.query_type_counts[query_type].keys():
								self.query_type_counts[query_type][table] = 1
							else:
								self.query_type_counts[query_type][table] += 1
				else:
					continue

	def getOverallQueryCountForATable(self,table):
		count = 0
		for key, value in self.query_type_counts.iteritems():
			if table in value.keys():
				count += value[table]
		return count

	def getQuerySpecificQueryCountForATable(self,operation,table):
		if operation not in self.query_type_counts.keys():
			return 0
		if table not in self.query_type_counts[operation].keys():
			return 0
		return self.query_type_counts[operation][table]

	def printQueryTypeCount(self):
		print json.dumps(self.query_type_counts, sort_keys=True,indent=4, separators=(',', ': '))


def main():
	config_items = {
					'mysql_user':			{'value':None}, \
					'mysql_password':		{'value':None}, \
					'mysql_host':			{'value':None}, \
					'mysql_port':			{'default':3306}, \
					'database':				{'value':None}, \
					'profile':				{'default':"Random"},  \
					'collect_stats':		{'default':True},  \
					'pre_generate_data':	{'default':False},  \
					'slow_query_file':		{'default':False}  \
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

	m = MetaData(config_items['profile']['value'])
	m.global_options['pre_generate_data'] = config_items['pre_generate_data']['value']
	if config_items['slow_query_file']['value'] != False:
		m.useSlowQueryData(config_items['slow_query_file']['value'])
	
	if config_items['collect_stats']['value'] == "False" or config_items['collect_stats']['value'] == "false":
		config_items['collect_stats']['value'] = False
	if config_items['collect_stats']['value'] == "True" or config_items['collect_stats']['value'] == "true":
		config_items['collect_stats']['value'] = True

	m.load(config_items['mysql_user']['value'], config_items['mysql_password']['value'], config_items['mysql_host']['value'],config_items['mysql_port']['value'],None,config_items['database']['value'],config_items['collect_stats']['value'])
	m.export_to_stdout()

if __name__ == '__main__':
	main()


#!/usr/bin/env python2.7
import MySQLdb
from metadata import MetaData
import random
from random import choice
from string import letters,digits,printable,hexdigits
import warnings
import time
import os,binascii
from datetime import datetime
from datetime import timedelta
import timeit
import math
import argparse
import multiprocessing
from ClientQueryStats import ClientQueryStats
#from RandomWorker import RandomWorker
import pickle
import thread
from decimal import Decimal
import types

class QueryGenerator(multiprocessing.Process):

	#join_types = ['INNER','LEFT','RIGHT']
	join_types = ['INNER','LEFT']
	aggregate_functions = ['MIN', 'MAX', 'AVG', 'COUNT', 'SUM']

	def __init__(self, pipe_home, mysqluser, mysqlpass, host, port, socket, database, table, duration, profile = "Random", write_sql_to_disk = False):
		multiprocessing.Process.__init__(self)
		self.pipe_home 		= pipe_home

		self.savepoint_list					= []
		self.savepoint_counter 				= 0
		self.potential_tables 				= []
		self.__begin_sql_insert	 			= {}
		self.__insert_sql 					= {}
		self.transaction_size 				= 1
		self.iterations_instead_of_duration  = True
		self.write_sql_to_disk 				= write_sql_to_disk
		self.update_min_max_counter			= 0
		self.tables_in_transaction 			= []
		self.num_inserts 					= 0
		self.delta 							= 0.0
		self.start_time 					= 0.0
		self.poison_pill 					= False
		self.where_clause_gen_having_trouble = 0
		self.where_clause_gen_trying_desc = 0

		if os.path.isfile(profile):
			self.__metadata 	= MetaData(profile)
			self.__metadata.import_from_file(profile)
			self.profilefilename = os.path.splitext(os.path.basename(profile))[0]
			#print self.name + " loaded " + profile + " from disk"
			# loaded Profile from disk.
		else:
			self.__metadata 	= MetaData(profile)
			self.__metadata.load(mysqluser, mysqlpass, host, port, socket, database, True)
			self.__metadata.export_to_file(os.path.dirname(os.path.realpath(__file__)) + "/meta_data/" + database + "." + profile + ".json")
			self.profilefilename = profile
			#print "creating profile on the fly..." 
		self.profile			= profile
		self.write_sql_to_file	= os.path.dirname(os.path.realpath(__file__)) + "/meta_data/" + self.name + self.profilefilename + ".sql"

		duration = str(duration)
		if duration[-1] == 's':
			self.iterations_instead_of_duration = False
			duration = duration[:-1]
			self._duration 	= float(duration)
		else:
			self.iterations_instead_of_duration = True
			self._duration 	= int(duration)

		self.__mysqluser 	= mysqluser
		self.__mysqlpass 	= mysqlpass
		self.__host			= host
		self.__port			= port
		self.__socket		= socket
		self.__database		= database
		self.__table		= table
		
		self.auto_increment_increment 	= 1  # number of clients
		self.auto_increment_offset 		= 1  # client number
		self.statistics = ClientQueryStats(self.name)
		#self.random_worker = RandomWorker()
		self.generated_queries = {}

		self.connect_to_mysql()

		self.__initialize_things()

	def connect_to_mysql(self):
		try:
			if self.__socket == None:
				self.__db = MySQLdb.connect(host=self.__host, port=self.__port, user=self.__mysqluser, passwd=self.__mysqlpass, db=self.__database)
			else:
				self.__db = MySQLdb.connect(unix_socket=self.__socket, user=self.__mysqluser, passwd=self.__mysqlpass, db=self.__database)
			self.__db.autocommit(1)
			self.__cur = self.__db.cursor()
			self.__cur.execute("SET @@session.time_zone='+00:00'")
			#self.__cur.execute('set unique_checks=0')
		except MySQLdb.Error, e:
			print "An Error occured in " + self.__class__.__name__ + " %s" %e
			exit(1)		

	def getMetaDataProfile(self):
		return self.__metadata.profile

	def getMetaDataWhatToDo(self):
		return self.__metadata.what_to_do

	def createQueries(self):

		amt = int(self._duration / len(self.potential_tables)) * 4
		#amt = int(self._duration)

		for table_name in self.potential_tables:


			outfile = os.path.dirname(os.path.realpath(__file__)) + "/data/" + self.name + "." + table_name + "." + str(amt) + "." + str(self.profilefilename)
			#print outfile
			if os.path.isfile(outfile):
				#print self.name + " Loading pickled file " + outfile
				with open(outfile,'r') as f:
					self.generated_queries[table_name] = pickle.load(f)
				continue

			for row in range(1,amt):

				#if self.where_clause_gen_having_trouble % 10 == 0 and self.where_clause_gen_having_trouble != 0 :
				#	print "self.where_clause_gen_having_trouble: " + str(self.where_clause_gen_having_trouble)

				#if self.where_clause_gen_trying_desc % 10 == 0 and self.where_clause_gen_trying_desc != 0:	
				#	print "self.where_clause_gen_trying_desc: " + str(self.where_clause_gen_trying_desc)

				todo = self.__metadata.findSomethingToDo(table_name)
				q = self.generateQuery(table_name,todo)
				if q != None:
					self.generated_queries[table_name][todo].append(q)
					#print "table: " + str(table_name) + "iteration: " + str(row) + " todo: " + str(todo) 
				#else:
				#	print "failed to generate " + str(todo)  + " query for table: " + str(table_name) 
			with open(outfile,'wb') as f:
				#print self.name + "Saving pickled file " + outfile
				pickle.dump(self.generated_queries[table_name], f)

		self.__metadata.pattern_position = 0

	def getDelta(self):
		self.delta = time.time() - self.start_time
		return self.delta

	def run(self):
		#self.random_worker.start()
		if self.__metadata.global_options['pre_generate_data'] and self.iterations_instead_of_duration:
			self.__update_current_min_max_values()
			#print self.name + " is generating queries..."
			self.createQueries()
			#print self.name + " - done Generating Queries."

		#sync to all start at the same time...
		if self.pipe_home != None:
			if self.pipe_home.poll(None):
				self.pipe_home.recv()

		if self.pipe_home != None:
			self.pipe_home.send("ready")

		#sync to all start at the same time...
		if self.pipe_home != None:
			if self.pipe_home.poll(None):
				self.pipe_home.recv()

		if self.__metadata.global_options['min_transaction_size'] == 1 and self.__metadata.global_options['max_transaction_size'] == 1:
			self.transaction_size = 1;
		else:
			self.transaction_size = random.randint(self.__metadata.global_options['min_transaction_size'], self.__metadata.global_options['max_transaction_size'])
			if self.transaction_size != 1:
				self.__execute_query("BEGIN ;")
		
		self.start_time = time.time()

		#self.random_worker.start()
		#self.__update_current_min_max_values()
		while True:

			self.statistics.setDuration(self.getDelta())
			if self.iterations_instead_of_duration:
				if self.statistics.getTotalOperationCount() >= self._duration or self.poison_pill:
					self.statistics.setDuration(self.getDelta())
					break
			else:
				#print self.name + " time left: " + str(self._duration - self.getDelta())
				if self.getDelta() >= self._duration or self.poison_pill:
					self.statistics.setDuration(self.delta)
					break

			if self.__table[0] == '*':
				#table_name = random.choice(self.__metadata.what_to_do.keys())
				table_name = self.__metadata.chooseATable()
			else:
				table_name = random.choice(self.__table)

			if table_name not in self.tables_in_transaction:
				self.tables_in_transaction.append(table_name)

			todo = self.__metadata.findSomethingToDo(table_name)
			if todo == None:
				continue
			#sleep some if need be
			if self.__metadata.what_to_do[table_name]['SLEEP']['MAX'] > 0:
				time.sleep(random.uniform(self.__metadata.what_to_do[table_name]['SLEEP']['MIN'] / 1000000.0, self.__metadata.what_to_do[table_name]['SLEEP']['MAX'] / 1000000.0)) 
			
			if self.update_min_max_counter % 10000 == 0:
				self.__update_current_min_max_values()

			if len(self.generated_queries[table_name][todo]) > 0:
				#print "using a generated query that was saved."
				sql = self.generated_queries[table_name][todo].pop(0)
			else:
				#print "Generating query on the fly for table " + table_name + " ..."
				sql = self.generateQuery(table_name,todo)
				#print "done making Query..."

			if sql != None:
				self.__execute_query(sql)
			else:
				continue
			
			if self.transaction_size > 1 and (self.statistics.current_transaction_count >= self.transaction_size  or len(self.tables_in_transaction) >= self.__metadata.global_options['max_tables_in_a_transaction']):
				self.tables_in_transaction = []
				operation = 'COMMIT ;'
				if self.__metadata.global_options['rollback_chance'] != 0 and random.randint(1,self.__metadata.global_options['rollback_chance']) == 1:
					operation = 'ROLLBACK ;'
				#else:
					#print "LET's COMMIT IT!!!!!!!!!!!!!!!!!!!!!! self.savepoint_counter:" + str(self.savepoint_counter) + "  self.transaction_size:" + str(self.transaction_size) + "  self.statistics.current_transaction_count:" + str(self.statistics.current_transaction_count)
				self.__execute_query(operation)
				self.savepoint_list = []
				self.savepoint_counter = 0
				self.transaction_size =  random.randint(self.__metadata.global_options['min_transaction_size'],self.__metadata.global_options['max_transaction_size'])
				if self.transaction_size > 1:
					self.__execute_query("BEGIN ;")
			elif self.transaction_size == 1:
				self.transaction_size = random.randint(self.__metadata.global_options['min_transaction_size'],self.__metadata.global_options['max_transaction_size'])
			elif self.transaction_size > 1 and self.statistics.current_transaction_count < self.transaction_size and self.statistics.current_transaction_count > 0:
				if self.__metadata.global_options.get('create_savepoint_chance') != None and self.__metadata.global_options.get('create_savepoint_chance') != 0 and random.randint(1,self.__metadata.global_options.get('create_savepoint_chance')) == 1:
					self.savepoint_counter += 1
					self.savepoint_list.append(self.savepoint_counter)
					#print str(self.name) + " - lets do a savepoint?  self.savepoint_counter:" + str(self.savepoint_counter) + "  self.transaction_size:" + str(self.transaction_size) + "  self.statistics.current_transaction_count:" + str(self.statistics.current_transaction_count)
					self.__execute_query("SAVEPOINT " + "savepoint" + str(self.savepoint_counter))
				elif self.__metadata.global_options.get('release_savepoint_chance') != None and self.__metadata.global_options.get('release_savepoint_chance') != 0 and random.randint(1,self.__metadata.global_options.get('release_savepoint_chance')) == 1 and len(self.savepoint_list) > 0:
					savepointitem = random.choice(self.savepoint_list)
					self.__execute_query("RELEASE SAVEPOINT savepoint" + str(savepointitem))		
					del self.savepoint_list[self.savepoint_list.index(savepointitem):]
				elif self.__metadata.global_options.get('rollback_to_savepoint_chance') != None and self.__metadata.global_options.get('rollback_to_savepoint_chance') != 0 and random.randint(1,self.__metadata.global_options.get('rollback_to_savepoint_chance')) == 1 and len(self.savepoint_list) > 0:
					savepointitem = random.choice(self.savepoint_list)
					self.__execute_query("ROLLBACK TO SAVEPOINT savepoint" + str(savepointitem))
					#print "let's rollback to an existing savepoint...  " + str(savepointitem) + "  self.savepoint_counter:" + str(self.savepoint_counter) + "  self.transaction_size:" + str(self.transaction_size) + "  self.statistics.current_transaction_count:" + str(self.statistics.current_transaction_count)
					del self.savepoint_list[self.savepoint_list.index(savepointitem):]

				elif self.__metadata.global_options.get('commit_early_chance') != None and self.__metadata.global_options.get('commit_early_chance') != 0 and random.randint(1,self.__metadata.global_options.get('commit_early_chance')) == 1:
					self.__execute_query("COMMIT ;")
					#print "LET's COMMIT Early self.savepoint_counter:" + "  self.transaction_size:" + str(self.transaction_size) + "  self.statistics.current_transaction_count:" + str(self.statistics.current_transaction_count)
					self.savepoint_list = []
					self.savepoint_counter = 0
					self.transaction_size =  random.randint(self.__metadata.global_options['min_transaction_size'],self.__metadata.global_options['max_transaction_size'])
					if self.transaction_size > 1:
						self.__execute_query("BEGIN ;")
				elif self.__metadata.global_options.get('rollback_early_chance') != None and self.__metadata.global_options.get('rollback_early_chance') != 0 and random.randint(1,self.__metadata.global_options.get('rollback_early_chance')) == 1:
					self.__execute_query("ROLLBACK ;")
					#print "LET's ROLLBACK Early self.savepoint_counter:" + "  self.transaction_size:" + str(self.transaction_size) + "  self.statistics.current_transaction_count:" + str(self.statistics.current_transaction_count)
					self.savepoint_list = []
					self.savepoint_counter = 0
					self.transaction_size =  random.randint(self.__metadata.global_options['min_transaction_size'],self.__metadata.global_options['max_transaction_size'])
					if self.transaction_size > 1:
						self.__execute_query("BEGIN ;")

			if self.pipe_home != None:
				if self.pipe_home.poll():
					if self.pipe_home.recv() == "gimme stats":		
						self.pipe_home.send(self.statistics)
		
		#self.random_worker.poisonpill = True
		#self.random_worker.join()

		if self.statistics.current_transaction_count >= self.transaction_size and self.transaction_size > 1:
			self.__execute_query("COMMIT ;")

		for k,data in enumerate(self.statistics.slowest_queries):
			if data['sql'].split(' ', 1)[0] == 'SELECT':

				try:
					self.__cur.execute('explain ' + data['sql'])
				
					name_to_index = dict((d[0], i) for i, d in enumerate(self.__cur.description))
					slow_query_info = self.__cur.fetchall()
					explain_list = []
					for row in slow_query_info:
						explain_info = {}
						for name,the_index in name_to_index.iteritems():
							explain_info[name] = slow_query_info[0][the_index]
						explain_list.append(explain_info)
					data['explain'] = explain_list
					self.statistics.slowest_queries[k] = data
				except:
					print self.name + " - failed to get explain"
			else:
				data['explain'] = []
				self.statistics.slowest_queries[k] = data
		if self.pipe_home != None:
			self.pipe_home.send(self.statistics)
			self.__db.close()

	def is_number(self, s):
		st=str(s)
		st=st.strip()
		st=st.lower()
		if st in ('inf','nan'):
			return False
		if str(s).isdigit():
			return True
		try:
			float(s)
			return not all(c in hexdigits for c in str(s))
		except ValueError:
			return False

	def generateQuery(self, table_name, type):
		if type == 'INSERT' or type == 'REPLACE':
			q = self.generateInsertQuery(table_name)
			if q != None:
				if type == 'INSERT':
					if self.__metadata.global_options.get('insert_ignore_chance') != None and self.__metadata.global_options.get('insert_ignore_chance') != 0 and random.randint(1,self.__metadata.global_options.get('insert_ignore_chance')) == 1:
						return type + " IGNORE " + q
					elif self.__metadata.global_options.get('insert_on_duplicate_key_update_chance') != None and self.__metadata.global_options.get('insert_on_duplicate_key_update_chance') != 0 and random.randint(1,self.__metadata.global_options.get('insert_on_duplicate_key_update_chance')) == 1:
						retval = type + " " + q + " ON DUPLICATE KEY UPDATE "
						for value in self.__metadata.meta_data[table_name]:
							if value['col_key'] not in ('PRI','UNI'):
								retval += "`" + value['column_name'] + "` = VALUES(`" + value['column_name'] + "`),"
						return retval[:-1]
					else:
						return type + " " + q
				else:
					return type + " " + q
			return q
		elif type == 'SELECT':
			return self.generateSelectQuery(table_name)
		elif type == 'UPDATE':
			return self.generateUpdateQuery(table_name)
		elif type == 'DELETE':
			return self.generateDeleteQuery(table_name)
		else:
			return None

	def generateInsertQuery(self, table_name):
		raw_data_array = []
		for value in self.__metadata.meta_data[table_name]:
			type = value['datatype']
			#if self.__metadata.fixed_data.has_key(table_name + "." + value['column_name']):
			if value['method'] == "random" and self.__metadata.fixed_data.has_key(table_name + "." + value['column_name']):
				if len(self.__metadata.fixed_data[table_name + "." + value['column_name']]) == 0:
					raw_data_array.append(self.__generateRandomValue(table_name, value['column_name']))
				else:
					if type == 'set':
						list_of_items = random.sample(self.__metadata.fixed_data[table_name + "." + value['column_name']],random.randint(0,len(self.__metadata.fixed_data[table_name + "." + value['column_name']])))
						raw_data_array.append(','.join([str(i) for i in list_of_items]))
					else:	
						d = random.choice(self.__metadata.fixed_data[table_name + "." + value['column_name']])
						if d == '*':
							raw_data_array.append(self.__generateRandomValue(table_name, value['column_name']))
						else:
							raw_data_array.append(d)
			else:
				if value['method'] == "autoinc" and type in self.__metadata.date_types:
					offset = self.auto_increment_offset + (self.num_inserts * self.auto_increment_increment)
					if type == 'date':
						raw_data_array.append(self.__generateRandomValue(table_name, value['column_name']))
						#raw_data_array.append((datetime.strptime('1900-01-01',self.__metadata.date_formats[type]) + timedelta(days=offset)).strftime(self.__metadata.date_formats[type]))
					elif type == 'datetime' or type == 'timestamp':
						raw_data_array.append((datetime.strptime(value['min'],self.__metadata.date_formats[type]) + timedelta(seconds=offset, microseconds=offset)).strftime(self.__metadata.date_formats[type]))
					else:
						raw_data_array.append(self.__generateRandomValue(table_name, value['column_name']))
				elif value['method'] == "autoinc":
					raw_data_array.append(self.auto_increment_offset + (self.num_inserts * self.auto_increment_increment))
				elif value['method'] == "ignore":
					continue
				else:
					raw_data_array.append(self.__generateRandomValue(table_name, value['column_name']))
		if self.__insert_sql[table_name] != self.__begin_sql_insert[table_name]: 
			self.__insert_sql[table_name] += ",";
		self.__insert_sql[table_name] += "("
		key = 0
		for data in raw_data_array:
			data = str(data)
			if key != 0:
				self.__insert_sql[table_name] += ","
			if len(data) > 2 and (data[0] == 'x' or data[0] == 'b') and data[1] == "'":
				self.__insert_sql[table_name] += data
			elif self.is_number(data):
				self.__insert_sql[table_name] += str(data)
			else:
				self.__insert_sql[table_name] += "'" + data + "'"
			key += 1
		self.__insert_sql[table_name] += ")"

		self.num_inserts += 1

		if self.currentInsertQuerySize(table_name) >= self.__metadata.global_options['extended_insert_size'] or ((self.iterations_instead_of_duration == True and self._duration - self.currentInsertQuerySize(table_name) < self.__metadata.global_options['extended_insert_size']) and self.currentInsertQuerySize(table_name) != 0):
			q = " " + self.__insert_sql[table_name]
			self.__insert_sql[table_name] = self.__begin_sql_insert[table_name]
			return q

		return None

	def currentInsertQuerySize(self,table):
		return self.__insert_sql[table].count(')') - 1

	def generateDeleteQuery(self, table_name):
		where = self.makeRandomWhereClause([(table_name,table_name)])
		if where != " ":
			return "DELETE FROM " + table_name + " " + where + " LIMIT " + str(self.__metadata.global_options['delete_limit_size'])
		else:
			return None

	def generateUpdateQuery(self, table_name):
		# find foreign key relationships, if we have some, randomly select some or none and fold it into the where clause.
		the_joins = []
		the_tables = []
		the_alias_tables = []
		self.findForeignKeyRelationships(table_name,the_joins,the_alias_tables,the_tables)
		# randomly slice some off
		the_joins = the_joins[random.randint(0,len(the_joins)):]
		the_tables.append(table_name)
		the_alias_tables.append(table_name)

		colums_to_update = random.sample(self.__metadata.meta_data[table_name],random.randint(2,len(self.__metadata.meta_data[table_name])))
		
		for i, v in enumerate(colums_to_update):
			if not self.__metadata.global_options['update_primary_key_columns'] and self.__metadata.columnInPrimary(table_name,v['column_name']):
				colums_to_update.pop(i)
			elif v['column_name'] != 'column1':
				colums_to_update.pop(i)
		random.shuffle(colums_to_update)
		if len(colums_to_update) == 0:
			#print "in generateUpdateQuery colums_to_update was 0"
			return None
		raw_data_array = []
		for value in colums_to_update:
			raw_data_array.append(self.__generateRandomValue(table_name,value['column_name']))
		where_clause_array = self.makeRandomWhereClause(zip(the_tables,the_alias_tables),True)
		if len(where_clause_array) == 0:
			#print "in generateUpdateQuery where_clause_array was 0"
			return None
		update_sql = "UPDATE " + table_name + " SET ";

		for key, value in enumerate(colums_to_update):
			if key != 0:
				update_sql += ","
			update_sql += "`" + table_name + "`." + value['column_name'] + "="
			if len(str(raw_data_array[key])) > 2 and (str(raw_data_array[key])[0] == 'x' or str(raw_data_array[key])[0] == 'b') and str(raw_data_array[key])[1] == "'":
				update_sql += raw_data_array[key]
			elif self.is_number(raw_data_array[key]):
				update_sql += str(raw_data_array[key])					
			else:
				update_sql += "'" + str(raw_data_array[key]) + "'"
		update_sql += " WHERE " + ' AND '.join([str(i) for i in where_clause_array]) 
		return update_sql

	def generateSelectQuery(self, table_name):
		the_joins = [];
		the_tables = [];
		the_alias_tables = [];
		self.findForeignKeyRelationships(table_name,the_joins,the_alias_tables,the_tables)


		if len(the_joins) > 1:
			#the_joins = the_joins[:random.randint(1,len(the_joins))]
			xx = random.randint(1,len(the_joins))
			the_tables = the_tables[:xx]
			the_joins = the_joins[:xx]
			the_alias_tables = the_alias_tables[:xx]

		#randomly pic some columns
		the_tables.append(table_name)
		the_alias_tables.append(table_name)
		colums_to_select = []
		colums_data_type = []
		for k, t in enumerate(the_alias_tables):
			for v in random.sample(self.__metadata.meta_data[the_tables[k]],random.randint(1,len(self.__metadata.meta_data[the_tables[k]]))):
				if v['datatype'] == 'binary':
					colums_to_select.append("HEX(" + t + "." + v['column_name'] + ")")
				else:
					colums_to_select.append(t + "." + v['column_name'])
				colums_data_type.append(v['datatype'])
		
		combined = list(zip(colums_to_select, colums_data_type))
		random.shuffle(combined)
		colums_to_select[:], colums_data_type[:] = zip(*combined)
		#random.shuffle(colums_to_select)
		where = self.makeRandomWhereClause(zip(the_tables,the_alias_tables))
		if where == " ":
			#print " gen select. where was > <"
			return None
		# decide if we want add a group by section.
		# if so we need to add aggregatefunctions for most of the colums_to_select. then group by the ones we didnt?
		new_select_group_by = []
		group_by = []
		do_group_by = False 
		if self.__metadata.global_options['select_group_by_chance'] != 0 and random.randint(1,self.__metadata.global_options['select_group_by_chance']) == 1:
			do_group_by = True
			for i,col in enumerate(colums_to_select):
				if i == 0:
					new_select_group_by.append(col)
					group_by.append(col)
				elif random.randint(0,1) == 0:
					new_select_group_by.append(col)
					group_by.append(col)
				else:
					if colums_data_type[i] in self.__metadata.float_types or colums_data_type[i] in self.__metadata.int_types:
						if len(new_select_group_by) < 5:
							new_select_group_by.append(random.choice(self.aggregate_functions) + "(" + col + ")")
				if len(group_by) > 0:
					break
			if len(group_by) == 0:
				#print " gen select. group_by was 0"
				return None

		#do_join = False
		#if self.__metadata.global_options['select_order_by_chance'] != 0 and random.randint(1,self.__metadata.global_options['select_order_by_chance']) == 1:
		#	do_join = True
		if do_group_by:
			sql = "SELECT " + ','.join([str(i) for i in new_select_group_by]) + " FROM " + table_name + " " + ' '.join([str(i) for i in the_joins]) + where
		else:
			sql = "SELECT " + ','.join([str(i) for i in colums_to_select]) + " FROM " + table_name + " " + ' '.join([str(i) for i in the_joins]) + where

		random.shuffle(colums_to_select)

		if do_group_by:
			sql += " GROUP BY " + ','.join([str(i) for i in group_by])

		if self.__metadata.global_options['select_order_by_chance'] != 0 and random.randint(1,self.__metadata.global_options['select_order_by_chance']) == 1:
			if do_group_by:
				sql += " ORDER BY " + ','.join([str(i) for i in new_select_group_by])
			else:
				sql += " ORDER BY " + ','.join([str(i) for i in colums_to_select])
		if self.__metadata.global_options['select_limit_size'] != 0:
			sql += " LIMIT " + str(self.__metadata.global_options['select_limit_size'])
		if self.__metadata.global_options['select_lock_in_share_mode_chance'] != 0 and random.randint(1,self.__metadata.global_options['select_lock_in_share_mode_chance']) == 1 and self.transaction_size > 1:
			sql += " LOCK IN SHARE MODE ";
		return sql

	def makeRandomWhereClause(self, tables, return_list = False):
		"""lets build a random where clause that uses a random index from a given table"""
		#if len(self.__metadata.index_info[tables[0]]) == 0:
		#	return " "
		statistics_processing = True
		if self.__metadata.global_options['pre_generate_data'] and self.iterations_instead_of_duration:
			statistics_processing = False

		temp = {}
		for t1, t2 in tables:
			if t1 not in temp.keys():
				temp[t1] = (t1,t2)
		tables = temp.values()
		where_clause_list = []
	
		from_list = []

		for table_name in tables:
			potential_columns = []
			indexes = self.__metadata.index_info[table_name[0]].keys()
			if len(indexes) == 0:
				return " "
			random.shuffle(indexes)
			indexes = indexes[:random.randint(1,len(indexes))]
			for index_name in indexes:
				for col_name in self.__metadata.index_info[table_name[0]][index_name]:
					potential_columns.append(col_name)
			#random.shuffle(potential_columns)
			from_list.append('`' + table_name[0] + '`')

			for col in potential_columns:
				k = 0
				while True:
					sql_grab_random_value = "SELECT " + table_name[0] + "." + col + " FROM " + ','.join([str(i) for i in from_list]) + " WHERE " + " AND ".join([str(i) for i in where_clause_list])
					sql_grab_random_value = sql_grab_random_value.rstrip('WHERE ')

					data = self.__generateRandomValue(table_name[0],col)
					where_or_and = " AND "
					if len(where_clause_list) == 0: 
						where_or_and = " WHERE "
					gt_or_lt = " >= "
					order_by = " LIMIT 1"
					#if k % 4 == 0:
					#if k > 8:
					#	gt_or_lt = " <= "
					#	order_by = " ORDER BY `" + col + "` DESC LIMIT 1"
					#	self.where_clause_gen_trying_desc += 1
					formated_data = str(data)
					
					if self.__metadata.getColumnDataType(table_name[0],col) in self.__metadata.text_types:
						formated_data = "'" + str(data) + "'"
					elif not(len(str(data)) > 2 and (str(data)[0] == 'x' or str(data)[0] == 'b') and str(data)[1] == "'") and not self.is_number(data):
						formated_data = "'" + str(data) + "'"

					sql_grab_random_value += where_or_and + table_name[0] + "." + col + gt_or_lt + formated_data + order_by;
					#print "running this query to get a value: " + sql_grab_random_value
					row,timeout_occured = self.__execute_query(sql_grab_random_value,statistics_processing)	
					if timeout_occured:
						return " "
					if row != None and row[0] != None:
						#print "it got a value: " + str(row[0])
						k = 0
						break
					else: 
						self.where_clause_gen_having_trouble += 1
						#print self.name + " - no value... table: " + table_name[0] + " column: " + col
						#print sql_grab_random_value 
						k += 1
						if k > 8:
							break
				if k > 8:
					#print self.name + " - Having issues finding values for a WHERE clause... table: " + table_name[0] + " column: " + col
					#print sql_grab_random_value
					if len(where_clause_list) > 0:
						#print "where_clause_list size:" + str(len(where_clause_list)) + "   breaking out."
						break
					else:
						#print " Failed to create where clause."
						return " "
				# if the col is a float type. lets do a range

				if self.__metadata.getColumnDataType(table_name[0],col) in self.__metadata.float_types:
					random_float_range = round(random.uniform(1,self.__metadata.global_options['range_max_float']),2)
					where_clause_list.append(table_name[0] + "." + col + " BETWEEN " + str(Decimal(row[0]) - Decimal(random_float_range)) + " AND " + str(Decimal(row[0]) + Decimal(random_float_range)))
				elif self.__metadata.getColumnDataType(table_name[0],col) in self.__metadata.int_types and self.__metadata.global_options['where_clause_range_chance'] != 0 and random.randint(1,self.__metadata.global_options['where_clause_range_chance']) == 1: 
					random_int_range = random.randint(1,self.__metadata.global_options['range_max_int'])
					where_clause_list.append(table_name[0] + "." + col + " BETWEEN " + str(int(row[0]) - random_int_range) + " AND " + str(row[0] + random_int_range))
				elif self.__metadata.getColumnDataType(table_name[0],col) in self.__metadata.date_types and self.__metadata.global_options['where_clause_range_chance'] != 0 and random.randint(1,self.__metadata.global_options['where_clause_range_chance']) == 1:
					if self.__metadata.getColumnDataType(table_name[0],col) == 'date':
						random_days = random.randint(1,self.__metadata.global_options['range_max_date_days'])
						other_date 	= (row[0] + timedelta(days=random_days)).strftime("%Y-%m-%d")
						where_clause_list.append(table_name[0] + "." + col + " BETWEEN " + "'" + self.__db.escape_string(str(row[0])) + "'" + " AND " + "'" + self.__db.escape_string(str(other_date)) + "'")
					elif self.__metadata.getColumnDataType(table_name[0],col) == 'datetime' or self.__metadata.getColumnDataType(table_name[0],col) == 'timestamp':
						random_days = random.randint(1,self.__metadata.global_options['range_max_date_days'])
						other_date 	= (row[0] + timedelta(days=random_days)).strftime("%Y-%m-%d %H:%M:%S")
						where_clause_list.append(table_name[0] + "." + col + " BETWEEN " + "'" + self.__db.escape_string(str(row[0])) + "'" + " AND " + "'" + self.__db.escape_string(str(other_date)) + "'")
					elif self.__metadata.getColumnDataType(table_name[0],col) == 'time':
						random_minutes = random.randint(1,self.__metadata.global_options['range_max_time_minutes'])
						other_date 	= (row[0] + timedelta(minutes=random_minutes))
						where_clause_list.append(table_name[0] + "." + col + " BETWEEN " + "'" + self.__db.escape_string(str(row[0])) + "'" + " AND " + "'" + self.__db.escape_string(str(other_date)) + "'")
					elif self.__metadata.getColumnDataType(table_name[0],col) == 'year':
						other_date 	= row[0] + random.randint(1,self.__metadata.global_options['range_max_year'])
						where_clause_list.append(table_name[0] + "." + col + " BETWEEN " + "'" + self.__db.escape_string(str(row[0])) + "'" + " AND " + "'" + self.__db.escape_string(str(other_date)) + "'")
				else:
					#print "row[0] = " + str(row[0])
					where_clause_list.append(table_name[0] + "." + col + " = '" + self.__db.escape_string(str(row[0])) + "'")

			if self.__metadata.global_options['where_clause_early_termination_chance'] != 0 and random.randint(1,self.__metadata.global_options['where_clause_early_termination_chance']) == 1:
 				break		

		where_clause_list = [w.replace('[br]', '<br />') for w in where_clause_list]
		new_where_clause_list = []
		for item in where_clause_list:
			for t in tables:
				item = item.replace(t[0]+'.',t[1]+'.',1)
			new_where_clause_list.append(item)
		if return_list:
			return new_where_clause_list
		else:
			if len(new_where_clause_list) == 0:
				return " "
			else:
				return " WHERE " + ' AND '.join([str(i) for i in new_where_clause_list])

	def findForeignKeyRelationships(self, table, fk_tables, the_alias_tables, the_tables, ext=""):

		for key, value in enumerate(self.__metadata.meta_data[table]):
			for fks in value['foreign_keys']:
				tt = fks.split('.',1)
				fk_table 	= tt[0]
				fk_col 		= tt[1]
				fk_table_alias = fk_table + str(len(fk_tables))

				fk_tables.append(random.choice(self.join_types) + " JOIN " + fk_table + " " + fk_table_alias + " ON " + fk_table_alias + "." + fk_col + " = " + table + ext + "." + value['column_name'] + " ")
				the_tables.append(fk_table)
				the_alias_tables.append(fk_table_alias)
				if len(the_tables) >= 8:
					return
				else:
					self.findForeignKeyRelationships(fk_table,fk_tables,the_alias_tables,the_tables,str(len(fk_tables) - 1))

	def __generateRandomString(self,length=10):
		#return self.random_worker.grabAstring(length)
		return self.__db.escape_string(''.join(choice(list(digits) + list(letters) + list(printable)) for _ in xrange(length)))
		#return self.__db.escape_string(''.join(choice(list(printable)) for _ in xrange(length)))

	def __getMinMax(self,table,column):
		try:
			sql = "SELECT MIN(" + column + "), MAX(" + column + ") FROM " + table
			self.__cur.execute(sql)
			row = self.__cur.fetchone()
			if row != None:
				if row[0] == None and row[1] == None:
					return self.__metadata.getColumnMinMax(table,column)
				else:
					if self.__metadata.getColumnDataType(table,column) in self.__metadata.int_types:
						return (row[0],row[1])
					elif self.__metadata.getColumnDataType(table,column) in self.__metadata.date_types:
						return (row[0].strftime(self.__metadata.date_formats[self.__metadata.getColumnDataType(table,column)]),row[1].strftime(self.__metadata.date_formats[self.__metadata.getColumnDataType(table,column)]))
			else:
				return self.__metadata.getColumnMinMax(table,column)
		except:
			return self.__metadata.getColumnMinMax(table,column)

	def __update_current_min_max_values(self):
		self.update_min_max_counter += 1
		for table_name in self.potential_tables:
			for i,column_info in enumerate(self.__metadata.meta_data[table_name]):
				#if (column_info['method'] == 'ignore' or column_info['method'] == 'autoinc') and column_info['datatype'] in self.__metadata.int_types:
				if (column_info['datatype'] in self.__metadata.int_types or column_info['datatype'] in self.__metadata.date_types) and self.__metadata.columnInAnIndex(table_name,column_info['column_name']):
					min_max = self.__getMinMax(table_name,column_info['column_name'])
					if column_info['datatype'] in self.__metadata.int_types:
						column_info['current_min'] = min_max[0]
						column_info['current_max'] = min_max[1]
					else:
						column_info['current_min'] = min_max[0]
						column_info['current_max'] = min_max[1]
					self.__metadata.meta_data[table_name][i] = column_info

	def __generateRandomValue(self, table, column):
		for value in self.__metadata.meta_data[table]:
			if value['column_name'] == column:
				break;
		type = value['datatype']
		if type in self.__metadata.int_types:
			if type == 'bit':
				return "b'" + "{0:b}".format(random.randint(value['min'],value['max'])) + "'"
			else:
				if value['method'] == 'ignore' or value['method'] == 'autoinc':
					if 'current_min' in value.keys() and 'current_max' in value.keys() and value['current_min'] != None and value['current_max'] != None:
						#print "current_min:"+str(value['current_min']) + " current_max:"+str(value['current_max'])
						return random.randint(value['current_min'],value['current_max'])
					else:
						return random.randint(value['min'],value['max'])
				else:
					return random.randint(value['min'],value['max'])

		elif type in self.__metadata.float_types:
			return round(random.uniform(value['min'],value['max']),2)
		elif type in self.__metadata.date_types:
			if value.get('current_min') != None:
   				return self.__getRandomDate(datetime.strptime(value['current_min'],self.__metadata.date_formats[type]),
											datetime.strptime(value['current_max'],self.__metadata.date_formats[type]),
											self.__metadata.date_formats[type]) 			
			else:
				return self.__getRandomDate(datetime.strptime(value['min'],self.__metadata.date_formats[type]),
											datetime.strptime(value['max'],self.__metadata.date_formats[type]),
											self.__metadata.date_formats[type])
		elif type in self.__metadata.text_types:
			v = self.__generateRandomString(random.randint(value['min'],value['max']))
			if v.lower() == 'inf' or v.lower() == 'nan':
				return self.__generateRandomString(random.randint(value['min'],value['max']))
			else:
				return v
		else:
			return ""

	def __getRandomDate(self, start, end, format):
		return (start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))).strftime(format)

	def __initialize_things(self):
		self.potential_tables = []
		if self.__table[0] == '*':
			self.potential_tables = self.__metadata.meta_data.keys();
		else:
			self.potential_tables = self.__table

		for table in self.potential_tables:
			self.generated_queries[table] = {}
			for operation in self.__metadata.what_to_do[table]['CRUD'].keys():
				self.generated_queries[table][operation] = []
			
			self.__begin_sql_insert[table] = "INTO `"+table+"` ("
			for value in self.__metadata.meta_data[table]:
				if value['method'] == "ignore":
					continue
				self.__begin_sql_insert[table] += "`" + value['column_name'] + "`"
				self.__begin_sql_insert[table] += ","
			self.__begin_sql_insert[table] = self.__begin_sql_insert[table][:-1]
			self.__begin_sql_insert[table] += ") VALUES "
			self.__insert_sql[table] = self.__begin_sql_insert[table]

	def __execute_query(self, sql, statistics_processing = True):
	#	with warnings.catch_warnings(record=True) as w:
	#		try:
		#print "Start: " + sql
		rows_affected 	= None
		row 			= None
		timout_occured 	= False
		e = None
		w = None
		conn1, conn2 = multiprocessing.Pipe(False)
		subproc = multiprocessing.Process(target=self.do_query, args=(sql, self.__cur, conn2))
		start = time.time()
		subproc.start()
		
		if self.iterations_instead_of_duration:
			timeout = None
		else:
			timeout = self._duration - self.getDelta()
			if timeout < 1.0:
				timeout = 1.0
	
                if conn1.poll(timeout):
                        rows_affected,e,w,row = conn1.recv()
	
		subproc.join(timeout)
		end = time.time()
		execution_time = end - start

		#if conn1.poll():
		#	rows_affected,e,w,row = conn1.recv()
		#else:
		#	print self.name + "nothing in the pipe?"
		#	if subproc.is_alive():
		#		print self.name + "pipe empty bc process is alive and isnt done? "

		if subproc.is_alive():
			#print self.name + " - join timed out(" + str(timeout) + "). Killing this query: " + sql
			timout_occured = True
			subproc.terminate()
			self.connect_to_mysql()
		else:
			if rows_affected != None:
				query_type = sql.split(' ', 1)[0]
				if query_type == 'UPDATE' or query_type == 'DELETE':
					self.update_min_max_counter += 1
				if statistics_processing:
					self.statistics.processQueryInfo(sql,execution_time,self.getDelta(),rows_affected)
				self.log_query_to_disk(sql)
			if e != None:
				self.statistics.processError(e)
				if e[1] == 'MySQL server has gone away':
					#lets try to gracefully kill ourself
					print self.name + " - MySQL server has gone away?  hmm.. Taking Poison Pill..."
					self.poison_pill = True
			if len(w) > 0:
				for t in w:
					self.statistics.processWarning(str(t.message))
				self.statistics.processQueryInfo(sql,execution_time,self.getDelta(),rows_affected)
				self.log_query_to_disk(sql)
				#print "WArning : " + sql
		#print "End: " + sql 
		return (row,timout_occured)



	def log_query_to_disk(self,sql):
		if isTrue(self.write_sql_to_disk):
			with open(self.write_sql_to_file, 'a') as f:
				f.write('/* ' + str(time.time()) + ' */ ' + sql + '\n')

	def do_query(self,sql,cur,conn):
		rows_affected 	= None
		e 				= None
		row 			= None
		w 				= None
		with warnings.catch_warnings(record=True) as w:
			try:
	  			rows_affected = cur.execute(sql)
	  			row = self.__cur.fetchone()
			except MySQLdb.Error, e:
				#self.statistics.processError(e)
				print self.name + " - An Error occured running query. %s" %e
				print sql
				print "----------------------------"
				if e[1] == 'MySQL server has gone away':
					#print self.name + " - An Error occured running query. %s" %e
					pass
				#print sql;
				#conn.send((rows_affected,e,w,row))
			except MySQLdb.ProgrammingError, e:
				print self.name + " - A ProgrammingError occured running query. %s" %e
				#print sql;
				#print "----------------end----------------"
		conn.send((rows_affected,e,w,row))

def isTrue(v):
	if type(v) == types.BooleanType:
		return v
	elif type(v) == types.StringType:
		return v.lower() in ("yes", "true", "t", "1")
	else:
		return False

if __name__ == '__main__':
	
	config_items = {
					'mysql_user':			{'value':None}, \
					'mysql_password':		{'value':None}, \
					'mysql_host':			{'value':None}, \
					'mysql_port':			{'value':None, 'default': 3306}, \
					'mysql_socket':			{'value':None, 'default': None}, \
					'database':				{'value':None}, \
					'duration':				{'value':None}, \
					'write_sql_to_disk':	{'default': False, 'value':None}, \
					'write_sql_to_disk_location':		{'default': '.', 'value':None}, \
					'table':				{'value':None},  \
					'num_clients':			{'default': 1},  \
					'profile':				{'default':"Random"}  \
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

	for k, v in config_items.iteritems():
		print k + ":" + str(v['value'])

	gen = {}
	for n in range(0,int(config_items['num_clients']['value'])):
		gen[n] = QueryGenerator(None,config_items['mysql_user']['value'], config_items['mysql_password']['value'], config_items['mysql_host']['value'], config_items['mysql_port']['value'], config_items['mysql_socket']['value'], config_items['database']['value'], config_items['table']['value'], config_items['duration']['value'],config_items['profile']['value'])
		gen[n].write_sql_to_disk 		= bool(config_items['write_sql_to_disk']['value'])
		gen[n].write_sql_to_disk_location 	= config_items['write_sql_to_disk_location']['value']
		gen[n].auto_increment_increment = int(config_items['num_clients']['value'])
		gen[n].auto_increment_offset 	= n
		gen[n].start()

	print "Waiting for clients to finish..."
	for n in range(0,int(config_items['num_clients']['value'])):
		gen[n].join()
	print "Done."

else:
	pass
	#print "im imported."




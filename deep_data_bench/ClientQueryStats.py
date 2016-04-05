class ClientQueryStats(object):
	
	def __init__(self,client_name):
		self.client_name = client_name
		self.query_stats = {'INSERT' 					: {'count' : 0, 'min_execution_time' : 0.0, 'max_execution_time' : 0.0, 'total_execution_time' : 0.0, 'operations per second' : 0, 'rows_affected' : 0}, 
							'SELECT' 					: {'count' : 0, 'min_execution_time' : 0.0, 'max_execution_time' : 0.0, 'total_execution_time' : 0.0, 'operations per second' : 0, 'rows_affected' : 0}, 
							'UPDATE' 					: {'count' : 0, 'min_execution_time' : 0.0, 'max_execution_time' : 0.0, 'total_execution_time' : 0.0, 'operations per second' : 0, 'rows_affected' : 0}, 
							'DELETE' 					: {'count' : 0, 'min_execution_time' : 0.0, 'max_execution_time' : 0.0, 'total_execution_time' : 0.0, 'operations per second' : 0, 'rows_affected' : 0},  
							'REPLACE' 					: {'count' : 0, 'min_execution_time' : 0.0, 'max_execution_time' : 0.0, 'total_execution_time' : 0.0, 'operations per second' : 0, 'rows_affected' : 0},
							'BEGIN' 					: {'count' : 0, 'min_execution_time' : 0.0, 'max_execution_time' : 0.0, 'total_execution_time' : 0.0, 'operations per second' : 0, 'rows_affected' : 0},
							'COMMIT' 					: {'count' : 0, 'min_execution_time' : 0.0, 'max_execution_time' : 0.0, 'total_execution_time' : 0.0, 'operations per second' : 0, 'rows_affected' : 0},
							'SAVEPOINT' 				: {'count' : 0, 'min_execution_time' : 0.0, 'max_execution_time' : 0.0, 'total_execution_time' : 0.0, 'operations per second' : 0, 'rows_affected' : 0},
							'ROLLBACK TO SAVEPOINT' 	: {'count' : 0, 'min_execution_time' : 0.0, 'max_execution_time' : 0.0, 'total_execution_time' : 0.0, 'operations per second' : 0, 'rows_affected' : 0},
							'ROLLBACK' 					: {'count' : 0, 'min_execution_time' : 0.0, 'max_execution_time' : 0.0, 'total_execution_time' : 0.0, 'operations per second' : 0, 'rows_affected' : 0},
							'RELEASE SAVEPOINT' 		: {'count' : 0, 'min_execution_time' : 0.0, 'max_execution_time' : 0.0, 'total_execution_time' : 0.0, 'operations per second' : 0, 'rows_affected' : 0}}
		self.duration 			= 0.0
		self.mysql_errors 		= {}
		self.mysql_warnings 	= {}
		#self.trend_data 		= {"INSERT" : [], "SELECT" : [], "UPDATE" : [], "DELETE" : [], "REPLACE" : [], "BEGIN" : [], "COMMIT" : [], "ROLLBACK" : []}
 		self.trend_data 		= []
 		self.slowest_queries	= []
 		self.num_transactions 		= 0
 		self.total_transaction_size = 0
 		self.ave_transaction_size 	= 0

 		self.transaction_has_started 	= False
 		self.current_transaction_count 	= 0

 	def getTotalOperationCount(self):
 		total = 0
 		for operation in self.query_stats.keys():
 			total += self.query_stats[operation]['count']
 		return total

	def processError(self, e):
		if e.args[0] in self.mysql_errors.keys():
			self.mysql_errors[e.args[0]]['count'] += 1
		else:
			self.mysql_errors[e.args[0]] = {'message':e.args[1], 'count':1}
			#print e.args[0]

	def processWarning(self, m):
		if m in self.mysql_warnings.keys():
			self.mysql_warnings[m]['count'] += 1
		else:
			self.mysql_warnings[m] = {'message':m, 'count':1}

	def processQueryInfo(self,sql,execution_time,elapsed_time,rows_affected):
		#query_type = sql.split(' ', 1)[0]
		query_type = None
		for query_prefix in self.query_stats.keys():
			if sql.startswith('ROLLBACK TO SAVEPOINT'):
				query_type = 'ROLLBACK TO SAVEPOINT'
				break
			if sql[:len(query_prefix)] == query_prefix:
				query_type = query_prefix
				break

		if query_type == None:
			print "Odd .. Unknown query_type: " 
			return

		if query_type == 'BEGIN':
			if self.current_transaction_count > 0:
				#implied commit.
				self.num_transactions 			+= 1
				self.total_transaction_size 	+= self.current_transaction_count
				self.current_transaction_count 	= 0
			self.transaction_has_started 		= True
		elif query_type == 'COMMIT':
			self.total_transaction_size 	+= self.current_transaction_count
			self.num_transactions 			+= 1
			self.transaction_has_started 	= False
			self.current_transaction_count 	= 0
		elif query_type == 'ROLLBACK':
			self.transaction_has_started 	= False
			self.current_transaction_count 	= 0
		elif self.transaction_has_started:
			self.current_transaction_count += 1
		else:
			self.total_transaction_size += 1
			self.num_transactions += 1


		if query_type == 'INSERT' or query_type == 'REPLACE':
			self.query_stats[query_type]['count'] += sql.count(')') - 1
		else:
			self.query_stats[query_type]['count'] += 1
		
		self.query_stats[query_type]['rows_affected'] += rows_affected

		self.query_stats[query_type]['total_execution_time'] += execution_time
		if self.query_stats[query_type]['min_execution_time'] == 0.0 or execution_time < self.query_stats[query_type]['min_execution_time']:
			self.query_stats[query_type]['min_execution_time'] = execution_time
		#print self.name + "> " + str('%f %d' % math.modf(time.time())) + "---- took(" +  str(end - start) + ")  --  " + sql 
		if execution_time > self.query_stats[query_type]['max_execution_time']:
			self.query_stats[query_type]['max_execution_time'] = execution_time
		self.query_stats[query_type]['operations per second'] = self.query_stats[query_type]['count'] / elapsed_time


		#maintain the slowest 10 queries
		if len(self.slowest_queries) < 50:
			self.slowest_queries.append( {"sql" : sql, "execution_time" : execution_time} )
			self.slowest_queries.sort(key=lambda dic: dic['execution_time'], reverse=True)
		else:
			if self.slowest_queries[-1]['execution_time'] < execution_time:
				self.slowest_queries.pop()
				self.slowest_queries.append( {"sql" : sql, "execution_time" : execution_time} )
				self.slowest_queries.sort(key=lambda dic: dic['execution_time'], reverse=True)
		
		self.trend_data.append((query_type,elapsed_time,execution_time))

	def setDuration(self,duration):
		self.duration = duration
	
	def getDuration(self):
		return self.duration

	def getAveQueryTime(self,key):
		if self.query_stats[key]['count'] != 0:
			return self.query_stats[key]['total_execution_time'] / self.query_stats[key]['count']
		else:
			return 0
 





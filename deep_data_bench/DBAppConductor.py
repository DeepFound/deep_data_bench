#!/usr/bin/env python2.7
from QueryGenerator import QueryGenerator
import time
import multiprocessing
from ClientQueryStats import ClientQueryStats
from PillarReport import PillarReport
import os 

class DBAppConductor(object):
 
	def __init__(self, num_clients, duration, mysql_user, mysql_pass, mysql_host, mysql_port, mysql_socket, database, profile, tables):
		self.num_clients 		= num_clients
		self.duration 			= duration
		self.client_list		= []
		self.client_stats		= []
		self.pipe_list			= []
		self.profile			= profile
		self.stats_frequency 	= 0.0
		self.total_clients = 0
		self.write_sql_to_disk 	= False
		for item in num_clients:
			self.total_clients += int(item)
			
		left_over = 0
		if str(duration)[-1] != 's':
			tduration = int(int(duration) / self.total_clients)
			left_over = int(duration) - (tduration * self.total_clients)
			duration = tduration	
		i = 0
		for j in range(len(num_clients)):
			for n in range(int(self.num_clients[j])):
				self.client_stats.append(ClientQueryStats(str(i)))
				self.pipe_list.append(multiprocessing.Pipe())
				if n == int(self.num_clients[j]) - 1 and left_over > 0:
					self.client_list.append(QueryGenerator(self.pipe_list[i][1], mysql_user, mysql_pass, mysql_host, mysql_port, mysql_socket, database, tables, duration + left_over, profile[j]))
				else:
					self.client_list.append(QueryGenerator(self.pipe_list[i][1], mysql_user, mysql_pass, mysql_host, mysql_port, mysql_socket, database, tables, duration, profile[j]))
				i += 1

	def setShowStatsFrequency(self,x):
		self.stats_frequency = float(x)

	def go(self):

		for n in range(len(self.client_list)):
			self.client_list[n].auto_increment_increment = self.total_clients
			self.client_list[n].auto_increment_offset = n
			self.client_list[n].name = "Client-" + str(n)
			self.client_list[n].write_sql_to_disk = self.write_sql_to_disk

		print "Starting " + str(len(self.client_list)) + " clients..."
		for t in self.client_list:
			t.start()

		#tell them all to really start
		print "Generating Queries..."
		for n in range(len(self.client_list)):
			#time.sleep(0.01)
			self.pipe_list[n][0].send("go")

		
		for n in range(len(self.client_list)):
			if self.pipe_list[n][0].poll(None):
				self.pipe_list[n][0].recv()
		print "Get Ready..."
		for n in range(len(self.client_list)):
			self.pipe_list[n][0].send("go")

		print "Running Queries..."

		if self.stats_frequency > 0:
			i = 0
			while self.client_list[0].is_alive(): 
				report = PillarReport()
				if i % 40 == 0:
					report.printStatColumns()
				time.sleep(self.stats_frequency)
				for n in range(len(self.client_list)):
					self.pipe_list[n][0].send("gimme stats")
				for n in range(len(self.client_list)):
					if self.pipe_list[n][0].poll():
						self.client_stats[n] = self.pipe_list[n][0].recv()

				profile_name = os.path.basename(self.profile[0]).split('.')[0]
				report.feedClientStats(profile_name,self.client_stats)
				report.printProfileStats(profile_name)
				report = None
				i += 1

		for n in range(len(self.pipe_list)):
			if self.pipe_list[n][0].poll(None):
				self.client_stats[n] = self.pipe_list[n][0].recv()

		print "Waiting for Clients to Finish..."
		for t in self.client_list:
			t.join()


	def getClientStats(self):
		return self.client_stats

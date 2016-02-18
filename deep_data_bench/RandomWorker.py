import threading 
import random
from random import choice
from string import ascii_lowercase
import time

class RandomWorker(threading.Thread): 
	def __init__(self): 
		threading.Thread.__init__(self) 
		self.strings = []
		self.poisonpill = False
		while len(self.strings) < 10000:
			self.strings.insert(0,self.__generateRandomString(20))

	def __generateRandomString(self,length=10):
		return ''.join(choice(list(ascii_lowercase)) for _ in xrange(length))

	def grabAstring(self,l=10):
		if len(self.strings) > 0:
			return self.strings.pop()[l:]
		else:
			return self.__generateRandomString(l)

	def run(self): 
		while not self.poisonpill:
			if len(self.strings) < 10000:
				self.strings.insert(0,self.__generateRandomString(20))
			#else:
			#	time.sleep(1)


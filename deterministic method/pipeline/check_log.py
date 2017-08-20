from time import sleep
from datetime import datetime
import time
import os

while 1:
	#ctime = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
	ctime = datetime.utcnow()
	with open("rules_log.log", "r") as f:
			for i, each in enumerate(f):
				log_time = datetime.strptime(each.split("task")[0].strip(), "%Y-%m-%d %H:%M:%S")
				diff = (log_time - ctime).total_seconds()
				# print(diff)
				if diff < 108000 and diff > -108000:
					print(each.split("task")[1].strip())

	time.sleep(30)
	clean = lambda: os.system('cls')
	clean()





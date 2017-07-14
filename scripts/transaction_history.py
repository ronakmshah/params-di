#!/usr/bin/env python

import copy
from elasticsearch import Elasticsearch, helpers
import random
import time
import yaml

CONFIG_DICT = {}
Branches_customers = [{'Mountain-View': 
							['Mike', 'Milan', 'Marie', 'Mia', 
				 			'Melissa', 'Megan', 'Mark', 'Mason']
					  },
					  {'San-Jose': 
					  		['Sandra', 'Sam', 'Scott', 'Sean',
					  		'Seth', 'Simon', 'Sara']
					  },
            		  {'Redwood-City': ['Ron', 'Ram', 'Rod', 'Riya', 'Ramya'] },
            		  {'San-Francisco': ['Sherwin', 'Shea', 'Shawn', 'Sharvil', 'Shashi'] },
            		  {'Palo-Alto': ['Peter', 'Patrick', 'Pia', 'Paul', 'Paige', 'Pam']},
            		  {'Santa-Clara': ['Suhas', 'Suhani', 'Sun', 'Sumit', 'Sunidhi', 'Sudhir']},
            		  {'Saratoga': ['Sia', 'Sita', 'Siam', 'Siri', 'Sibeal']},
            		  {'Cupertino': ['Claire', 'Cynthia', 'Catherine', 'Charles', 'Cody', 'Cameron']},
            		  {'Los-Gatos':['Lee', 'Leo', 'Logan', 'Laila', 'Lila', 'Lisa', 'Laura']},
            		  {'Los-Altos': ['Lucas', 'Levi', 'Liam', 'Landon', 'Lucy', 'Leslie', 'Lorraine']}
          			  ]
Transaction_type = ['Withdrawal', 'Deposit']

def insertTransactData():
	es_data = {}
	for branch_customer in Branches_customers:
			es_data['branch'] = branch_customer.keys()[0]
			customer_list = branch_customer.values()[0]
			for customer in customer_list:
				es_data['customer'] = customer
				# Always write it in specific index in specific doc_type
				es_data['_index'] = "demo_transaction"
				es_data['_type'] = "demo_type"
				writeToES(es_data)


def writeToES(es_data):
	es = Elasticsearch()
	write_data = []
	startTime = int(time.time()) * 1000 - (30 * 24 * 60 * 60 * 1000)
	endTime = int(time.time()) * 1000
	times = random.randint(1,15)
	balance = random.randint(5000, 10000)
	for i in range(times):
		es_data['balance'] = balance
		es_data['transaction_type'] = random.sample(Transaction_type, 1)[0]
		es_data['amount'] = random.randint(100,500)
		es_data['timestamp'] = random.randint(startTime, endTime)
		write_data.append(copy.deepcopy(es_data))
		if es_data['transaction_type'] == "Withdrawal":
			balance -= es_data['amount']
		else:
			balance += es_data['amount']
		if balance < 0:
			balance = 0
	#es1.index(index="flowindex", doc_type="flow", body=es_data)
	helpers.bulk(es, iter(write_data), request_timeout=50)

def configRead():
	global CONFIG_DICT
	with open("wifi.yml", "r") as fileread:
		try:
			CONFIG_DICT = yaml.load(fileread)
		except yaml.YAMLError as exc:
			print(exc)

if __name__ == "__main__":
	insertTransactData()

from sqlalchemy import create_engine
import csv
import os
import sys
SQLALCHEMY_DATABASE_URI = "oracle://alexgre:alex1988@temp1.clx2hx01phun.us-east-1.rds.amazonaws.com/ORCL"
engine = create_engine(SQLALCHEMY_DATABASE_URI)
title = ['ENTERPRISEID','LAST_','FIRST_','MIDDLE','SUFFIX_','DOB','GENDER','SSN','ADDRESS1','ADDRESS2', 
			'ZIP','MOTHERS_MAIDEN_NAME','MRN','CITY','STATE_','PHONE','PHONE2','EMAIL','ALIAS_']

def sql(rule):
	with engine.begin() as conn:
		res = conn.execute(rule)
	for each in res:
		print(each)
	return res

def get_rules():
	d = dict()
	with open('rules.txt', 'r') as f:
		for line in f:
			rule = line[:-1].split("@")
			rule_title = rule[0]
			rule_sql = rule[1]
			d[rule_title] = rule_sql 
	return d

def rule2csv(file, data):
	with open(file, "w") as f:
		writer = csv.writer(f)


def main(): 
	rules = get_rules()
	for each in rules:
		sql(rules[each])
			
if __name__ == '__main__':
	main()
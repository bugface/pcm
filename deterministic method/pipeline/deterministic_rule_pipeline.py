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



def main(): 
	# with open('temp\\1.txt', 'w') as f:
	# 	print("hello", file = f)

if __name__ == '__main__':
	main()
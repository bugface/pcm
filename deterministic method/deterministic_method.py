from sqlalchemy import create_engine
import cx_Oracle
import csv
import os
import sys
SQLALCHEMY_DATABASE_URI = "oracle://alexgre:alex1988@temp1.clx2hx01phun.us-east-1.rds.amazonaws.com/ORCL"
engine = create_engine(SQLALCHEMY_DATABASE_URI)
title = ['ENTERPRISEID',	'LAST_',	'FIRST_',	'MIDDLE',	'SUFFIX_',	'DOB',	'GENDER',	'SSN',	'ADDRESS1',	'ADDRESS2',	'ZIP',	'MOTHERS_MAIDEN_NAME',	'MRN',	'CITY',	'STATE_',	'PHONE',	'PHONE2',	'EMAIL',	'ALIAS_']

def find_lfgd_records(sql, file):
	# sql = '''
	# 		select p1.ENTERPRISEID, p2.ENTERPRISEID from pcm p1, pcm p2 where
	# 		p1.ENTERPRISEID <> p2.ENTERPRISEID and
	# 		p1.FIRST_ = p2.FIRST_ and
	# 		p1.LAST_ = p2.LAST_ and
	# 		p1.DOB = p2.DOB and
	# 		p1.ssn = p2.ssn
	# '''

	# sql = '''
	# 		select p1.ENTERPRISEID, p2.ENTERPRISEID from pcm p1, pcm p2 where
	# 		p1.ENTERPRISEID <> p2.ENTERPRISEID and
	# 		p1.FIRST_ = p2.FIRST_ and
	# 		p1.LAST_ = p2.LAST_ and
	# 		p1.DOB = p2.DOB and
	# 		p1.gender = p2.gender
	# '''

	# sql = '''
	# 		select p1.ENTERPRISEID, p2.ENTERPRISEID from pcm p1, pcm p2 where
	# 		p1.ENTERPRISEID <> p2.ENTERPRISEID and
	# 		p1.FIRST_ = p2.FIRST_ and
	# 		p1.LAST_ = p2.LAST_ and
	# 		p1.gender = p2.gender and
	# 		p1.ssn = p2.ssn
	# '''

	# sql = '''
	# 		select p1.ENTERPRISEID, p2.ENTERPRISEID from pcm p1, pcm p2 where
	# 		p1.ENTERPRISEID <> p2.ENTERPRISEID and
	# 		p1.FIRST_ = p2.FIRST_ and
	# 		p1.LAST_ = p2.LAST_ and
	# 		p1.ADDRESS1 = p2.ADDRESS1
	# '''

	# sql = '''
	# 		select p1.ENTERPRISEID, p2.ENTERPRISEID from pcm p1, pcm p2
	# 		where p1.ENTERPRISEID <> p2.ENTERPRISEID and
	# 		p1.FIRST_ = p2.FIRST_ and
	# 		p1.LAST_ = p2.LAST_
	# '''

	with engine.begin() as conn:
		res = conn.execute(sql)
		with open(file, "w", newline='\n') as f:
			s = set()
			for row in res:
				if row[0] not in s:
					s.add(row[1])
					data = "{}\t{}".format(row[0], row[1])
					print(data, file=f)
		s = set()
		i = 0
		for row in res2:
			if row[0] not in s:
				s.add(row[1])
				i += 1
		print(i)

def find_exact_same_records():
	sql = '''
			select *
			from pcm p1, pcm p2
			where p1.dob = p2.dob
			and p1.FIRST_ = p2.first_
			and p1.ENTERPRISEID <> p2.ENTERPRISEID
			and p1.GENDER = p2.GENDER
			and p1.ssn = p2.ssn
			and p1.LAST_ = p2.LAST_
	'''
	with engine.begin() as conn:
		res = conn.execute(sql)
		for row in res:
			if row[7] != row[26]:
				output(row, "enterpriseID_pair.txt")

def check_overlap(file1, file2, meld):
	# file1 must have more data then file2
	fs1 = os.stat(file1).st_size
	fs2 = os.stat(file2).st_size
	if fs1 < fs2:
		print("switch two files! file1 must be the larger file.")
		sys.exit(1)

	d1 = dict()
	dd1 = dict()
	d2 = dict()
	dd2 = dict()
	d3 = dict()
	d4 = dict()

	with open(file1, "r") as f1, open(file2, "r") as f2:
		for each in f1:
			data1 = each[:-1].split('\t')
			key1 = int(data1[0])
			val1 = int(data1[1])
			if key1 in d1:
				dd1[key1] = val1
			else:
				d1[key1] = val1
			if val1 in d2:
				dd2[val1] = key1
			else:
				d2[val1] = key1
		for each in f2:
			data2 = each[:-1].split('\t')
			key1 = int(data2[0])
			val1 = int(data2[1])
			d3[key1] = val1

		for each in d3:
			if each in d1:
				if d3[each] == d1[each]:
					continue
			elif each in dd1:
				if d3[each] == dd1[each]:
					continue
			elif each in d2:
				if d3[each] == d2[each]:
					continue
			elif each in dd2:
				if d3[each] == dd2[each]:
					continue
			d4[each] = d3[each]

	if meld:
		file_name = ".txt"
		with open(file_name, "w") as f:
			for each in d1:
				line = "{}\t{}".format(each, d1[each])
				print(line, file=f)
			for each in dd1:
				line = "{}\t{}".format(each, dd1[each])
				print(line, file=f)
			for each in d4:
				line = "{}\t{}".format(each, d4[each])
				print(line, file=f)

def create_submission_csv(file, csv_file):
	with open(file, "r") as f:
		with open(csv_file, "w", newline='') as f1:
			writer = csv.writer(f1)
			for each in f:
				l = []
				data = each[:-1].split('\t')
				l.append(data[0])
				l.append(data[1])
				l.append(1)
				writer.writerow(l)

def main():
	#find_exact_same_records()
	#find_lfgd_records()
	#no_dob.txt, no_gender.txt, no_ssn.txt
	#check_overlap("no_ssn.txt", "no_gender.txt", True)
	#check_overlap("com_ssn_gender_dob.txt", "only_address.txt", True)
	#create_submission_csv('com_ssn_gender_dob_addr.txt', 'sub4.csv')
	#check_overlap("no_gender_no_dob.txt", "fldsg.txt", True)
	#create_submission_csv("no_gender_no_dob.txt","sub5.csv")
	pass


if __name__ == '__main__':
	main()
from sqlalchemy import create_engine
import csv
import logging
import glob
import os
from names_normalization import main as name_norm_main
from multiprocessing import Process
#from multiprocessing import pool

SQLALCHEMY_DATABASE_URI = "oracle://alexgre:alex1988@temp1.clx2hx01phun.us-east-1.rds.amazonaws.com/ORCL"
FORMAT = '%(asctime)-20s %(name)-5s %(levelname)-10s %(message)s'
logging.basicConfig(filename='rules.log',level=logging.INFO, format=FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("task")
title = ['ENTERPRISEID','LAST_','FIRST_','MIDDLE','SUFFIX_','DOB','GENDER','SSN','ADDRESS1','ADDRESS2', 'ZIP','MOTHERS_MAIDEN_NAME','MRN','CITY','STATE_','PHONE','PHONE2','EMAIL','ALIAS_']
engine = create_engine(SQLALCHEMY_DATABASE_URI, pool_size=4, pool_recycle=3600)

def create_submission_csv(txt_file, csv_file):
	with open(txt_file, "r") as f:
		with open(csv_file, "w", newline='') as f1:
			writer = csv.writer(f1)
			for each in f:
				l = []
				data = each[:-1].split('\t')
				l.append(data[0])
				l.append(data[1])
				l.append(1)
				writer.writerow(l)

def execute_sql(rule):

	with engine.begin() as conn:
		res = conn.execute(rule)
	return res

def get_rules(file):
	d = dict()
	with open(file, 'r') as f:
		for line in f:
			rule = line[:-1].split("@")
			rule_title = rule[0]
			rule_sql = rule[1]
			d[rule_title] = rule_sql
	return d

def pair2txt(file, data):
	with open(file, "w") as f:
		for each in data:
			output = "{}\t{}".format(each[0], each[1])
			print(output, file=f, end='\n')

def store_result_as_pairs(rules, folder, job):
	for rule in rules:
		file = folder + "\\" + rule + ".txt"
		res = execute_sql(rules[rule])
		data = []
		s = set()

		if job == "p":
			for each_res in res:
				t = (each_res[0], each_res[1])
				tprim = (each_res[1], each_res[0])
				if t not in s and tprim not in s:
					s.add(t)
					data.append(t)
		elif job == "d":
			for each_res in res:
				t = (each_res[7], each_res[26])
				tprim = (each_res[7], each_res[26])
				if t not in s and tprim not in s:
					s.add(t)
					data.append(t)

		pair2txt(file, data)
		logger.info("rule: {}; pairs number: {}".format(file, len(data)))
		res.close()

def _dedupe(file, dataset):
	with open(file, "r") as f:
		for each in f:
			data = each[:-1].split('\t')
			t = (data[0], data[1])
			tprim = (data[1], data[0])
			if t not in dataset and tprim not in dataset:
				dataset.add(t)

def combine_pair_files_with_dedupe(base_file, new_files, output_file):
	pairs = set()
	_dedupe(base_file, pairs)

	for file in new_files:
		_dedupe(file, pairs)

	with open(output_file, "w") as f:
		for each in pairs:
			output = "{}\t{}".format(each[0], each[1])
			print(output, file = f, end='\n')

def get_extra_pairs_not_in_base(base, files):
	pairs = set()
	diff_pairs = set()
	_dedupe(base, pairs)

	for file in files:
		with open(file, "r") as f:
			for each in f:
				data = each[:-1].split('\t')
				t = (data[0], data[1])
				tprim = (data[1], data[0])
				if t not in pairs and tprim not in pairs:
					pairs.add(t)
					diff_pairs.add(t)
	return diff_pairs

def pairs2csv(pairs, output_file):
	title.insert(0, "index")
	#engine = create_engine(SQLALCHEMY_DATABASE_URI, pool_size=4, pool_recycle=3600)
	with open(output_file, "w", newline='') as f:
		writer = csv.writer(f)
		writer.writerow(title)
		title.pop(0)
		for i, pair in enumerate(pairs):
			l1 = [i]
			l2 = [i]
			pair1 = int(pair[0])
			pair2 = int(pair[1])
			sql1 = "select * from pcm where enterpriseid = {}".format(pair1)
			sql2 = "select * from pcm where enterpriseid = {}".format(pair2)
			with engine.begin() as conn:
				res1 = conn.execute(sql1).fetchone()
				for each in title:
					l1.append(res1[each.lower()])
				writer.writerow(l1)
				res2 = conn.execute(sql2).fetchone()
				for each in title:
					l2.append(res2[each.lower()])
				writer.writerow(l2)

def pairs2txt(data, file):
	with open(file, "w") as f:
		for each in data:
			output = "{}\t{}".format(each[0], each[1])
			print(output, file=f, end='\n')

def pipline_get_detail(rule_file, folder, base_file, output_csv_file, output_pair_file, job):
	# rules = get_rules(rule_file)
	# store_result_as_pairs(rules, folder, job)
	new_pair_files = glob.glob(folder + "\\" + "*.txt")
	#combine_pair_files_with_dedupe(base_file, new_pair_files, output_pair_file)
	extra_pairs = get_extra_pairs_not_in_base(base_file, new_pair_files)
	print(len(extra_pairs))
	pairs2txt(extra_pairs, output_pair_file)
	#change pairs2csv to multiprocessing
	pairs2csv(extra_pairs, output_csv_file)
	#extra_pairs = list(extra_pairs)

def main():
	#work 1 config input:
	# base_file = "stgy7.txt"
	# rule_file = "rules_detail_process_address.txt"
	# folder = "txt\\3_fields_process_address"
	# output_csv_file = "addr_to_process.csv"
	# output_pair_file = "addr_to_process.txt"
	# job = "d" #("d" = detail, "p" = only pairs)
	# #run pipline
	# if not os.path.exists(folder):
	# 	os.makedirs(folder)
	# pipline_get_detail(rule_file, folder, base_file, output_csv_file, output_pair_file, job)

	#work 2 config input:
	# input_file = "addr_to_process_final_pairs2.txt"
	# output_file = "addr_to_process_final_pairs2.csv"
	# pairs = []
	# with open(input_file, "r") as f:
	# 	for each in f:
	# 		#print(each[:-1])
	# 		t = each[:-1].split('\t')
	# 		#print(t)
	# 		t1 = t[0]
	# 		t2 = t[1]
	# 		ts = (int(t1), int(t2))
	# 		pairs.append(ts)
	# pairs2csv(pairs, output_file)

	#work 3
	'''
	condfig:
	output query file names for name normalization:
		process_first_name.csv
		process_last_name.csv
	output name normalization file names:
		process_first_name.txt
		process_last_name.txt
	'''
	#files used in project
	base_file = "stgy7.txt"
	# rule_file_first = "rules_detail_first.txt"
	# rule_file_last = "rules_detail_last.txt"
	# folder1 = "txt\\3_fields_process_name\\first_name"
	# folder2 = "txt\\3_fields_process_name\\last_name"
	# output_pair_file_first = "process_first_name_pairs.txt"
	# output_csv_file_first = "process_first_name1.csv"
	# output_pair_file_last = "process_last_name_pairs.txt"
	# output_csv_file_last = "process_last_name1.csv"
	# job = "d"

	if not os.path.exists(folder1):
		os.makedirs(folder1)
	if not os.path.exists(folder2):
		os.makedirs(folder2)

	#p1 = Process(target=pipline_get_detail, args=(rule_file_first, folder1, base_file, output_csv_file_first, output_pair_file_first, job))
	#p2 = Process(target=pipline_get_detail, args=(rule_file_last, folder2, base_file, output_csv_file_last, output_pair_file_last, job))

	# p1.start()
	# p2.start()

	# p1.join()
	# p2.join()
	#pipline_get_detail(rule_file_first, folder1, base_file, output_csv_file_first, output_pair_file_first, job)

	# t1 = get_extra_pairs_not_in_base(base_file, ["process_first_name_222.txt"])
	# t2 = get_extra_pairs_not_in_base(base_file, ["process_last_name_222.txt"])

	# print(len(t1))
	# print(len(t2))

	# create_submission_csv("process_first_name_222.txt", "sub10.csv")
	# create_submission_csv("process_last_name_222.txt", "sub11.csv")

if __name__ == '__main__':
	main()
from sqlalchemy import create_engine
import csv
import logging
import glob
import os

FORMAT = '%(asctime)-20s %(name)-5s %(levelname)-10s %(message)s'
logging.basicConfig(filename='rules.log',level=logging.INFO, format=FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("task")

SQLALCHEMY_DATABASE_URI = "oracle://alexgre:alex1988@temp1.clx2hx01phun.us-east-1.rds.amazonaws.com/ORCL"
engine = create_engine(SQLALCHEMY_DATABASE_URI)
title = ['ENTERPRISEID','LAST_','FIRST_','MIDDLE','SUFFIX_','DOB','GENDER','SSN','ADDRESS1','ADDRESS2', 'ZIP','MOTHERS_MAIDEN_NAME','MRN','CITY','STATE_','PHONE','PHONE2','EMAIL','ALIAS_']

def clsw():
    os.system('cls' if os.name=='nt' else 'clear')

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

def sql(rule):
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

def store_pairs(rules):
	for rule in rules:
		file = "txt\\" + rule + ".txt"
		res = sql(rules[rule])
		data = []
		s = set()
		for each_res in res:
			t = (each_res[0], each_res[1])
			tprim = (each_res[1], each_res[0])
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

def combine(base, files):
	pairs = set()
	_dedupe(base, pairs)

	for file in files:
		_dedupe(file, pairs)

	with open("combined.txt", "w") as f:
		for each in pairs:
			output = "{}\t{}".format(each[0], each[1])
			print(output, file = f, end='\n')

def diff(base, files):
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

def pairs2csv(pairs, file):
	title.insert(0, "index")
	with open(file, "w", newline='') as f:
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

def check_csv_by_hand(csv_file):
	with open('same_pair_from_diff_csv.txt', "w") as fs, open('diff_pair_from_diff_csv.txt', "w") as fd:
		with open(csv_file, "r") as f:
			reader = csv.reader(f)
			records = []
			for i, row in enumerate(reader):
				if i == 0:
					continue
				records.append(row)
			j = 0
			for i in range(0, len(records), 2):
				l = []
				for each in records[i]:
					l.append(each)
				for each in records[i+1]:
					l.append(each)
				if l[8] == l[28] and l[8].strip() != '':
					print(l[8])
					output = "{}\t{}".format(records[i][1], records[i+1][1])
					print(output, file = fs, end='\n')
				else:
					j += 1
					for k in range(len(records[i])):
						if k in [2,3,4,6,7,8,9,10,11,16,17,18,19]:
							print("{}\t\t{}\t\t\t\t\t{}".format(title[k-1], l[k], l[k+20]))
					decision = input("same or not (y/n):")
					if decision in ['y', 'Y']:
						output = "{}\t{}".format(records[i][1], records[i+1][1])
						print(output, file = fs, end='\n')

					if decision in ['n', 'N']:
						output = "{}\t{}".format(records[i][1], records[i+1][1])
						print(output, file = fd, end='\n')
						print()
						print()
					if j % 2 == 0:
						j = 0
						clsw()

def main():
	# rules = get_rules('rules_pair.txt')
	# store_pairs(rules)
	# txt_files = glob.glob("txt\*.txt")
	# base_file = "union_stgy4.txt"
	# combine(base_file, txt_files)
	# create_submission_csv('combined.txt', 'sub7.csv')
	# dp_set = diff(base_file, txt_files)
	# pairs2txt(dp_set, 'diffpairs.txt')
	# create_submission_csv('diffpairs.txt', 'sub8.csv')
	# pairs2csv(dp_set, "rulesdiff.csv")
	check_csv_by_hand("rulesdiff.csv")

if __name__ == '__main__':
	main()
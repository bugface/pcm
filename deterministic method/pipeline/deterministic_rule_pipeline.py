from sqlalchemy import create_engine
import csv
import logging
import glob
import os

SQLALCHEMY_DATABASE_URI = "oracle://alexgre:alex1988@temp1.clx2hx01phun.us-east-1.rds.amazonaws.com/ORCL"

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

def store_result_as_pairs(rules, folder):
	for rule in rules:
		file = folder + "\\" + rule + ".txt"
		res = execute_sql(rules[rule])
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

def pipline_get_detail(rule_file, folder, base_file, output_csv_file):
	rules = get_rules(rule_file)
	store_result_as_pairs(rules, folder)
	new_pair_files = golb.glob(folder + "\\" + "*.txt")
	#combine_pair_files_with_dedupe(base_file, new_pair_files, output_pair_file)
	extra_pairs = get_extra_pairs_not_in_base()
	pairs2csv(extra_pairs, output_csv_file)
	print(output_csv_file)

def main():
	#work 1 config input:
	base_file = "stgy7.txt"
	rule_file = "rules_detail_process_address.txt"
	folder = "txt\\3_fields_process_address"
	output_csv_file = "addr_to_process.csv"

	#run pipline
	if not os.path.exists(folder):
		os.makedirs(folder)
	pipline_get_detail(rule_file, folder, base_file, output_csv_file)


if __name__ == '__main__':
	FORMAT = '%(asctime)-20s %(name)-5s %(levelname)-10s %(message)s'
	logging.basicConfig(filename='rules.log',level=logging.INFO, format=FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
	logger = logging.getLogger("task")
	engine = create_engine(SQLALCHEMY_DATABASE_URI)
	title = ['ENTERPRISEID','LAST_','FIRST_','MIDDLE','SUFFIX_','DOB','GENDER','SSN','ADDRESS1','ADDRESS2', 'ZIP','MOTHERS_MAIDEN_NAME','MRN','CITY','STATE_','PHONE','PHONE2','EMAIL','ALIAS_']
	main()
from sqlalchemy import create_engine
import datetime
import csv
from deterministic_rule_pipeline import create_submission_csv
import logging

# helper_title = ['ZIP', 'FIRST_', 'CITY', 'DOB', 'ADDRESS2', 'ADDRESS1', 'GENDER', 'ENTERPRISEID',
# 				'MIDDLE', 'LAST_', 'SUFFIX_', 'MOTHERS_MADIDEN_NAME', 'MRN', 'STATE_', 'PHONE',
# 				'PHONE2', 'EMAIL', 'ALIAS_', 'SSN']
title = ['ENTERPRISEID','LAST_','FIRST_','MIDDLE','SUFFIX_','DOB','GENDER','SSN','ADDRESS1','ADDRESS2', 'ZIP','MOTHERS_MADIDEN_NAME','MRN','CITY','STATE_','PHONE','PHONE2','EMAIL','ALIAS_']

# SQLALCHEMY_DATABASE_URI = "oracle://alexgre:alex1988@temp1.clx2hx01phun.us-east-1.rds.amazonaws.com/ORCL"
# engine = create_engine(SQLALCHEMY_DATABASE_URI, pool_size=4, pool_recycle=3600)

FORMAT = '%(asctime)-20s %(name)-5s %(levelname)-10s %(message)s'
logging.basicConfig(filename='process_dob.log',level=logging.INFO, format=FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("process_dob")

def pre_processing_dob_string(s):
	return dict(zip(['year', 'month', 'day'], s.split(" ")[0].split("-")))

def check_dob(d1, d2):
	if d1 == "" or d2 == "" or d1 == None or d2 == None:
		return False
	else:
		result = False
		dd1 = pre_processing_dob_string(d1)
		# print("1  ", dd1)
		dd2 = pre_processing_dob_string(d2)
		#print("2   ", dd2)

		if dd1['year'] == dd2['year'] and dd1['month'] == dd2['month'] and dd1['day'] == dd2['day']:
			result = True
		elif dd1['day'] == dd2['day'] and dd1['month'] == dd2['month']:
			result = True
		elif dd1['year'] == dd2['year'] and dd1['month'] == dd2['month']:
			result = True
		elif dd1['month'] == dd2['month'] and dd1['day'] == dd2['day']:
			result = True
		elif dd1['day'] == dd2['month'] and dd1['month'] == dd2['day']:
			result = True

		return result

def preprocess_results(res):
	organized_res = []
	for each in title:
		organized_res.append(res[each])
	return organized_res

def process_dob_from_csv(csv_file):
	matched_results = set()
	matched_results_all = []

	with open(csv_file, "r") as f:
		reader = csv.DictReader(f)
		fir_dob = ""
		fir_id = ""
		fir = None
		for i, each in enumerate(reader):
			if each['DOB'] == None:
				each['DOB'] = ""
			if i % 2 == 0:
				fir_dob += each['DOB']
				fir_id += each['ENTERPRISEID']
				fir = preprocess_results(each)
			if i % 2 == 1:
				sec_dob = each['DOB']
				sec_id = each['ENTERPRISEID']
				#print(sec_id)
				sec = preprocess_results(each)

				result = False
				try:
					result = check_dob(fir_dob, sec_dob)
				except Exception as e:
					print(e)
					logger.error(e)

				if result:
					matched_results.add((fir_id, sec_id))
					matched_results_all.append(fir)
					matched_results_all.append(sec)

				fir_dob = ""
				fir_id = ""
				fir = None
	return matched_results, matched_results_all


def output_result_pair(result_pairs, output_file):
	with open(output_file, "w") as f:
		for each in result_pairs:
			output_data = "{}\t{}".format(each[0], each[1])
			print(output_data, file=f, end='\n')

def output_result_csv(results, output_file):
	with open(output_file, "w", newline='') as f:
		writer = csv.writer(f)
		writer.writerow(title)
		for result in results:
			writer.writerow(result)

def main():
	input_csv = "process_dob1.csv"
	output_txt = "processed_dob.txt"
	output_csv = "processed_dob.csv"
	sub = "sub12.csv"

	res_set, res_all_list = process_dob_from_csv(input_csv)
	output_result_pair(res_set, output_txt)
	output_result_csv(res_all_list, output_csv)

	create_submission_csv(output_txt, sub)

if __name__ == '__main__':
	main()
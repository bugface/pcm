from sqlalchemy import create_engine
import datetime
import csv

# SQLALCHEMY_DATABASE_URI = "oracle://alexgre:alex1988@temp1.clx2hx01phun.us-east-1.rds.amazonaws.com/ORCL"
# engine = create_engine(SQLALCHEMY_DATABASE_URI, pool_size=4, pool_recycle=3600)

def pre_processing_dob_string(s):
	return dict(zip(['year', 'month', 'day'], s.split(" ")[0].split("-")))

def check_dob(d1, d2):
	if d1 == "" or d2 == "":
		return False
	else:
		result = False
		dd1 = pre_processing_dob_string(d1)
		dd2 = pre_processing_dob_string(d2)

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

def process_dob_from_csv(csv_file):
	matched_results = set()

	with open(csv_file, "r") as f:
		reader = csv.DictReader(f)
		fir_dob = ""
		fir_id = ""
		for i, each in enumerate(f):
			if i % 2 == 0:
				fir_dob += each['DOB']
				fir_id += each['ENTERPRISEID']
			if i % 2 == 1:
				sec_dob = each['DOB']
				sec_id = each['ENTERPRISEID']

				result = check_dob(fir_dob, sec_dob)

				if result:
					matched_results.append((fir_id, sec_id))

				fir_dob = ""
				fir_id = ""

	return matched_results

def output_result(result_pairs, output_file):
	with open(output_file, "w") as f:
		for each in result_pairs:
			output_data = "{}\t{}".format(each[0], each[1])
			print(output_data, file=f, end='\n')

def main():
	input_csv = "process_dob.csv"
	output_txt = "processed_dob.txt"

	res_set = process_dob_from_csv(input_csv)
	output_result(res_set, output_txt)


if __name__ == '__main__':
	main()
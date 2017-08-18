import jellyfish
import csv
from deterministic_rule_pipeline import create_submission_csv, pairs2csv
# from multiprocessing import Process, Pool, cpu_count
# import concurrent.futures
# import functools
# SQLALCHEMY_DATABASE_URI = "oracle://alexgre:alex1988@temp1.clx2hx01phun.us-east-1.rds.amazonaws.com/ORCL"
# engine = create_engine(SQLALCHEMY_DATABASE_URI, pool_size=4, pool_recycle=3600)

def measure_ssn_similarity(ssn1, ssn2, sign):
	if ssn1 == "" or ssn2 == "" or ssn1 is None or ssn2 is None:
		return 0

	r1 = jellyfish.jaro_winkler(ssn1, ssn2)
	r2 = 1 - jellyfish.hamming_distance(ssn1, ssn2)/len(ssn1)

	if sign == "t":
		print("jw-{} vs hd-{}".format(r1, r2))
	elif sign == "w":
		return max(r1, r2)

def test():
	l = [('853-21-7390', '653-21-7390'), ('853-21-7390', '828-21-7390'), ('853-51-7390', '853-21-7390'),
		('853-51-7390', '853-21-7380'), ('853-51-7390', '853-51-4391')]

	for each in l:
		measure_ssn_similarity(each[0], each[1], "t")

def process_ssn_from_csv(csv_file, threshold, output_file, sign):
	matched_results = []

	with open(csv_file, "r") as f:
		reader = csv.DictReader(f)
		fir_id = ""
		fir_ssn = ""

		for i, each in enumerate(reader):
			if i % 2 == 0:
				fir_id += each["ENTERPRISEID"]
				fir_ssn += each["SSN"]
			else:
				sec_id = each["ENTERPRISEID"]
				sec_ssn = each["SSN"]

				if measure_ssn_similarity(fir_ssn, sec_ssn, sign) > threshold:
					matched_results.append((fir_id, sec_id))

				fir_id = ""
				fir_ssn = ""

	return matched_results

def output_ssn_similarity_result(data, output_file):
	with open(output_file, "w") as f:
		for each in data:
			output_data = "{}\t{}".format(each[0], each[1])
			print(output_data, file=f, end='\n')

def main():
	threshold = 0.9
	input_file = "process_ssn.csv"
	output_file = "normalized_ssn_pairs.txt"
	output_sub = "sub13.csv"
	sign = "w"

	res = process_ssn_from_csv(input_file, threshold, output_file, sign)
	output_ssn_similarity_result(res, output_file)

	pairs2csv(res, "check_ssn.csv")
	create_submission_csv(output_file, output_sub)

if __name__ == '__main__':
	main()
	#test()
	#pass
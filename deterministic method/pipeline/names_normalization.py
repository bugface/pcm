import jellyfish
import csv
from multiprocessing import Process

def measure_string_distance(s1, s2, method):
	'''
		Four methods will be used with method code from 1 to 4
		Two methods focused on string similarity and the other two will be focused on phonetic encoding
		Method code to method name:
		1. jaro-winkler distance
		2. damerau-levenshtein distance
		3. Metaphone
		4. NYSIIS
		5. match_rating_codex

		note:
			for methods 4,5 and 6, they only can provide results as 1 (match) or 0 (not match)
			for methods 1 and 2, the methods will return a value in range [0, 1]
	'''
	result = 0

	if method == 1:
		result = jellyfish.jaro_winkler(s1, s2)
	elif method == 2:
		try:
			diff = jellyfish.damerau_levenshtein_distance(s1, s2)
			result = 1 - (diff / max(len(s1), len(s2)))
		except:
			result = 0
	elif method == 3:
		result = 1 if jellyfish.metaphone(s1) == jellyfish.metaphone(s2) else 0
	elif method == 4:
		result = 1 if jellyfish.nysiis(s1) == jellyfish.nysiis(s2) else 0
	elif method == 5:
		result = 1 if jellyfish.match_rating_codex(s1) == jellyfish.match_rating_codex(s2) else 0
	# elif method == 0:
	# 	raise ValueError("provide a method code (1-6).")
	# else:
	# 	raise ValueError("the method parameter must be in the range from 1 to 6.")

	return result


def output_result(result_pairs, output_file):
	with open(output_file, "w") as f:
		for each in result_pairs:
			output_data = "{}\t{}".format(each[0], each[1])
			print(output_data, file=f, end='\n')

def process_names_from_csv(csv_file, name_option, threshold, output_file):
	'''
		Three name options are needed:
		1. only first name
		2. only last name

		threshold (0-1) needed to be assign to indicate at which level we believe two names are the same
	'''
	field = ""
	if name_option == 1:
		field += "FIRST_" #must match the column name
	elif name_option == 2:
		field += "LAST_" #must match the column name
	else:
		raise ValueError("the name operation parameter must be 1 or 2.")

	matched_results = []

	with open(csv_file, "r") as f:
		file_reader = csv.DictReader(f)
		fir_name = ""
		fir_id = ""
		for i, each_row in enumerate(file_reader):
			if i % 2 == 0:
				fir_name += each_row[field]
				fir_id += each_row["ENTERPRISEID"]
			if i % 2 == 1:
				sec_name = each_row[field]
				sec_id = each_row["ENTERPRISEID"]
				sim_max = 0
				for method in range(1, 6):
					similarity = measure_string_distance(fir_name, sec_name, method)
					sim_max = max(similarity, sim_max)

				if sim_max > threshold:
					matched_results.append((fir_id, sec_id)) # append as a tuple

				fir_name = ""
				fir_id = ""
				print("{}\t{}".format(name_option, len(matched_results)))

	output_result(matched_results, output_file)

def main():
	'''
	comfig:
		all tasks use threshold 0.8

		task 1:
		csv file = process_first_name.csv
		method = 1

		task 2:
		csv file = process_last_name.csv
		method = 2
	'''
	#task args tuple list
	threshold = 0.8
	tasks_args = [("process_first_name.csv", 1, threshold, "process_first_name_222.txt"), ("process_last_name.csv", 2, threshold, "process_last_name_222.txt")]

	ps = []
	for each_args in tasks_args:
		#print(each_args)
		ps.append(Process(target=process_names_from_csv, args=each_args))

	for p in ps:
		p.start()

	for p in ps:
		p.join()

	print("done")

if __name__ == '__main__':
	main()

import csv
from deterministic_rule_pipeline import create_submission_csv

def check_dupe(file1, file2, file3):
	s = set()
	l = []
	with open(file1, "r") as f:
		for each in f:
			t = each[:-1].split("\t")
			t1 = t[0]
			t2 = t[1]
			s.add((t1, t2))

	with open(file2, "r") as f:
		count = 0
		for each in f:
			t = each[:-1].split("\t")
			t1 = t[0]
			t2 = t[1]

			tp1 = (t1, t2)
			tp2 = (t2, t1)
			if tp1 in s or tp2 in s:
				count += 1
			else:
				l.append(tp1)
		print(count)

	with open(file3, "w") as f:
		for each in l:
			print("{}\t{}".format(each[0], each[1]), file=f, end='\n')

def csv2txt(csv_file, txt_file):
	with open(txt_file, "w") as f1:
		with open(csv_file, "r") as f:
			reader = csv.reader(f)
			for each in reader:
				line = "{}\t{}".format(each[0], each[1])
				print(line, file=f1, end='\n')

def main():
	#csv2txt("submission2.csv", "submission2.txt")
	#check_dupe("stgy7_process_first_last_address_dob_ssn_combined.txt", "submission2.txt", "der_vs_pro.txt")
	# create_submission_csv("der_vs_pro.txt", "sub14.csv")
	pass

if __name__ == '__main__':
	main()

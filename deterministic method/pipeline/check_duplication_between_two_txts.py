import os
from deterministic_rule_pipeline import create_submission_csv

def check():
	f1 = None
	f2 = None
	# file1 = "55437.txt"
	# file2 = "processed_alternative.txt"

	file1 = "process_alter_last.txt"
	file2 = "56068.txt"

	if os.path.getsize(file1) > os.path.getsize(file2):
		f1 = file1
		f2 = file2
	else:
		f1 = file2
		f2 = file1

	base = set()
	i = 0

	with open(file1, "r") as f:
		for each in f:
			p = each[:-1].split("\t")
			t = (p[0], p[1])
			base.add(t)
			i += 1

	print(len(base))
	print(i)

	if i > len(base):
		print(file1 + " has duplicated records.")
	k = 0
	with open(file2, "r") as f:
		for each in f:
			p = each[:-1].split("\t")
			t = (p[0], p[1])
			tprim = (p[1], p[0])
			if t not in base and tprim not in base:
				#print("ok")
				pass
			else:
				k += 1

	create_submission_csv(file1, "sub23_raw.csv")
	print("dupe num: {}".format(k))

def cal_len():
	file1 = "processed_alternative1.txt"
	file2 = "processed_alternative.txt"

	files = [file1, file2]

	for each in files:
		s = set()
		with open(each, "r") as f:
			for each in f:
				s.add(each)
		print(len(s))


def main():
	check()
	#cal_len()


if __name__ == '__main__':
	main()
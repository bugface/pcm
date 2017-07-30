s = set()
l = []
with open("stgy7.txt", "r") as f:
	for each in f:
		t = each[:-1].split("\t")
		t1 = t[0]
		t2 = t[1]
		s.add((t1, t2))

with open("addr_to_process_final_pairs.txt", "r") as f:
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

with open("addr_to_process_final_pairs2.txt", "w") as f:
	for each in l:
		print("{}\t{}".format(each[0], each[1]), file=f, end='\n')
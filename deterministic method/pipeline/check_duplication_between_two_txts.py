import os

f1 = None
f2 = None
file1 = ""
file2 = ""

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

with open(file2, "r") as f:
	for each in f:
		p = each[:-1].split("\t")
		t = (p[0], p[1])
		tprim = (p[1], p[0])
		if t not in base and tprim not in base:
			#print("ok")
			pass
		else:
			print(t, " duplicated.")

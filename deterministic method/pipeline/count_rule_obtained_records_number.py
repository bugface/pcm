from deterministic_rule_pipeline_new_version import execute_sql

rules = "rules_detail_3_field_missing.txt"
with open(rules, "r") as f:
	for each in f:
		content = each[:-1].split("@")
		rule = content[0]
		condition = content[1].split("where")[1]
		sql = "select count(*) from pcm p1, pcm p2 where {}".format(condition)
		ress = execute_sql(sql)
		for res in ress:
			print(rule)
			# print(sql)
			print(res[0])
			#print()
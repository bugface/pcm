# f_dob_addr_phone@select * from pcm p1, pcm p2 where
# p1.ENTERPRISEID <> p2.ENTERPRISEID and p1.address1 = p2.address1 and p1.dob = p2.dob and p1.first_ = p2.first_ and p1.phone = p2.phone
#MOTHERS_MADIDEN_NAME


def convert_rules(file):
    rl = []
    with open("newrules_detail_full_cover.txt", "r") as f:
        for each in f:
            rl.append(each[:-1].split("@")[0])

    output_file = "new2_" + file
    with open(output_file, "w") as fw:
        with open(file, "r") as fr:
            for line in fr:
                line = line[:-1]
                query = line.split("@")[1].split("where")
                select = "select * from pmac p1, pmac p2 where"
                condition = query[1].split("and")
                if 'p1.dob' in query[1]:
                    num = 3
                else:
                    num = 1
                for i in range(num):
                    new_condition = [condition[0]]
                    new_rule = []
                    for each in condition[1:]:
                        field = each.split("=")[0].split(".")[1].strip()
                        if field == "address1":
                            nc = each.replace("address1", "address")
                            new_condition.append(nc)
                            new_rule.append("address")
                        elif field == "dob":
                            nc = None
                            if i == 0:
                                #year, month
                                nc = " p1.year = p2.year and p1.month = p2.month "
                                new_rule.append("year_month")
                            elif i == 1:
                                # month day
                                nc = " p1.day = p2.day and p1.month = p2.month "
                                new_rule.append("day_month")
                            elif i == 2:
                                # year day
                                nc = " p1.day = p2.day and p1.year = p2.year "
                                new_rule.append("day_year")
                            new_condition.append(nc)
                        elif field == "gender":
                            new_condition.append(" p1.MOTHERS_MAIDEN_NAME = p2.MOTHERS_MAIDEN_NAME ")
                            new_rule.append("mmn")
                        else:
                            new_condition.append(each)
                            new_rule.append(field)
                    # recombine to sql
                    new_sql = " ".join([select, "and".join(new_condition)])
                    new_rule_detail = "@".join(["_".join(new_rule), new_sql])
                    if "_".join(new_rule) not in rl:
                        print(new_rule_detail, file=fw, end='\n')


def main():
    file = "rules_detail_full_cover.txt"
    convert_rules(file)


if __name__ == '__main__':
    main()

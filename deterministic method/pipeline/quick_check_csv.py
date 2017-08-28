import csv
from ssn_normalization import measure_ssn_similarity
from process_dob import check_dob
from process_address_with_normalization import normalize_address
from deterministic_rule_pipeline import pairs2csv, pairs2txt, create_submission_csv

def filter_check():
    csv_file = "processed_alter_last_diff.csv"

    with open(csv_file, "r") as f:
        reader = csv.DictReader(f)

        matched_results = set()
        not_matched = []

        f_ssn = ""
        f_dob = ""
        f_id = 0
        f_addr = ""
        f_phone = ""
        f_f = ""
        f_l = ""
        f_mrn = ""
        f_int_mrn = 0
        for i, each in enumerate(reader):
            if each['DOB'] is None:
                each['DOB'] = ""
            if i % 2 == 0:
                f_ssn += each['SSN']
                f_dob += each['DOB']
                f_id += int(each['ENTERPRISEID'])
                f_addr += normalize_address(each['ADDRESS1'])
                f_phone += each['PHONE']
                f_f += each['FIRST_']
                f_l += each['LAST_']
                f_mrn += each['MRN']
                if f_mrn != "":
                    f_int_mrn += int(each['MRN'])
                #print(f_addr)
            else:
                s_ssn = each['SSN']
                s_dob = each['DOB']
                s_id = int(each['ENTERPRISEID'])
                s_addr = normalize_address(each['ADDRESS1'])
                s_phone = each['PHONE']
                s_f = each['FIRST_']
                s_l = each['LAST_']
                s_mrn = each['MRN']
                if s_mrn != "":
                    s_int_mrn = int(each['MRN'])
                else:
                    s_int_mrn = -1000
                #print(s_addr)
                # print(f_ssn + " : " + s_ssn)

                # if measure_ssn_similarity(f_ssn, s_ssn, "w") >= 0.98:
                #     matched_results.add((f_id, s_id))

                # elif check_dob(f_dob, s_dob):
                #     matched_results.add((f_id, s_id))

                # elif f_addr != "" and f_addr == s_addr:
                #     matched_results.add((f_id, s_id))

                # elif f_phone == s_phone and f_phone != "":
                #     matched_results.add((f_id, s_id))

                # elif measure_ssn_similarity(f_mrn, s_mrn, "w") >= 0.9:
                #     matched_results.add((f_id, s_id))

                # elif abs(f_int_mrn - s_int_mrn) < 100:
                #     matched_results.add((f_id, s_id))

                # else:
                #     not_matched.append((f_id, s_id))

                matched_results.add((f_id, s_id))

                f_int_mrn = 0
                f_ssn = ""
                f_id = 0
                f_dob = ""
                f_addr = ""
                f_phone = ""
                f_f = ""
                f_l = ""
                f_mrn = ""

    print(len(matched_results))
    # for each in matched_results:
    #     print(each)
    # l = set()
    # with open("processed_alter_last_diff.txt", 'r') as f:
    #     for each in f:
    #         p = each[:-1].split("\t")
    #         t = (int(p[0]), int(p[1]))
    #         l.add(t)
    # f = []
    # for each in l:
    #     f.append(each[0])
    #     f.append(each[1])
    # for each in matched_results:
    #     s1 = each[0]
    #     s2 = each[1]
    #     if s1 in f:
    #         f.remove(s1)
    #     if s2 in f:
    #         f.remove(s2)
    # print(f)

    #pairs2txt(matched_results ,"alter_last_check_pairs.txt")
    #create_submission_csv("altern_last_check_pairs.txt", "sub21b.csv")
    #pairs2csv(matched_results, "alternaitve_check_pairs_1.csv")


def exclude_check():
    csv_file = "alternaitve_check_pairs_1.csv"
    matched_results = set()
    ssn_pair = []

    with open(csv_file, "r") as f:
        reader = csv.DictReader(f)

        f_ssn = ""
        f_dob = ""
        f_id = ""
        f_addr = ""
        f_phone = ""
        f_f = ""
        f_l = ""
        f_mrn = ""
        for i, each in enumerate(reader):
            if each['DOB'] is None:
                each['DOB'] = ""
            if i % 2 == 0:
                f_ssn += each['SSN']
                f_dob += each['DOB']
                f_id += each['ENTERPRISEID']
                f_addr += normalize_address(each['ADDRESS1'])
                f_phone += each['PHONE']
                f_f += each['FIRST_']
                f_l += each['LAST_']
                f_mrn += each['MRN']
                #print(f_addr)
            else:
                s_ssn = each['SSN']
                s_dob = each['DOB']
                s_id = each['ENTERPRISEID']
                s_addr = normalize_address(each['ADDRESS1'])
                s_phone = each['PHONE']
                s_f = each['FIRST_']
                s_l = each['LAST_']
                s_mrn = each['MRN']

                if measure_ssn_similarity(f_ssn, s_ssn, "w") < 0.95 and f_ssn != "" and s_ssn != "":
                    matched_results.add((f_id, s_id))
                    ssn_pair.append((f_ssn, s_ssn))

                # if check_dob(f_dob, s_dob):
                #     matched_results.add((f_id, s_id))

                # if f_addr != "" and f_addr == s_addr:
                #     matched_results.add((f_id, s_id))

                # if f_phone == s_phone and f_phone != "":
                #     matched_results.add((f_id, s_id))

                # if measure_ssn_similarity(f_mrn, s_mrn, "w") > 0.81:
                #     matched_results.add((f_id, s_id))

                f_ssn = ""
                f_id = ""
                f_dob = ""
                f_addr = ""
                f_phone = ""
                f_f = ""
                f_l = ""
                f_mrn = ""

    print(len(matched_results))
    # pairs2txt(matched_results ,"alternaitve_check_pairs_2.txt")
    # create_submission_csv("alternaitve_check_pairs_2.txt", "sub21d.csv")
    # pairs2csv(matched_results, "alternaitve_check_pairs_2.csv")
    # for each in matched_results:
    #     print(each)
    # for each in ssn_pair:
    #     print(each)

def merge_two_txt():
    f1 = "alternaitve_check_pairs_1.txt"
    f2 = "alternaitve_check_pairs_2.txt"
    ff = "alternaitve_check_pairs.txt"

    results = set()

    k = 0

    with open(f1, "r") as f:
        for each in f:
            t = each[:-1].split('\t')
            results.add((t[0], t[1]))

    with open(f2, "r") as f:
        for each in f:
            t = each[:-1].split('\t')
            tp = (t[0], t[1])
            if tp in results:
                k += 1
                results.remove(tp)

    print(k)
    print(len(results))
    pairs2txt(results , ff)
    pairs2csv(results, "alternaitve_check_pairs_merged.csv")

def main():
    #filter_check()
    #exclude_check()
    #merge_two_txt()
    #create_submission_csv("processed_alter_last_diff_v2.txt", "sub23diffc.csv")

if __name__ == '__main__':
    main()
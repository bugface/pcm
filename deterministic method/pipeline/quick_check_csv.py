import csv
import sys
from ssn_normalization import measure_ssn_similarity
from process_dob import check_dob
from deterministic_rule_pipeline_new_version import pairs2csv, pair2txt, create_submission_csv, extract_pairs_from_txt
from mrn_similarity_meaasurement import measure_mrn_distance, measure_mrn_similarity


def filter_check():
    csv_file = "56588.csv"

    with open(csv_file, "r") as f:
        reader = csv.DictReader(f)
        for i, each in enumerate(reader):
            print(i)
            print(each)

            if i > 8:
                break

        # matched_results = set()
        # not_matched = []

        # f_ssn = ""
        # f_dob = ""
        # f_id = 0
        # f_addr = ""
        # f_phone = ""
        # f_f = ""
        # f_l = ""
        # f_mrn = ""
        # f_int_mrn = 0
        # for i, each in enumerate(reader):
        #     if each['DOB'] is None:
        #         each['DOB'] = ""
        #     if i % 2 == 0:
        #         f_ssn += each['SSN']
        #         f_dob += each['DOB']
        #         f_id += int(each['ENTERPRISEID'])
        #         f_addr += normalize_address(each['ADDRESS1'])
        #         f_phone += each['PHONE']
        #         f_f += each['FIRST_']
        #         f_l += each['LAST_']
        #         f_mrn += each['MRN']
        #         if f_mrn != "":
        #             f_int_mrn += int(each['MRN'])
        #         #print(f_addr)
        #     else:
        #         s_ssn = each['SSN']
        #         s_dob = each['DOB']
        #         s_id = int(each['ENTERPRISEID'])
        #         s_addr = normalize_address(each['ADDRESS1'])
        #         s_phone = each['PHONE']
        #         s_f = each['FIRST_']
        #         s_l = each['LAST_']
        #         s_mrn = each['MRN']

        #         if measure_ssn_similarity(f_ssn, s_ssn, "w") >= 0.90:
        #             matched_results.add((f_id, s_id))

        #         # elif check_dob(f_dob, s_dob):
        #         #     matched_results.add((f_id, s_id))

        #         # elif f_addr != "" and f_addr == s_addr:
        #         #     matched_results.add((f_id, s_id))

        #         # elif f_phone == s_phone and f_phone != "":
        #         #     matched_results.add((f_id, s_id))

        #         # elif measure_mrn_similarity(f_mrn, s_mrn, "w") >= 0.8:
        #         #     matched_results.add((f_id, s_id))

        #         elif measure_mrn_distance(f_mrn, s_mrn) < 200:
        #             matched_results.add((f_id, s_id))

        #         else:
        #             not_matched.append((f_id, s_id))

        #         #matched_results.add((f_id, s_id))

        #         f_int_mrn = 0
        #         f_ssn = ""
        #         f_id = 0
        #         f_dob = ""
        #         f_addr = ""
        #         f_phone = ""
        #         f_f = ""
        #         f_l = ""
        #         f_mrn = ""

    # print(len(matched_results))
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
    csv_file = "56588.csv"
    # do not use set()
    matched_results = list()
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
        i = 0
        for each in reader:
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
                # print(f_addr)
                i += 1
            elif i % 2 == 1:
                s_ssn = each['SSN']
                s_dob = each['DOB']
                s_id = each['ENTERPRISEID']
                s_addr = normalize_address(each['ADDRESS1'])
                s_phone = each['PHONE']
                s_f = each['FIRST_']
                s_l = each['LAST_']
                s_mrn = each['MRN']
                re = measure_mrn_distance(f_mrn, s_mrn)

                if measure_ssn_similarity(f_ssn, s_ssn, "w") < 0.90 and f_ssn != "" and s_ssn != "":
                    matched_results.append((f_id, s_id))
                elif re > 400 and re != sys.maxsize:
                    matched_results.append((f_id, s_id))

                f_ssn = ""
                f_id = ""
                f_dob = ""
                f_addr = ""
                f_phone = ""
                f_f = ""
                f_l = ""
                f_mrn = ""
                i = 0

    print(len(matched_results))
    print(matched_results[0])
    pair2txt("sub25_neg.txt", matched_results)
    #create_submission_csv("sub25_neg.txt", "sub25_neg.csv")
    pairs2csv(matched_results, "56588_neg.csv")


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
    pairs2txt(results, ff)
    pairs2csv(results, "alternaitve_check_pairs_merged.csv")


def main():
    # filter_check()
    # exclude_check()
    # merge_two_txt()
    # create_submission_csv("process_full_cover.txt", "sub25_raw.csv")
    ps = extract_pairs_from_txt("56588.txt")
    print(len(ps))
    pairs2csv(ps, "56588a.csv")


if __name__ == '__main__':
    main()

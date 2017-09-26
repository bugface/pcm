import csv
import sys
from ssn_normalization import measure_ssn_similarity
from process_dob import check_dob
from deterministic_rule_pipeline_new_version import pairs2csv, pair2txt, create_submission_csv, extract_pairs_from_txt
from mrn_normalization import measure_mrn_distance, measure_mrn_similarity
from address_normalization import normalize_address


def filter_check():
    base = set(extract_pairs_from_txt("latest_result.txt"))
    csv_file = "processed_full_cover_detail.csv"
    #csv_file = "filtered_full_cover_comnined_deduped.csv"

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
        f_m = ""
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
                f_m += each['MIDDLE']
                f_mrn += each['MRN']
                if f_mrn != "":
                    f_int_mrn += int(each['MRN'])
                # print(f_addr)
            else:
                s_ssn = each['SSN']
                s_dob = each['DOB']
                s_id = int(each['ENTERPRISEID'])
                s_addr = normalize_address(each['ADDRESS1'])
                s_phone = each['PHONE']
                s_f = each['FIRST_']
                s_l = each['LAST_']
                s_m = each['MIDDLE']
                s_mrn = each['MRN']

                mm = measure_mrn_distance(f_mrn, s_mrn)
                ss = measure_ssn_similarity(f_ssn, s_ssn, "w")

                t = (f_id, s_id)
                tp = (s_id, f_id)
                if t not in base and tp not in base:

                    # if f_l == s_l and f_dob == s_dob and f_dob != "" and f_m == s_m and f_m != "":
                    #     matched_results.add((f_id, s_id))

                    # if f_f == s_f and f_l == s_l and check_dob(f_dob, s_dob):
                    #     matched_results.add((f_id, s_id))

                    # if f_f == s_f and f_l == s_l and f_addr == s_addr and f_addr != "":
                    #     matched_results.add((f_id, s_id))

                    # if ((f_f == s_f and f_dob == s_dob) or (f_l == s_l and f_dob == s_dob)) and f_addr == s_addr and f_addr != "":
                    #     matched_results.add((f_id, s_id))

                    # #2209
                    # if f_f==s_f and s_l == f_l and f_m == s_m and f_f != '' and f_l != "" and f_m != "":
                    #     matched_results.add((f_id, s_id))

                    if mm > 0 and (mm / max(int(f_mrn), int(s_mrn))) <= 0.005 and s_dob == f_dob and f_dob != "":
                        matched_results.add((f_id, s_id))

                    # if measure_ssn_similarity(f_ssn, s_ssn, "w") >= 0.95:
                    #     matched_results.add((f_id, s_id))

                    # elif check_dob(f_dob, s_dob):
                    #     matched_results.add((f_id, s_id))

                    # if f_addr != "" and f_addr == s_addr:
                    #     matched_results.add((f_id, s_id))

                    # elif f_phone == s_phone and f_phone != "":
                    #     matched_results.add((f_id, s_id))

                    # elif measure_mrn_similarity(f_mrn, s_mrn, "w") >= 0.90:
                    #     matched_results.add((f_id, s_id))

                    # else:
                    #     not_matched.append((f_id, s_id))

                    #matched_results.add((f_id, s_id))

                f_int_mrn = 0
                f_ssn = ""
                f_id = 0
                f_dob = ""
                f_addr = ""
                f_phone = ""
                f_f = ""
                f_l = ""
                f_mrn = ""
                f_m = ""

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

    # pair2txt("check_now.txt", list(matched_results))
    # create_submission_csv("check_now.txt", "sub_9-25-2254.csv")
    pairs2csv(list(matched_results), "check_now_dob.csv")


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
    filter_check()
    # merge_two_txt()


if __name__ == '__main__':
    main()

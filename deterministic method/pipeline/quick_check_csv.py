import csv
import sys
from ssn_normalization import measure_ssn_similarity, match_partial_ssn
from process_dob import check_dob
from deterministic_rule_pipeline_new_version import pairs2csv, pair2txt, create_submission_csv, extract_pairs_from_txt
from mrn_normalization import measure_mrn_distance, measure_mrn_similarity
from address_normalization import normalize_address
from multiprocessing import Process, Manager

def filter_check():
    base = set(extract_pairs_from_txt("latest_result_sacrify_percision.txt"))
    for each in extract_pairs_from_txt("neg_pairs.txt"):
        base.add(each)
    csv_file = "working_csvfile.csv"

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
        f_g = ""
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
                f_g += each['GENDER']
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
                s_g = each['GENDER']

                mm = measure_mrn_distance(f_mrn, s_mrn)
                ss = measure_ssn_similarity(f_ssn, s_ssn, "w")

                t = (str(f_id), str(s_id))
                tp = (str(s_id), str(f_id))

                if t not in base and tp not in base:
                    ####possible features

                    if f_g == s_g and f_g == 'F' and f_f == s_f and f_f != "" and check_dob(f_dob, s_dob) and mm > 0 and (mm / max(int(f_mrn), int(s_mrn))) <= 0.005:
                        matched_results.add((f_id, s_id))


                    # if f_g == s_g and f_g == 'M' and f_l == s_l and f_l != "" and check_dob(f_dob, s_dob) and mm > 0 and (mm / max(int(f_mrn), int(s_mrn))) <= 0.005:
                    #     matched_results.add((f_id, s_id))

                    # if (f_dob == s_dob and f_dob != "") or (f_addr == s_addr and f_addr != "") or (f_f == s_f and f_f != "") or (mm > 0 and mm < 200) or (f_m == s_m and f_m != "") or match_partial_ssn(f_ssn, s_ssn):
                    #     # can add a neg filter for not same ssn
                    #     matched_results.add((f_id, s_id))

                    # if f_l == s_l and f_dob == s_dob and f_dob != "" and f_m == s_m and f_m != "":
                    #     matched_results.add((f_id, s_id))

                    # if f_f == s_f and f_l == s_l and check_dob(f_dob, s_dob):
                    #     matched_results.add((f_id, s_id))

                    # if f_f == s_f and f_l == s_l and f_addr == s_addr and f_addr != "":
                    #     matched_results.add((f_id, s_id))

                    # if ((f_f == s_f and f_dob == s_dob) or (f_l == s_l and f_dob == s_dob)) and f_addr == s_addr and f_addr != "":
                    #     matched_results.add((f_id, s_id))

                    # #2209
                    # if f_f==s_f and s_l == f_l and f_m == s_m and f_f != "" and f_l != "" and f_m != "":
                    #     matched_results.add((f_id, s_id))

                    #37 -> dob (new dataset)
                    # if mm > 0 and mm < 100:
                    #     matched_results.add((f_id, s_id))

                    #1086 -> one of the f or l name
                    #260 -> both f, l match
                    # if mm > 0 and (mm / max(int(f_mrn), int(s_mrn))) <= 0.005 and (f_f == s_f and f != "" and f_l == s_l and f != ""):
                    #     matched_results.add((f_id, s_id))

                    #60 on new dataset
                    # if f_f == s_f and f_f != "" and (s_l == "" or f_l == "") and check_dob(s_dob, f_dob) and f_addr == s_addr:
                    #     matched_results.add((f_id, s_id))

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
                f_g = ""

    print(len(matched_results))
    pair2txt("exp8g.txt", list(matched_results))
    create_submission_csv("exp8g.txt", "sub_9-29-136.csv")
    pairs2csv(list(matched_results), "exp8g.csv")


def reshape_data():
    from collections import OrderedDict

    with open("merged_working_csvfile.csv", "w", newline='') as f1:
        with open("working_csvfile.csv", "r") as f:
            reader = csv.DictReader(f)
            header = reader.fieldnames
            new_header = []
            for each in header:
                h1 = each + "_1"
                h2 = each + "_2"
                new_header.append(h1)
                new_header.append(h2)
            writer = csv.DictWriter(f1, fieldnames=new_header)
            writer.writeheader()
            l1 = None
            l2 = None
            d = OrderedDict()
            for i, each in enumerate(reader):
                if i % 2 == 0:
                    l1 = each
                else:
                    l2 = each
                    for k, v in l1.items():
                        d[k+"_1"] = v
                    for k, v in l2.items():
                        d[k+"_2"] =v
                    writer.writerow(d)
                    l1 = None
                    l2 = None
                    d = OrderedDict()


def filter_check_multiprocess():
    base = set(extract_pairs_from_txt("latest_result_sacrify_percision.txt"))
    base_neg = set(extract_pairs_from_txt("neg_pairs.txt"))

    csv_file = "working_csvfile.csv"



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
    # reshape_data()


if __name__ == '__main__':
    main()

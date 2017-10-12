import csv
import sys
import json
from ssn_normalization import measure_ssn_similarity, match_partial_ssn
from process_dob import check_dob
from deterministic_rule_pipeline_new_version import pairs2csv, pair2txt, create_submission_csv, extract_pairs_from_txt
from mrn_normalization import measure_mrn_distance, measure_mrn_similarity
from address_normalization import normalize_address
from multiprocessing import Process, Manager
from names_normalization import measure_name_distance

def dob_diff(f_dob, s_dob):
    if f_dob != "" and s_dob != "":
        y = abs(int(f_dob.split(" ")[0].split("-")[0]) - int(s_dob.split(" ")[0].split("-")[0]))
        m = int(f_dob.split(" ")[0].split("-")[1]) - int(s_dob.split(" ")[0].split("-")[1])
        d = abs(int(f_dob.split(" ")[0].split("-")[2]) - int(s_dob.split(" ")[0].split("-")[2]))
        if (y in [0, 10, 20, 30] and m == 0 and d == 0) or (d ==1 and m == 0 and y == 0) or (d ==0 and m == 0 and y == 0):
            return True
    return False

def filter_check():
    with open("name_alter_spelling_json_data.json", "r") as f:
        ansd = json.load(f)
    base = set(extract_pairs_from_txt("latest_result_sacrify_percision.txt"))
    # base = set(extract_pairs_from_txt("latest_result.txt"))
    for each in extract_pairs_from_txt("neg_pairs.txt"):
        base.add(each)
    csv_file = "working_csvfile.csv"
    # csv_file = "process_new1_full_cover.csv" # stll have 100 left

    # csv_file = "xxxx.csv"

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
        f_email = ""
        f_int_mrn = 0
        f_mmn = ""
        f_a = ""
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
                f_email += each['EMAIL']
                f_mmn += each['MOTHERS_MADIDEN_NAME']
                f_a += each['ALIAS_']
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
                s_email = each['EMAIL']
                s_mmn = each['MOTHERS_MADIDEN_NAME']
                s_a = each['ALIAS_']
                mm = measure_mrn_distance(f_mrn, s_mrn)
                ss = measure_ssn_similarity(f_ssn, s_ssn, "w")
                cb = check_dob(f_dob, s_dob)

                try:
                    f_alter = ansd[f_f]
                except:
                    f_alter = []

                try:
                    s_alter = ansd[s_f]
                except:
                    s_alter = []

                t = (str(f_id), str(s_id))
                tp = (str(s_id), str(f_id))
                # if mm > 1000 or (f_m != s_m and f_m != "" and s_m != ""):
                #     matched_results.add((f_id, s_id))

                if t not in base and tp not in base:
                    if mm > 0 and (mm / max(int(f_mrn), int(s_mrn))) <= 0.01:
                        matched_results.add((f_id, s_id))


                    # if ss == -1 or mm == -1 and f_f == s_f and s_l == f_l and f_m == s_m and (cb or (f_addr == s_addr)):
                    #     matched_results.add((f_id, s_id))

                    # fff = []
                    # ffl = []
                    # ffm = []
                    # sff = []
                    # sfl = []
                    # sfm = []
                    # #print(f_a)
                    # f1 = process_alias(f_a, fff, ffm, ffl)
                    # s1 = process_alias(s_a, sff, sfm, sfl)
                    # #print(f1)
                    # #2336
                    # if ((f_f in s1[0] and f_l in s1[2]) or (s_f in f1[0] and s_l in f1[2])):
                    #         matched_results.add((f_id, s_id))

                    # if (mm > 0 and (mm / max(int(f_mrn), int(s_mrn))) <= 0.005 and (f_f == s_f and f_f != "" and f_l == s_l and f_l != "")) or (mm >0 and mm < 1000) or match_partial_ssn(f_ssn, s_ssn):
                    #35/2050
                   # if (f_ssn == "" or s_ssn == "") and  mm == -1 and measure_name_distance(f_f, s_f) > 0.8 and f_m == s_m and f_g == s_g:
                    # if (f_ssn == "" or s_ssn == "") and  mm == -1 and measure_name_distance(f_f, s_f) > 0.8 and measure_name_distance(f_l, s_l) > 0.8 and f_m == s_m and f_g == s_g:
                    #
                    #             matched_results.add((f_id, s_id))


                        # if f_m != "" and s_m != "":
                        #     if (len(f_m)==1 or len(s_m)==1) and f_m[0] == s_m[0]:
                        #         matched_results.add((f_id, s_id))
                        #     elif f_m == s_m:
                        #         matched_results.add((f_id, s_id))


                    # if (f_alter != "" and s_f in f_alter) or (s_alter != "" and f_f in s_alter):
                    #     matched_results.add((f_id, s_id))

                    # if cb:
                    #     matched_results.add((f_id, s_id))

                    # if f_mmn == s_mmn and f_mmn != "":
                    #     matched_results.add((f_id, s_id))


                    #     if f_ssn == s_ssn and f_ssn != "":
                    #         print(f_id)


                    # if f_f==s_f and s_l == f_l and f_m == s_m and f_f != "" and f_l != "" and f_m != "" and f_addr == s_addr:
                    #     matched_results.add((f_id, s_id))


                    # if  not check_dob(f_dob,s_dob):
                    #     print((f_id, s_id))
                    #     matched_results.add((f_id, s_id))

                    # if f_addr == s_addr and f_addr != "" and check_dob(f_dob, s_dob) and f_m==s_m and f_m != "":
                    #      matched_results.add((f_id, s_id))

                    #print(f_dob.split(" ")[0].split("-")[0])
                    # y = int(f_dob.split(" ")[0].split("-")[0]) - int(s_dob.split(" ")[0].split("-")[0])
                    # m = int(f_dob.split(" ")[0].split("-")[1]) - int(s_dob.split(" ")[0].split("-")[1])
                    # d = int(f_dob.split(" ")[0].split("-")[2]) - int(s_dob.split(" ")[0].split("-")[2])

                    # if ((y == 10 or y==20 or y==30) and d == 0 and m == 0) or (y == 0 and  m == 0 and d == 1):
                    #     # print((f_id, s_id))
                    #     matched_results.add((f_id, s_id))

                    # if f_f==s_f and f_l==s_l:
                    #     matched_results.add((f_id, s_id))

                    # if f_phone == s_phone and f_phone != "":
                    # if f_ssn == s_ssn and f_ssn != "":
                    # if mm > 1000:
                    # # if d == 30:
                    # # if f_l == s_l:
                    #     # print((f_id, s_id))
                    #     matched_results.add((f_id, s_id))

                    # if (mm > 0 and mm < 200) or match_partial_ssn(f_ssn, s_ssn) or (f_dob == s_dob and f_dob != "" and f_addr == s_addr and f_addr != "") or (f_phone == s_phone and f_phone != ""): #1133
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

                    # if f_ssn == s_ssn and f_ssn != "":
                    #     matched_results.add((f_id, s_id))

                    # if (mm >0 and mm < 1000) or match_partial_ssn(f_ssn, s_ssn):
                    # if mm > 0 and (f_ssn != "" and s_ssn != "") and (f_f != s_f or f_l != s_l):
                    #     # print(f_id, s_id)
                        # matched_results.add((f_id, s_id))

                    # if not check_dob(f_dob, s_dob):
                    #     matched_results.add((f_id, s_id))
                    # y = int(f_dob.split(" ")[0].split("-")[0]) - int(s_dob.split(" ")[0].split("-")[0])
                    # m = int(f_dob.split(" ")[0].split("-")[1]) - int(s_dob.split(" ")[0].split("-")[1])
                    # d = int(f_dob.split(" ")[0].split("-")[2]) - int(s_dob.split(" ")[0].split("-")[2])
                    # if f_f == s_f and f_l == s_l and (((y == 10 or y==20 or y==30) and d == 0 and m == 0) or (y == 0 and  m == 0 and d == 1)):
                    #     matched_results.add((f_id, s_id))
                    # if f_m != s_m:
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
                f_email = ""
                f_mmn = ""
                f_a = ""
    print(len(matched_results))

    dec = input("output? ")
    if dec == "Y":
        pair2txt("xxxx1.txt", list(matched_results))
        create_submission_csv("xxxx1.txt", "sub_10-9-.csv")
        pairs2csv(list(matched_results), "xxxx2.csv")

def process_alias(f_a, fff, ffm, ffl):
    num = 0
    if f_a != "":
        if "^^" in f_a:
            fass = f_a.split("^^")
            for each in fass:
                fas = each.split(" ")
                if len(fas) == 2:
                    fff.append(fas[0])
                    ffl.append(fas[1])
                elif len(fas) == 3:
                    fff.append(fas[0])
                    ffm.append(fas[1])
                    ffl.append(fas[2])
        else:
            fas = f_a.split(" ")
            if len(fas) == 2:
                fff.append(fas[0])
                ffl.append(fas[1])
            elif len(fas) == 3:
                fff.append(fas[0])
                ffm.append(fas[1])
                ffl.append(fas[2])
        num = len(fas)
    return (fff, ffm, ffl, num)


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

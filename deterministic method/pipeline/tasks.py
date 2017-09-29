from deterministic_rule_pipeline_new_version import pipline_get_detail, pairs2csv, pair2txt, create_submission_csv, extract_pairs_from_txt, pairs2csv_single
from filter_pipeline import filter_data_in_csv
from multiprocessing import Process
from concurrent.futures import ThreadPoolExecutor
from address_normalization import geocoding
import csv
import time
from geopy.geocoders import Nominatim
import os


def task1(csv_file, out_txt, out_csv, s, m, i):
    res = filter_data_in_csv(csv_file, s, m, i)
    pair2txt(out_txt, res)
    pairs2csv(res, out_csv)


def main():
    #create_submission_csv("pairs_in_txt_generated_from_pipline\\process_full_cover.txt", "sub25_raw.csv")
    # work 1 config input:
    # base_file = "stgy7.txt"
    # rule_file = "rules_detail_process_address.txt"
    # folder = "txt\\3_fields_process_address"
    # output_csv_file = "addr_to_process.csv"
    # output_pair_file = "addr_to_process.txt"
    # job = "d" #("d" = detail, "p" = only pairs)
    # #run pipline
    # if not os.path.exists(folder):
    #   os.makedirs(folder)
    # pipline_get_detail(rule_file, folder, base_file, output_csv_file, output_pair_file, job)

    # work 2 config input:
    # input_file = "addr_to_process_final_pairs2.txt"
    # output_file = "addr_to_process_final_pairs2.csv"
    # pairs = []
    # with open(input_file, "r") as f:
    #   for each in f:
    #       #print(each[:-1])
    #       t = each[:-1].split('\t')
    #       #print(t)
    #       t1 = t[0]
    #       t2 = t[1]
    #       ts = (int(t1), int(t2))
    #       pairs.append(ts)
    # pairs2csv(pairs, output_file)

    # work 3
    '''
    condfig:
    output query file names for name normalization:
        process_first_name.csv
        process_last_name.csv
    output name normalization file names:
        process_first_name.txt
        process_last_name.txt
    '''
    # files used in project
    # base_file = "stgy7.txt"
    # rule_file_first = "rules_detail_first.txt"
    # rule_file_last = "rules_detail_last.txt"
    # folder1 = "txt\\3_fields_process_name\\first_name"
    # folder2 = "txt\\3_fields_process_name\\last_name"
    # output_pair_file_first = "process_first_name_pairs.txt"
    # output_csv_file_first = "process_first_name1.csv"
    # output_pair_file_last = "process_last_name_pairs.txt"
    # output_csv_file_last = "process_last_name1.csv"
    # job = "d"

    # if not os.path.exists(folder1):
    #   os.makedirs(folder1)
    # if not os.path.exists(folder2):
    #   os.makedirs(folder2)

    #p1 = Process(target=pipline_get_detail, args=(rule_file_first, folder1, base_file, output_csv_file_first, output_pair_file_first, job))
    #p2 = Process(target=pipline_get_detail, args=(rule_file_last, folder2, base_file, output_csv_file_last, output_pair_file_last, job))

    # p1.start()
    # p2.start()

    # p1.join()
    # p2.join()
    #pipline_get_detail(rule_file_first, folder1, base_file, output_csv_file_first, output_pair_file_first, job)

    # t1 = get_extra_pairs_not_in_base(base_file, ["process_first_name_222.txt"])
    # t2 = get_extra_pairs_not_in_base(base_file, ["process_last_name_222.txt"])

    # print(len(t1))
    # print(len(t2))

    # create_submission_csv("process_first_name_222.txt", "sub10.csv")
    # create_submission_csv("process_last_name_222.txt", "sub11.csv")

    # e = get_extra_pairs_not_in_base("process_first_last_combined.txt", [ "addr_to_process_final_pairs2.txt"])
    # print(len(e))
    # pairs2txt(e, "ex.txt")

    # process dob job
    # base_file = "stgy7_process_first_last_address_combined.txt"
    # rule_file = "rules_detail_dob.txt"
    # folder = "txt\\3_fields_process_dob"
    # job = "d"
    # output_csv_file = "process_dob1.csv"
    # output_pair_file = "process_dob1.txt"

    # process ssn job
    # base_file = "stgy7_process_first_last_address_dob_combined.txt"
    # rule_file = "rules_detail_ssn.txt"
    # folder = "txt\\3_fields_process_ssn"
    # job = "d"
    # output_csv_file = "process_ssn.csv"
    # output_pair_file = "process_ssn.txt"

    # process mrn jobP
    # base_file = "stgy7_process_first_last_address_dob_ssn_merged_with_dedupe_combined.txt"
    # rule_file = "rules_detail_mrn.txt"
    # folder = "txt\\3_fields_process_mrn"
    # job = "d"
    # output_csv_file = "process_mrn.csv"
    # output_pair_file = "process_mrn.txt"

    # process alternative job
    # base_file = "55437.txt"
    # rule_file = "rules_detail_alternative.txt"
    # folder = "txt\\3_fields_process_alternative"
    # job = "d"
    # output_csv_file = "process_alternative.csv"
    # output_pair_file = "process_alternative.txt"

    # csv_file1 = "process_full_cover.csv"
    # out_txt1 = "processed_full_cover.txt"
    # out_csv1 = "processed_full_cover.csv"

    # csv_file2 = "56588.csv"
    # out_txt2 = "processed_neg_56588.txt"
    # out_csv2 = "processed_neg_56588.csv"

    # task1(csv_file2, out_txt2, out_csv2, 0.9, 400, "neg")

    # from multiprocessing import Process
    # p1 = Process(target=task1, args=(csv_file1, out_txt1, out_csv1, 0.9, 400, "pos"))
    # p2 = Process(target=task1, args=(csv_file2, out_txt2, out_csv2, 0.9, 200, "neg"))

    # p1.start()
    # p2.start()
    # p1.join()
    # p2.join()

    # process alter_last job
    # base_file = "56588.txt"
    # rule_file = "rules_detail_full_cover.txt"
    # folder = "txt\\full_cover"
    # job = "p"
    # output_csv_file = "process_full_cover.csv"
    # output_pair_file = "process_full_cover.txt"

    # if not os.path.exists(folder):
    #     os.makedirs(folder)

    # pipline_get_detail(rule_file, folder, base_file,
    #                    output_csv_file, output_pair_file, job)

    #create_submission_csv("stgy7_process_first_last_address_dob_ssn_merged_with_dedupe_combined.txt", "sub19.csv")

    # create_submission_csv("processed_neg_56588.txt", "processed_neg_56588.csv")
    # create_submission_csv("processed_full_cover.txt", "processed_full_cover.csv")

    #pairs = extract_pairs_from_txt("56626.txt")

    #pairs2csv(pairs, "56626.csv")
    #pairs2csv_single(pairs, "56626(1).csv")

    #create_submission_csv("56626.txt", "sub27.csv")
    # print("pos: ")
    # filter_data_in_csv("processed_full_cover_detail.csv", "pos")

    # print("mul: ")

    # p1 = Process(target=filter_data_in_csv, args=(".csv", "pos", 0.95, 500, 4, 0.8, "latest_result.txt"))
    # p2 = Process(target=filter_data_in_csv, args=(".csv", "mul", 1, 300, 4, 0.85, "latest_result.txt"))
    # # p3 = Process(target=filter_data_in_csv, args=("56626.csv", "neg", 1, 500, 3, 0.85, "latest_result.txt"))

    # p1.start()
    # p2.start()
    # # p3.start()

    # p1.join()
    # p2.join()
    # p3.join()

    # pairs = filter_data_in_csv("todo/filtered_56626_neg_1(135 error).csv", "mul", 1, 300, 4, 0.85, "dummyb.txt")
    # print(len(pairs))
    # task1(pairs, "neg_lastest.txt", "sub_neg_lastest.csv", "neg_lastest.csv")

    # redo again
    # p1 = filter_data_in_csv("processed_full_cover_detail.csv", "mul", 0.95, 1000, 3, 0.85, "latest_result.txt")
    # task1(p1, "filtered_full_cover_1.txt", "sub28_1.csv", "filtered_full_cover_1.csv")

    # p2 = filter_data_in_csv("56626.csv", "neg", 1, 500, 3, 0.85, "latest_result.txt")
    # task1(p2, "filtered_56626_neg_1.txt", "sub_56626_neg_1.csv", "filtered_56626_neg_1.csv")
    # locator = Nominatim(country_bias="us")

    # with open("processed_full_cover_detail.csv", "r") as f:
    #     reader = csv.DictReader(f)
    #     for each in reader:
    #         addr = ",".join([each['ADDRESS1'], each['CITY'], each['STATE_'].strip(), "USA"])
    #         #used for google API or Nominatim
    #         na = locator.geocode(query=addr, timeout=10)
    #         print(addr)
    #         if na is not None:
    #             print(na.address)
    #         else:
    #             print("None")
    #         time.sleep(2)

#merge two files on distinct pairs
    # s = set()
    # s1 = set()
    # s2 = set()
    # with open("latest_result_sacrify_percision.txt", "r") as f:
    #     for each in f:
    #         p = each[:-1].split("\t")
    #         t = (p[0], p[1])
    #         s2.add(t)

    # with open("temp.txt", "r") as f:
    #      for each in f:
    #         p = each[:-1].split("\t")
    #         t = (p[0], p[1])
    #         s.add(t)

    # with open("exp8f_sub.txt", "r") as f:
    #     for each in f:
    #         p = each[:-1].split("\t")
    #         t = (p[0], p[1])
    #         tp = (p[1], p[0])
    #         if tp not in s and t not in s and t not in s2 and tp not in s2:
    #             s1.add(t)
    # print(len(s1))

    # pair2txt("exp8f.txt", list(s1))
    # # #create_submission_csv("filtered_full_cover_comnined_deduped.txt", "sub_filtered_full_cover_comnined_deduped.csv")
    # pairs2csv(list(s), "dugeyunpengheti_1.csv")

    # create_submission_csv("temp.txt", "sub_9-29-1243.csv")
    # pairs = extract_pairs_from_txt("new.txt")
    # pairs2csv(pairs, "56656_neg_filtered.csv")
    #create_submission_csv("check_now.txt", "sub_9-22-643.csv")

    # create_submission_csv("latest_result_sacrify_percision.txt", "sub_9-28-1230.csv")

    # p = extract_pairs_from_txt("process_new_full_cover_remove_dupes.txt")
    # pairs2csv(p, "process_new_full_cover_remove_dupes.csv")

    # s = []
    # header = None

    ## reduce the working_csv.csv to two pairs are in one line
    # csv_files = ["process_new_full_cover_remove_dupes.csv", "processed_full_cover_detail.csv"]
    # for each_csv in csv_files:
    #     with open(each_csv, "r") as f:
    #         reader = csv.DictReader(f)

    #         header = reader.fieldnames
    #         for each in header:


    #         for each in reader:
    #             s.append(each)
    # print(len(s))
    # with open("working_csvfile.csv", "w", newline='') as f:
    #     writer = csv.DictWriter(f, fieldnames=header)
    #     writer.writeheader()
    #     writer.writerows(s)


def task1(pairs, file, sub, detail):
    pair2txt(file, pairs)
    create_submission_csv(file, sub)
    pairs2csv(pairs, detail)


if __name__ == '__main__':
    main()

'''
pipeline work template:

##query pcm table
base_file = "56588.txt"
rule_file = "rules_detail_full_cover.txt"
folder = "txt\\full_cover"
job = "p"
output_csv_file = "process_full_cover.csv"
output_pair_file = "process_full_cover.txt"

if not os.path.exists(folder):
    os.makedirs(folder)

pipline_get_detail(rule_file, folder, base_file,
                   output_csv_file, output_pair_file, job)

##query pmac table
base_file = "latest_result_sacrify_percision.txt"
rule_file = "newrules_detail_full_cover.txt"
folder = "txt\\new_full_cover"
job = "p"
output_csv_file = "process_new_full_cover.csv"
output_pair_file = "process_new_full_cover.txt"
table = "pmac"

if not os.path.exists(folder):
    os.makedirs(folder)

pipline_get_detail(rule_file, folder, base_file,
                   output_csv_file, output_pair_file, job, table)
'''

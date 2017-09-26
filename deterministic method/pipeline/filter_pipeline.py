import csv
import json
import time
from ssn_normalization import measure_ssn_similarity
from mrn_normalization import measure_mrn_distance
from address_normalization import normalize_address
from deterministic_rule_pipeline_new_version import pairs2csv, pair2txt, extract_pairs_from_txt
from names_normalization import measure_string_distance
from process_dob import check_dob

fields = ["SSN", "MRN", "ADDRESS", "FNAME", "LNAME", "PHONE", "DOB"]


def _remove_covered_pairs(matched_pairs, base_file):
    after_dedupe = []
    base_pairs = set(extract_pairs_from_txt(base_file))
    for each in matched_pairs:
        each_prime = (each[1], each[0])
        if each not in base_pairs and each_prime not in base_pairs:
            after_dedupe.append(each)
    return after_dedupe

# main function for filter data in csv


def filter_data_in_csv(csv_file, filter_flag, ssn_level=0.90, mrn_level=400, multi_filter_threshold=3, name_similarity_threhold=0.9, base_file="latest_result.txt"):
    # filter flag has values in ["pos", "neg", "mul"]
    # filter mainly based on mrn distance and ssn similarity
    # the two filter pramaters can be manully tuned for best performance
    matched_pairs = list()

    with open("name_alter_spelling_json_data.json", "r") as f:
        alter_spelling_dict = json.load(f)

    f_ssn = ""
    f_mrn = ""
    f_id = ""
    f_addr = ""
    f_f_name = ""
    f_phone = ""
    f_list = list()

    with open(csv_file, "r") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if row['DOB'] is None:
                row['DOB'] = ""
            if i % 2 == 0:
                f_ssn = row["SSN"]
                f_mrn = row["MRN"]
                f_id = row["ENTERPRISEID"]
                f_addr = row["ADDRESS1"]
                f_f_name = row["FIRST_"]
                f_l_name = row["LAST_"]
                f_phone = row["PHONE"]
                f_dob = row['DOB']
                f_list = [f_ssn, f_mrn, f_addr,
                          f_f_name, f_l_name, f_phone, f_dob]
            else:
                measured_ssn_level = measure_ssn_similarity(
                    f_ssn, row["SSN"], "w")
                measured_mrn_level = measure_mrn_distance(f_mrn, row["MRN"])

                if filter_flag == "pos":
                    if positive_filter(measured_ssn_level, ssn_level, measured_mrn_level, mrn_level):
                        matched_pairs.append((f_id, row["ENTERPRISEID"]))
                elif filter_flag == "neg":
                    if negative_filter(measured_ssn_level, ssn_level, measured_mrn_level, mrn_level):
                        matched_pairs.append((f_id, row["ENTERPRISEID"]))
                elif filter_flag == "mul":
                    s_list = [row["SSN"], row["MRN"], row["ADDRESS1"],
                              row["FIRST_"], row["LAST_"], row["PHONE"], row['DOB']]
                    if multiple_fields_filter(multi_filter_threshold,
                                              f_list,
                                              s_list,
                                              ssn_level,
                                              mrn_level,
                                              name_similarity_threhold,
                                              alter_spelling_dict):
                        matched_pairs.append((f_id, row["ENTERPRISEID"]))

                # reset f_ssn, f_mrn, f_id
                f_ssn = ""
                f_mrn = ""
                f_id = ""
                f_addr = ""
                f_f_name = ""
                f_list = []

    if filter_flag == "pos":
        print("using positive filter, totally find {} pairs of records.".format(
            len(matched_pairs)))
        matched_pairs = _remove_covered_pairs(matched_pairs, base_file)
    elif filter_flag == "neg":
        print("using negative filter, totally find {} pairs of records.".format(
            len(matched_pairs)))
    elif filter_flag == "mul":
        print("using multi-fields filter, totally find {} pairs of records.".format(len(matched_pairs)))
        matched_pairs = _remove_covered_pairs(matched_pairs, base_file)

    return matched_pairs


def positive_filter(measured_ssn_level, ssn_level, measured_mrn_level, mrn_level):
    if measured_ssn_level >= ssn_level or (measured_mrn_level >= 0 and measured_mrn_level <= mrn_level):
        return True
    return False


def negative_filter(measured_ssn_level, ssn_level, measured_mrn_level, mrn_level):
    if measured_ssn_level >= ssn_level or (measured_mrn_level < mrn_level and measured_mrn_level > 0):
        return False
    return True


def multiple_fields_filter(multi_filter_threshold, f_list, s_list, ssn_level, mrn_level, name_similarity_threhold, alter_spelling_dict):
    # if threshold meet the input which means more than certain amount of attributes are the same
    # return true only if certain number of attibutes are the same which indicate at large possibility
    # that the two records are the same
    d = dict()
    for field, pairs in zip(fields, zip(f_list, s_list)):
        d[field] = pairs
    count = 0
    # print(d)
    # process each field in dictionary
    for k, v in d.items():
        if k == "SSN":
            #print('ssn: ' + str(measure_ssn_similarity(v[0], v[1], "w")))
            if measure_ssn_similarity(v[0], v[1], "w") >= ssn_level:
                count += 1
                # print("ssn: " + str(measure_ssn_similarity(v[0], v[1], "w")))
        elif k == "MRN":
            mrn_measured_level = measure_mrn_distance(v[0], v[1])
            #print("mrn" + str(mrn_measured_level))
            if mrn_measured_level <= mrn_level and mrn_measured_level >= 0:
                count += 1
                # print("mrn")
        elif k == "ADDRESS":
            an1 = normalize_address(v[0])
            an2 = normalize_address(v[1])
            #print("address: {}, {}".format(an1, an2))
            if an1 != "" and an1 == an2:
                count += 1
                # print("addr")
        elif k == "FNAME" or k == "LNAME":
            sim_max = 0
            for method in range(1, 6):
                similarity = measure_string_distance(v[0], v[1], method)
                sim_max = max(similarity, sim_max)
            if sim_max > name_similarity_threhold:
                count += 1
                # print("name")
            else:
                if v[0] in alter_spelling_dict:
                    v_alter = alter_spelling_dict[v[0]]
                    if v[1] in v_alter:
                        count += 1
                        # print("alter name")
                elif v[1] in alter_spelling_dict:
                    v_alter = alter_spelling_dict[v[1]]
                    if v[0] in v_alter:
                        count += 1
                        # print("alter name")
        elif k == "PHONE":
            # not considering error phone number here
            if v[0] == v[1] and v[0] != '':
                count += 1
                # print("phone")
        elif k == "DOB":
            # print(v)
            if check_dob(v[0], v[1]):
                count += 1
                # print("dob")
    # print(count)
    if count >= multi_filter_threshold:
        # ii = input("c?")
        return True
    return False

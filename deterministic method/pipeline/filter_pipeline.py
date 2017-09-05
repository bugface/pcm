import csv
from ssn_normalization import measure_ssn_similarity
from mrn_normalization import measure_mrn_distance
from address_normalization import normalize_address
from deterministic_rule_pipeline_new_version import pairs2csv, pair2txt

#main function for filter data in csv
def filter_data_in_csv(csv_file, ssn_level=0.9, mrn_level=250, filter_flag="pos", multi_filter_threshold=3, name_similarity_threhold=0.9):
    # filter mainly based on mrn distance and ssn similarity
    # the two filter pramaters can be manully tuned for best performance
    if filter_flag == "pos":
        print("using positive fiter...")
    elif filter_flag == "neg":
        print("using negative fiter...")
    matched_pairs = set()
    fields = ["SSN", "MRN", "ADDRESS", "FNAME", "LNAME", "PHONE", "DOB"]

    f_ssn = ""
    f_mrn = ""
    f_id = ""
    f_addr = ""
    f_f_name = ""
    f_phone = ""
    f_list = list(s)


    with open(csv_file, "r") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i % 2 == 0:
                f_ssn = row["SSN"]
                f_mrn = row["MRN"]
                f_id = row["ENTERPRISEID"]
                f_addr = row["ADDRESS1"]
                f_f_name = row["FIRST_"]
                f_phone = row["PHONE"]

            else:
                measured_ssn_level = measure_ssn_similarity(
                    f_ssn, row["SSN"], "w")
                measured_mrn_level = measure_mrn_distance(f_mrn, row["MRN"])

                if filter_flag == "pos":
                    if positive_filter(measured_ssn_level, ssn_level, measured_mrn_level, mrn_level):
                        matched_pairs.add((f_id, row["ENTERPRISEID"]))
                elif filter_flag == "neg":
                    if negative_filter(measured_ssn_level, ssn_level, measured_mrn_level, mrn_level):
                        matched_pairs.add((f_id, row["ENTERPRISEID"]))
                elif filter_flag == "mul":
                    if multiple_fields_filter(multi_filter_threshold, ):
                        matched_pairs.add((f_id, row["ENTERPRISEID"]))

                # reset f_ssn, f_mrn, f_id
                f_ssn = ""
                f_mrn = ""
                f_id = ""
                f_addr = ""
                f
                f_list = []

    print("Totally find {} pairs of records.".format(len(matched_pairs)))
    return list(matched_pairs)


def positive_filter(measured_ssn_level, ssn_level, measured_mrn_level, mrn_level):
    if measured_ssn_level >= ssn_level or (measured_mrn_level >= 0 and measured_mrn_level <= mrn_level):
        return True
    return False


def negative_filter(measured_ssn_level, ssn_level, measured_mrn_level, mrn_level):
    if (measured_ssn_level < ssn_level and measured_ssn_level >= 0) and measured_mrn_level > mrn_level:
        return True
    return False

def multiple_fields_filter(multi_filter_threshold, f_list, s_list, ssn_level, mrn_level, name_similarity_threhold):
    # if threshold meet the input which means more than certain amount of attributes are the same
    # return true only if certain number of attibutes are the same which indicate at large possibility
    # that the two records are the same
    d = dict()
    for field, pairs in zip(fields, zip(f_list, s_list)):
        d[field] = pairs
    count = 0

    #process each field in dictionary
    for k, v in d.items():
        if k == "SSN":
            if measure_ssn_similarity(v[0], v[1], "w") > ssn_level:
                count += 1
        elif k == "MRN":
            if measure_mrn_distance(v[0], v[1]) < mrn_level:
                count += 1
        elif k == "ADDRESS":
            an1 = address_normalization(v[0])
            an2 = address_normalization(v[0])
            if an1 != "" and an1 == an2:
                count += 1
        elif k == "FNAME":
            pass
        elif k == "LNAME":
            pass
        elif k == "PHONE":
            pass
        elif k == "DOB":
            pass



    if count >= multi_filter_threshold:
        return True
    return False
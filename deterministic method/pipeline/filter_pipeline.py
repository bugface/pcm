import csv
from ssn_normalization import measure_ssn_similarity
from mrn_similarity_meaasurement import measure_mrn_distance
from deterministic_rule_pipeline_new_version import pairs2csv, pair2txt


def filter_data_in_csv(csv_file, ssn_level=0.9, mrn_level=250, filter_flag="pos"):
    # filter mainly based on mrn distance and ssn similarity
    # the two filter pramaters can be manully tuned for best performance
    if filter_flag == "pos":
        print("using positive fiter...")
    elif filter_flag == "neg":
        print("using negative fiter...")
    matched_pairs = set()
    f_ssn = ""
    f_mrn = ""
    f_id = ""

    with open(csv_file, "r") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i % 2 == 0:
                f_ssn = row["SSN"]
                f_mrn = row["MRN"]
                f_id = row["ENTERPRISEID"]
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


                # reset f_ssn, f_mrn, f_id
                f_ssn = ""
                f_mrn = ""
                f_id = ""
    print("Totally find {} pairs of records.".format(len(matched_pairs)))
    return list(matched_pairs)


def positive_filter(measured_ssn_level, ssn_level, measured_mrn_level, mrn_level):
    if measured_ssn_level >= ssn_level or (measured_mrn_level >= 0 and measured_mrn_level <= mrn_level):
        return True
    return False


def negative_filter(measured_ssn_level, ssn_level, measured_mrn_level, mrn_level):
    if measured_ssn_level < ssn_level or (measured_mrn_level < 0 or measured_mrn_level > mrn_level):
        return True
    return False

def multiple_fields_filter():
    pass
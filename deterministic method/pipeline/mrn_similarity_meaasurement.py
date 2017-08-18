import jellyfish
import csv
from deterministic_rule_pipeline import create_submission_csv, pairs2csv

def measure_mrn_similarity(ssn1, ssn2, sign):
    if ssn1 == "" or ssn2 == "" or ssn1 is None or ssn2 is None:
        return 0

    r1 = jellyfish.jaro_winkler(ssn1, ssn2)
    r2 = 1 - jellyfish.hamming_distance(ssn1, ssn2)/len(ssn1)


    if sign == "t":
        print("jw-{} vs hd-{}".format(r1, r2))
    elif sign == "w":
        return max(r1, r2)

def process_mrn_from_csv(csv_file, threshold, sign):
    matched_results = []

    with open(csv_file, "r") as f:
        reader = csv.DictReader(f)
        fir_id = ""
        fir_mrn = ""

        for i, each in enumerate(reader):
            if i % 2 == 0:
                fir_id += each["ENTERPRISEID"]
                fir_mrn += each["MRN"]
            else:
                sec_id = each["ENTERPRISEID"]
                sec_mrn = each["MRN"]

                if measure_mrn_similarity(fir_mrn, sec_mrn, sign) > threshold:
                    matched_results.append((fir_id, sec_id))

                fir_id = ""
                fir_mrn = ""

    return matched_results

def output_mrn_similarity_result(data, output_file):
    with open(output_file, "w") as f:
        for each in data:
            output_data = "{}\t{}".format(each[0], each[1])
            print(output_data, file=f, end='\n')

def main():
    threshold = 0.8
    input_file = "process_mrn.csv"
    output_file = "normalized_mrn_pairs.txt"
    output_sub = "sub18.csv"
    sign = "w"

    res = process_mrn_from_csv(input_file, threshold, sign)
    #print(res)
    output_mrn_similarity_result(res, output_file)

    pairs2csv(res, "check_mrn.csv")
    create_submission_csv(output_file, output_sub)

if __name__ == '__main__':
    main()
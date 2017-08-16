import csv
from ssn_normalization import measure_ssn_similarity
from process_dob import check_dob
from process_address_with_normalization import normalize_address
from deterministic_rule_pipeline import pairs2csv, pairs2txt, create_submission_csv

def main():
    csv_file = "der_vs_pro_after_dedupe.csv"

    with open(csv_file, "r") as f:
        reader = csv.DictReader(f)

        matched_results = set()

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
                #print(s_addr)
                # print(f_ssn + " : " + s_ssn)

                if measure_ssn_similarity(f_ssn, s_ssn, "w") > 0.9:
                    matched_results.add((f_id, s_id))

                if check_dob(f_dob, s_dob):
                    matched_results.add((f_id, s_id))

                if f_addr != "" and f_addr == s_addr:
                    matched_results.add((f_id, s_id))

                if f_phone == s_phone and f_phone != "":
                    matched_results.add((f_id, s_id))

                if measure_ssn_similarity(f_mrn, s_mrn, "w") > 0.8:
                    matched_results.add((f_id, s_id))

                # if f_f != s_f:
                #     matched_results.add((f_id, s_id))

                # if f_l != s_l:
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
    # pairs2txt(matched_results ,"pro_vs_der_after_check_pairs.txt")
    # create_submission_csv("pro_vs_der_after_check_pairs.txt", "sub15.csv")
    # pairs2csv(matched_results, "pro_vs_der_after_check_pairs.csv")


if __name__ == '__main__':
    main()
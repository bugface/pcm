import jellyfish
import csv

stop_ssn_list = set(['111-11-1111', '222-22-2222', '000-00-0000', '333-33-3333', '444-44-4444', '555-55-5555',
                '666-66-6666', '777-77-7777', '888-88-8888', '999-99-9999', '012-34-5678', '123-45-6789'])


def match_partial_ssn(ssn1, ssn2):
    if ssn1 == "" or ssn2 == "" or ssn1 in stop_ssn_list or ssn2 in stop_ssn_list:
        return False

    sl1 = ssn1.split("-")
    sl2 = ssn2.split("-")

    if (sl1[0] == sl2[0] and sl1[1] == sl2[1]) or (sl1[2] == sl2[2] and sl1[1] == sl2[1]) or (sl1[0] == sl2[0] and sl1[2] == sl2[2]):
        return True
    return False



def measure_ssn_similarity(ssn1, ssn2, sign):
    if ssn1 == "" or ssn2 == "" or ssn1 is None or ssn2 is None:
        return -1

    #r1 = jellyfish.jaro_winkler(ssn1, ssn2)
    r2 = 1 - jellyfish.hamming_distance(ssn1, ssn2) / len(ssn1)

    if sign == "t":
        # print("jw-{} vs hd-{}".format(r1, r2))
        pass
    elif sign == "w":
        return r2  # max(r1, r2)


def test():
    l = [('853-21-7390', '653-21-7390'), ('853-21-7390', '828-21-7390'), ('853-51-7390', '853-21-7390'),
         ('853-51-7390', '853-21-7380'), ('853-51-7390', '853-51-4391')]

    for each in l:
        measure_ssn_similarity(each[0], each[1], "t")


def process_ssn_from_csv(csv_file, threshold, output_file, sign):
    matched_results = []

    with open(csv_file, "r") as f:
        reader = csv.DictReader(f)
        fir_id = ""
        fir_ssn = ""

        for i, each in enumerate(reader):
            if i % 2 == 0:
                fir_id += each["ENTERPRISEID"]
                fir_ssn += each["SSN"]
            else:
                sec_id = each["ENTERPRISEID"]
                sec_ssn = each["SSN"]

                if measure_ssn_similarity(fir_ssn, sec_ssn, sign) > threshold:
                    matched_results.append((fir_id, sec_id))

                fir_id = ""
                fir_ssn = ""

    return matched_results


def output_ssn_similarity_result(data, output_file):
    with open(output_file, "w") as f:
        for each in data:
            output_data = "{}\t{}".format(each[0], each[1])
            print(output_data, file=f, end='\n')

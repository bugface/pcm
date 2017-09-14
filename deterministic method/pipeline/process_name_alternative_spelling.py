import csv
# from threading import RLock
# from concurrent.futures import ThreadPoolExecutor, wait
# import functools
# from ssn_normalization import measure_ssn_similarity
# from mrn_similarity_meaasurement import measure_mrn_similarity
# import os
# from deterministic_rule_pipeline import pairs2csv, create_submission_csv
import json

# lock = RLock()

def create_alternative_name_dict():
    src = "spellingalternative"
    name_dict = dict()
    with open(src, "r") as f:
        for each in f:
            l = each[:-1].split("\t")
            key = l[0]
            value = l[1]
            if key in name_dict:
                name_dict[key].append(value)
            else:
                name_dict[key] = [value]

    return name_dict


def single_task_setup(input_file, output_file, alternative_spelling_dict):
    f_id = ""
    ff = ""
    ff_list = []
    fl = ""
    fl_list = []
    f_ssn = ""
    f_mrn = ""
    matched_result = set()

    with open(input_file, "r") as f:
        reader = csv.DictReader(f)
        t = None
        for i, each in enumerate(reader):
            if i % 2 == 0:
                f_id += each["ENTERPRISEID"]
                ff += each["FIRST_"]
                ff += each["LAST_"]
                f_ssn += each["SSN"]
                f_mrn += each["MRN"]
            else:
                s_id = each["ENTERPRISEID"]
                sf = each["FIRST_"]
                sl = each["LAST_"]
                s_ssn = each["SSN"]
                s_mrn = each["MRN"]

                if ff in alternative_spelling_dict:
                    ff_list = alternative_spelling_dict[ff][:]

                if sf in ff_list:
                    matched_result.add((f_id, s_id))

                if fl in alternative_spelling_dict:
                    fl_list = alternative_spelling_dict[fl][:]

                if sl in fl_list or (fl == sl and fl != ""):
                    matched_result.add((f_id, s_id))

                if measure_ssn_similarity(f_ssn, s_ssn, "w") > 0.9:
                    matched_result.add((f_id, s_id))

                if measure_mrn_similarity(f_mrn, s_mrn, "w") > 0.8:
                    matched_result.add((f_id, s_id))

                ff = ""
                ff_list = []
                fl = ""
                fl_list = []
                f_id = ""
                f_ssn = ""
                f_mrn = ""
                t = i

    print(t)
    print(len(matched_result))

    # with open(output_file, "w") as f:
    #     for each in matched_result:
    #         print("{}\t{}".format(each[0], each[1]), file=f, end='\n')


def output_matched_result(future, output_file):
    pairs = future.result()

    if os.path.isfile(output_file):
        mode = "a"
    else:
        mode = "w"
    lock.acquire()
    try:
        with open(output_file, mode) as f:
            for pair in pairs:
                print("{}\t{}".format(pair[0], pair[1]), file=f, end='\n')
    finally:
        lock.release()


def process_alternative_name(alternative_spelling_dict, csv_input_file, se):
    f_id = ""
    ff = ""
    f_ssn = ""
    f_mrn = ""
    matched_result = set()
    start = se[0]
    end = se[1]
    print(se)

    with open(csv_input_file, "r") as f:
        reader = csv.DictReader(f)
        for i, each in enumerate(reader):
            if i >= start and i < end:
                if i % 2 == 0:
                    f_id += each["ENTERPRISEID"]
                    ff += each["FIRST_"]
                    f_ssn += each["SSN"]
                    f_mrn += each["MRN"]
                else:
                    s_id = each["ENTERPRISEID"]
                    sf = each["FIRST_"]
                    s_ssn = each["SSN"]
                    s_mrn = each["MRN"]

                    if ff in alternative_spelling_dict:
                        ff = alternative_spelling_dict[ff].append(ff)

                    if ff == sf and ff != "":
                        matched_result.add((f_id, s_id))

                    if measure_ssn_similarity(f_ssn, s_ssn, "w") > 0.9:
                        matched_result.add((f_id, s_id))

                    if measure_mrn_similarity(f_mrn, s_mrn, "w") > 0.8:
                        matched_result.add((f_id, s_id))

                    ff = ""
                    f_id = ""
                    f_ssn = ""
                    f_mrn = ""
            elif i == end:
                break

    return matched_result


def bufcount(filename):
    f = open(filename)
    lines = 0
    buf_size = 1024 * 1024
    read_f = f.read  # loop optimization

    buf = read_f(buf_size)
    while buf:
        lines += buf.count('\n')
        buf = read_f(buf_size)

    return lines


def multi_task_setup(input_file, output_file, tasks, alternative_spelling_dict):
    futures_ = []
    with ThreadPoolExecutor(max_workers=4) as excutor:
        for task in tasks:
            future_ = excutor.submit(
                process_alternative_name, alternative_spelling_dict=alternative_spelling_dict, csv_input_file=input_file, se=task)
            future_.add_done_callback(functools.partial(
                output_matched_result, output_file=output_file))
            futures_.append(future_)
        wait(futures_)


def main():
    #input_file = "process_alter_last.csv"
    #output_file = "processed_alter_last_diff_v2.txt"
    alternative_spelling_dict = create_alternative_name_dict()
    with open('data.json', 'w') as outfile:
        json.dump(alternative_spelling_dict, outfile)
    # single thread is way faster than multithreading
    #single_task_setup(input_file, output_file, alternative_spelling_dict)

    # n = bufcount(input_file) - 1
    # tasks = []
    # # a = n // 4
    # # b = n // 2
    # # c = n // 4 * 3
    # # tasks = [(0, a), (a, b), (b, c), (c, n)]
    # for j in range(1, 65):
    #     task = (n*((j-1)/64), n*(j/64))
    #     tasks.append(task)

    # multi_task_setup(input_file, output_file, tasks, alternative_spelling_dict)

    # pairs = set()
    # with open(output_file, "r") as f:
    #     for each in f:
    #         t = each[:-1].split("\t")
    #         pairs.add((int(t[0]), int(t[1])))
    # print(len(pairs))
    # pairs2csv(pairs, "processed_alter_last_diff.csv")

    #create_submission_csv(output_file, "sub23diffb.csv")


if __name__ == '__main__':
    main()

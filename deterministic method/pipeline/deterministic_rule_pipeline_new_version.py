from sqlalchemy import create_engine
import csv
import logging
import glob
import os
#from names_normalization import main as name_norm_main
from multiprocessing import Process, Pool, cpu_count
import concurrent.futures
import functools
import threading
#from multiprocessing import pool

SQLALCHEMY_DATABASE_URI = "oracle://alexgre:alex1988@temp1.clx2hx01phun.us-east-1.rds.amazonaws.com/ORCL"
FORMAT = '%(asctime)-20s %(name)-5s %(levelname)-10s %(message)s'
logging.basicConfig(filename='rules_log.log', level=logging.INFO,
                    format=FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("task")
title = ['ENTERPRISEID', 'LAST_', 'FIRST_', 'MIDDLE', 'SUFFIX_', 'DOB', 'GENDER', 'SSN', 'ADDRESS1',
         'ADDRESS2', 'ZIP', 'MOTHERS_MAIDEN_NAME', 'MRN', 'CITY', 'STATE_', 'PHONE', 'PHONE2', 'EMAIL', 'ALIAS_']
engine = create_engine(SQLALCHEMY_DATABASE_URI, pool_size=4, pool_recycle=3600)
helper_title = ['ZIP', 'FIRST_', 'CITY', 'DOB', 'ADDRESS2', 'ADDRESS1', 'GENDER', 'ENTERPRISEID',
                'MIDDLE', 'LAST_', 'SUFFIX_', 'MOTHERS_MADIDEN_NAME', 'MRN', 'STATE_', 'PHONE',
                'PHONE2', 'EMAIL', 'ALIAS_', 'SSN']
process_num = cpu_count()
#process_num = 8
lock = threading.RLock()


def extract_pairs_from_txt(txt_file):
    pairs = []
    with open(txt_file, "r") as f:
        for each in f:
            pairs.append(tuple(each[:-1].split()))
    return pairs


def create_submission_csv(txt_file, csv_file):
    with open(txt_file, "r") as f:
        with open(csv_file, "w", newline='') as f1:
            writer = csv.writer(f1)
            for each in f:
                l = []
                data = each[:-1].split('\t')
                l.append(data[0])
                l.append(data[1])
                l.append(1)
                writer.writerow(l)


def execute_sql(rule):
    with engine.begin() as conn:
        res = conn.execute(rule)
    return res


def get_rules(file):
    d = dict()
    with open(file, 'r') as f:
        for line in f:
            rule = line[:-1].split("@")
            rule_title = rule[0]
            rule_sql = rule[1]
            d[rule_title] = rule_sql
    return d


def pair2txt(file, data):
    print("write out to file: {}".format(file))
    with open(file, "w") as f:
        for each in data:
            output = "{}\t{}".format(each[0], each[1])
            print(output, file=f, end='\n')


def pair2txt_multi(future, file):
    data = future.result()
    lock.acquire()
    try:
        with open(file, "w") as f:
            for each in data:
                output = "{}\t{}".format(each[0], each[1])
                print(output, file=f, end='\n')
    finally:
        lock.release()


def store_result_csv_job(future, output_csv_file):
    results = future.result()
    lock.acquire()
    try:
        with open(output_csv_file, "a", newline='') as f:
            writer = csv.writer(f)
            for result in results:
                line1 = result[: 19]
                line2 = result[19:]
                writer.writerow(line1)
                writer.writerow(line2)
    finally:
        lock.release()


def store_result_job(job, rule, sql, file):
    res = execute_sql(sql)
    #data = []
    s = set()
    detail = []

    if job == "p" or job == "n":
        for each_res in res:
            t = (each_res[7], each_res[26])
            tprim = (each_res[26], each_res[7])
            if t not in s and tprim not in s:
                s.add(t)
                # data.append(t)
    elif job == "d":
        for each_res in res:
            t = (each_res[7], each_res[26])
            tprim = (each_res[26], each_res[7])
            if t not in s and tprim not in s:
                s.add(t)
                # data.append(t)
                detail.append(each_res)

    logger.info("rule: {}; pairs number: {}".format(file, len(s)))
    res.close()

    if job == "p":
        return s
    elif job == "d":
        return detail
    elif job == "n":
        pairs2txt(file, s)


def store_result_as_pairs(rules, folder, job, rule_file):
    futures_ = []
    output_file = None

    if job == "d":
        output_file = rule_file.split(".")[0] + "_all_in_one_csv.csv"
        with open(output_csv_file, "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(helper_title)
    # elif job == "p":
    #     output_file = rule_file.split(".")[0] + "_all_in_one_txt.txt"
    #     with open(output_file, "w") as f:
    #         pass

    # with  concurrent.futures.ProcessPoolExecutor(max_workers=process_num) as excutor:
    with concurrent.futures.ThreadPoolExecutor(max_workers=process_num) as excutor:
        for rule, sql in rules.items():
            file = folder + "\\" + rule + ".txt"
            try:
                future_ = excutor.submit(
                    store_result_job, job=job, rule=rule, sql=sql, file=file)
                if job == "d":
                    future_.add_done_callback(functools.partial(
                        store_result_csv_job, output_csv_file=output_file))
                elif job == "p":
                    future_.add_done_callback(
                        functools.partial(pair2txt_multi, file=file))
                elif job == "n":
                    pass
                futures_.append(future_)
            except Exception as e:
                logger.error(e)

        concurrent.futures.wait(futures_)
        # executor.shutdown()

    # another way to implement
    # pool = Pool(processes=process_num)
    # for rule, sql in rules.items():
    #   pool.apply_async(store_result_job, args=(rule, sql))
    # pool.close()
    # pool.join()


def _dedupe(file, dataset):
    with open(file, "r") as f:
        for each in f:
            data = each[:-1].split('\t')
            # print(data)
            t = (data[0], data[1])
            tprim = (data[1], data[0])
            if t not in dataset and tprim not in dataset:
                dataset.add(t)


def combine_pair_files_with_dedupe(base_file, new_files, output_file):
    pairs = set()
    _dedupe(base_file, pairs)

    for file in new_files:
        _dedupe(file, pairs)

    with open(output_file, "w") as f:
        for each in pairs:
            output = "{}\t{}".format(each[0], each[1])
            print(output, file=f, end='\n')


def get_extra_pairs_not_in_base(base, files):
    pairs = set()
    diff_pairs = set()
    _dedupe(base, pairs)

    for file in files:
        with open(file, "r") as f:
            for each in f:
                data = each[:-1].split('\t')
                t = (data[0], data[1])
                tprim = (data[1], data[0])
                if t not in pairs and tprim not in pairs:
                    pairs.add(t)
                    diff_pairs.add(t)
    return diff_pairs


def pair2csv_helper_output(future, index, output_file):
    results = future.result()

    lock.acquire()
    try:
        row1 = [index] + list(results[0])
        row2 = [index] + list(results[1])
        with open(output_file, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(row1)
            writer.writerow(row2)
    finally:
        lock.release()


def pair2csv_helper_query(pair):
    pair1 = int(pair[0])
    pair2 = int(pair[1])
    sql = '''select * from
             (select * from pcm where enterpriseid={})
             union
             (select * from pcm where ENTERPRISEID={})
        '''.format(pair1, pair2)

    l = []

    with engine.begin() as conn:
        res = conn.execute(sql)
        for each in res:
            l.append(each)
        res.close()

    return l


def pairs2csv(pairs, output_file):
    helper_title.insert(0, "index")
    #engine = create_engine(SQLALCHEMY_DATABASE_URI, pool_size=4, pool_recycle=3600)
    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(helper_title)
        helper_title.pop(0)

    futures_ = []
    pairs_for_query = []
    # might not need multithreading which suspect to lower the efficiency because if threads exchange cost
    with concurrent.futures.ThreadPoolExecutor(max_workers=process_num) as executor:
        for i, pair in enumerate(pairs):
            future_ = executor.submit(pair2csv_helper_query, pair=pair)
            future_.add_done_callback(functools.partial(
                pair2csv_helper_output, index=i, output_file=output_file))
            futures_.append(future_)

        concurrent.futures.wait(futures_)

    # with open(output_file, "w", newline='') as f:
    #     writer = csv.writer(f)
    #     writer.writerow(helper_title)
    #     title.pop(0)
    #     for i, pair in enumerate(pairs):
    #         l = []
    #         pair1 = int(pair[0])
    #         pair2 = int(pair[1])
    #         sql = '''select * from
    #                  (select * from pcm where enterpriseid={})
    #                  union
    #                  (select * from pcm  where ENTERPRISEID={})
    #             '''.format(pair1, pair2)

    #         with engine.begin() as conn:
    #             res = conn.execute(sql)
    #             for each in res:
    #                 l.append(each)
    #             res.close()
    #         for each in l:
    #             writer.writerow(each[:19])
    #             writer.writerow(each[19:])

#stage 1
def generate_individual_pairs_file_from_rules(rule_file, rules, folder, job):
    #only generate indivudual pairs based on rules input
    print("read rules...")
    rules = get_rules(rule_file)
    print("collect pairs based on rules...")
    store_result_as_pairs(rules, folder, job, rule_file)

#stage 2
def output_deduped_pairs_and_detail(folder, base_file, output_pair_file, output_csv_file):
    #collect all the individual pairs file and dedupe all the resulted pairs based on base_file and output results
    print("combine all the pairs and perform deduplication...")
    new_pair_files = glob.glob(folder + "\\" + "*.txt")
    #combine_pair_files_with_dedupe(base_file, new_pair_files, output_pair_file)
    extra_pairs = get_extra_pairs_not_in_base(base_file, new_pair_files)
    print(len(extra_pairs))
    print("output results...")
    pair2txt(output_pair_file, extra_pairs)
    pairs2csv(extra_pairs, output_csv_file)
    print("done")

def pipline_get_detail(rule_file, folder, base_file, output_csv_file, output_pair_file, job):
    #pipeline combine stage 1 and stage 2
    generate_individual_pairs_file_from_rules(rule_file, rules, folder, job)
    output_deduped_pairs_and_detail(folder, base_file, output_pair_file, output_csv_file)



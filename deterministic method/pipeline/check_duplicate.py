import csv
from deterministic_rule_pipeline_new_version import create_submission_csv, pairs2csv, extract_pairs_from_txt


def check_dupe(file1, file2, file3):
    s1 = set()
    s2 = set()
    with open(file1, "r") as f:
        for each in f:
            t = each[:-1].split("\t")
            t1 = t[0]
            t2 = t[1]
            s1.add((t1, t2))

    with open(file2, "r") as f:
        for each in f:
            t = each[:-1].split("\t")
            t1 = t[0]
            t2 = t[1]
            s2.add((t1, t2))

    # for each in s1:
    #     if each not in s2:
    #         print(each)

    # remove shared pairs and output into a new file
    l = []
    for each in s1:
        each_p = (each[1], each[0])
        if not each in s2 and not each_p in s2:
            l.append(each)

    print(len(l))

    with open(file3, "w") as f:
        for each in l:
            print("{}\t{}".format(each[0], each[1]), file=f, end='\n')

    return l

def main():
    # p = check_dupe("todo/filtered_56626_neg_1.txt",
    #                "todo/neg_lastest.txt", "todo/56626_neg.txt")
    # pairs2csv(p, "todo/56626_neg.csv")
    check_dupe("process_new_full_cover.txt", "processed_full_cover.txt", "process_new_full_cover_remove_dupes.txt")


if __name__ == '__main__':
    main()

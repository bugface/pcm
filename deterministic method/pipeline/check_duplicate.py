import csv
from deterministic_rule_pipeline_new_version import create_submission_csv, pairs2csv, extract_pairs_from_txt


def check_dupe(file1, file2, file3):
    s = set()
    l = []
    with open(file1, "r") as f:
        for each in f:
            t = each[:-1].split("\t")
            t1 = t[0]
            t2 = t[1]
            s.add((t1, t2))

    with open(file2, "r") as f:
        count = 0
        for each in f:
            t = each[:-1].split("\t")
            t1 = t[0]
            t2 = t[1]

            tp1 = (t1, t2)
            tp2 = (t2, t1)
            if tp1 in s or tp2 in s:
                count += 1
            else:
                l.append(tp1)
        print(count, " pairs are overlaped.")

    with open(file3, "w") as f:
        for each in l:
            print("{}\t{}".format(each[0], each[1]), file=f, end='\n')

    return l


def csv2txt(csv_file, txt_file):
    with open(txt_file, "w") as f1:
        with open(csv_file, "r") as f:
            reader = csv.reader(f)
            for each in reader:
                line = "{}\t{}".format(each[0], each[1])
                print(line, file=f1, end='\n')


def main():
    #csv2txt("submission2.csv", "submission2.txt")
    #pairs = check_dupe("56626.txt", "process_full_cover.txt", "processed_full_cover.txt")
    create_submission_csv("processed_full_cover.txt", "processed_full_cover_sub.csv")
    pairs2csv(extract_pairs_from_txt("processed_full_cover.txt"), "processed_full_cover_detail.csv")

if __name__ == '__main__':
    main()

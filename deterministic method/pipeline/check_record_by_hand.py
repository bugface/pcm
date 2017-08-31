import os
import sys
import csv


def clsw():
    os.system('cls' if os.name == 'nt' else 'clear')


def check_csv_by_hand(csv_file, title):
    with open('same_pair_from_diff_csv.txt', "w") as fs, open('diff_pair_from_diff_csv.txt', "w") as fd:
        with open(csv_file, "r") as f:
            reader = csv.reader(f)
            records = []
            for i, row in enumerate(reader):
                if i != 0:
                    # print(row)
                    records.append(row)

            j = 0
            for i in range(0, len(records), 2):
                l = []
                for each in records[i]:
                    l.append(each)
                for each in records[i + 1]:
                    l.append(each)
                if l[8] == l[28] and l[8].strip() != '':
                    print(l[8])
                    output = "{}\t{}".format(records[i][1], records[i + 1][1])
                    print(output, file=fs, end='\n')
                else:
                    j += 1
                    for k in range(len(records[i])):
                        if k in [2, 3, 4, 6, 7, 8, 9, 10, 11, 16, 17, 18, 19]:
                            print("{}\t\t{}\t\t\t\t\t{}".format(
                                title[k - 1], l[k], l[k + 20]))
                    decision = input("same or not (y/n):")
                    if decision in ['y', 'Y']:
                        output = "{}\t{}".format(
                            records[i][1], records[i + 1][1])
                        print(output, file=fs, end='\n')

                    if decision in ['n', 'N']:
                        output = "{}\t{}".format(
                            records[i][1], records[i + 1][1])
                        print(output, file=fd, end='\n')
                        print()
                        print()
                    if j % 2 == 0:
                        j = 0
                        clsw()


def main():
    title = ['ENTERPRISEID', 'LAST_', 'FIRST_', 'MIDDLE', 'SUFFIX_', 'DOB', 'GENDER', 'SSN', 'ADDRESS1',
             'ADDRESS2', 'ZIP', 'MOTHERS_MAIDEN_NAME', 'MRN', 'CITY', 'STATE_', 'PHONE', 'PHONE2', 'EMAIL', 'ALIAS_']
    args = sys.argv[1]
    check_csv_by_hand(args, title)


if __name__ == '__main__':
    main()

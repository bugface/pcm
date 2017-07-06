#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import logging
import logging.handlers
import jellyfish
from multiprocessing import cpu_count
import concurrent.futures
import functools
import time

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s]: %(levelname)s: %(message)s')
path_1 = '../processed_dataset/processed_dataset_test_1.csv'
path_2 = '../processed_dataset/processed_dataset_test_2.csv'
'''
def eng2vec(word):
    word = word.lower()
    vector = [0] * 26
    for character in word:
        vector[ord(character) - 97] += 1
    return vector

#cos_distance reference: http://blog.csdn.net/sscssz/article/details/51907121
def cos_distance(str_1,str_2):
    vector1 = eng2vec(str_1)
    vector2 = eng2vec(str_2)
    dot_product = 0.0
    normA = 0.0
    normB = 0.0
    for a,b in zip(vector1,vector2):
        dot_product += a*b
        normA += a**2
        normB += b**2
    if normA == 0.0 or normB==0.0:
        return None
    else:
        return dot_product / ((normA*normB)**0.5)

#code reference: http://blog.csdn.net/wateryouyo/article/details/50917812
def lcsubstr_distance(s1, s2): 
    m=[[0 for i in range(len(s2)+1)]  for j in range(len(s1)+1)]  #生成0矩阵，为方便后续计算，比字符串长度多了一列
    mmax=0   #最长匹配的长度
    p=0  #最长匹配对应在s1中的最后一位
    for i in range(len(s1)):
        for j in range(len(s2)):
            if s1[i]==s2[j]:
                m[i+1][j+1]=m[i][j]+1
                if m[i+1][j+1]>mmax:
                    mmax=m[i+1][j+1]
                    p=i+1
    return float(mmax)/float(max(len(s1),len(s2)))   #返回最长子串长度

print find_lcsubstr('abcdfg','abdfg')
'''

def write_distance2csv(column1_name, column2_name):
    process = 0
    process_sub = 0
    total = 100
#    distance_info = []
    with open(path_1) as f1, open(column1_name + column2_name + '_similarity.csv', 'w+', newline='') as csv_file:
        headers = ['id_1', 'id_2', column1_name + '_similarity', column2_name + '_similarity']
        writer = csv.writer(csv_file)
        writer.writerow(headers)
        reader1 = csv.DictReader(f1)
        for id_1 in reader1:
            if process >= 100:
                break
#                logger.info(id_1)
            process += 1
            with open(path_2) as f2:
                reader2 = csv.DictReader(f2)
                process_sub = 0
                for id_2 in reader2:
    #                    logger.info(id_1)
                    process_sub += 1
                    logger.info('processing: ' + str(process) + '/' + str(total) + '__' + str(process_sub) + '/' + str(total))
                    logger.info(id_1['EnterpriseID'])
                    logger.info(len(id_2['EnterpriseID']))
                    if len(id_2['EnterpriseID']) == 0:
                        break
                    elif id_1['EnterpriseID'] >= id_2['EnterpriseID']:
                        continue
                    else:
                        try:
                            writer.writerow([str(id_1['EnterpriseID']), str(id_2['EnterpriseID']),
                            str(jellyfish.hamming_distance(id_1[column1_name], id_2[column1_name])), str(jellyfish.hamming_distance(id_1[column2_name], id_2[column2_name]))])
                        except Exception as e:
                            logger.info(Exception, ': ', e)


'''
    with open(column_name + '_distance.csv', 'w+', newline='') as csv_file:
        process = 0
        headers = ['id_1', 'id_2', column_name + '_distance']
        writer = csv.DictWriter(csv_file, fieldnames=headers)
        writer.writeheader() 
        for row in distance_info:
            process += 1
            logger.info('writing: ' + str(process))
            try:
                writer.writerow(row)
            except Exception:
                logger.info(Exception)
'''


'''
#levenshtein code reference: http://blog.csdn.net/haichao062/article/details/8079748
def levenshtein(a,b):
    "Calculates the Levenshtein distance between a and b."
    n, m = len(a), len(b)
    if n > m:
        # Make sure n <= m, to use O(min(n,m)) space
        a,b = b,a
        n,m = m,n
    current = range(n+1)
    for i in range(1,m+1):
        previous, current = current, [i]+[0]*n
        for j in range(1,n+1):
            add, delete = previous[j]+1, current[j-1]+1
            change = previous[j-1]
            if a[j-1] != b[i-1]:
                change = change + 1
            current[j] = min(add, delete, change)
    return current[n]
'''
if __name__ == '__main__':
    st = time.time()
    write_distance2csv('LAST_', 'FIRST_')
    logger.info("COMPLETED in [%.2fs]"%(time.time() - st))
#    print(levenshtein('eeba', 'abac'))
#    print(levenshtein('hahaha','hahaha'))
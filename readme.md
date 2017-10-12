# This is a project of Patient Matching Algorithm Challenge
##### more information: https://www.patientmatchingchallenge.com/

> The current work is using deterministic method which the most of the work are in deterministic method\pipeline
> All the previous submissions are in folder for submission
> The database is oracle with table name as pcm and pcm_update. The database is hosted at amazon aws with SQLALCHEMY_DATABASE_URI = "oracle://alexgre:alex1988@temp1.clx2hx01phun.us-east-1.rds.amazonaws.com/ORCL"
> All the results are list below

## dedupe
1. use csvdedupe_modified.py obtained a submission result as submission1 -> precision=0.413 recall=0.3
2. seperate 3 records into two or three pairs -> obtain submission2 -> precision=0.84 recall 0.65
<br><br>
#### *************************************************************************************************************

## Deterministic Experiments:
### Version 1:
1. 	- strategy: query db for same f+l+dob+ssn+gender
	- txt files: f_l_dob_ssn_g.txt (total: 10950)
	- submission file: per1.csv
	- result: precision=1 and recall=0.19
<br><br>
2.	- strategy: query db for same f+l+dob+gender
	- txt files: f_l_dob_g.txt (total: 33474) (this dataset contains all the data from stgy1)
	- submission file: per2.csv
	- result: precision=0.9997 and recall=0.58
<br><br>
3.	- strategy: query db for same f+l+dob+ssn and query db for same f+l+gender+ssn, union the two query results
	- txt files: f_l_ssn_dob.txt (total: 13960), f_l_ssn_g.txt (total: 13235), union_stgy3.txt(total: 16011)
	- submission file: sub5.csv (this dataset covers all the data from stgy1)
	- result: precision=1 and recall=0.27854
<br><br>
4.  - strategy: union the dataset(f_l_dob_g.txt) from stgy2 and the dataset(union_stgy3.txt) from stgy3
	- txt files: union_stgy4.txt(total: 38635)
	- submission: sub3.csv
	- result: precision=0.9997 and recall=0.67 (12 error pairs)
<br><br>
5.  - strategy: query db for same f+l+address1 and union with dataset(union_stgy4.txt) from stgy4
	- txt files: union_stgy5.txt (total: 40281)
	- submission: sub4.csv
	- result: precision=0.997 and recall=0.71
<br><br>
6.  - strategy: query db for same f+l
	- txt files: f_l.txt (total: 740281)
<br><br>
7.  - strategy: query db using rules combined with exp5(stgy5) (deduped)
	- txt files: stgy7.txt (total: 48388)
	- submission: sub7.csv
	- result: precision=0.99657 and recall=0.839 (f-score is 0.91)
<br><br>
8.  - strategy: query db using all rules, filter all the distinct pairs not contained in experiment 4
	- txt files: combined.txt (total: 9853)
	- submission: sub8.csv
	- result: precision=0.984 and recall=0.17 (has around 158 error pairs, will be checked by hand in diffpair.csv)
<br><br>
9.  - strategy: collect a set of rules in rules_dtail_process_address.txt which used only three fields. The query result based on these rules will be combined and deduped with stgy7. The resulted data pairs will be checked again by the new address rule which will compare the similarity based on normalized address
	- txt files: addr_to_process_final_pairs2.txt (total: 1112)
	- submission: sub9.csv
	- result: percision=0.993 => 8 out of 1112 are not true
<br><br>
10. - strategy: collect a set of rules into rule_detail_first.txt and rule_detail_last.txt which contained only three fields. The query results based on these rules will be combined and deduped with stgy7, respectively. The resutled data pairs will be checked again by the new name normalization rule which will compare the similarity of two names based on name normalization algorithms (string similarity and phonetic)
	- txt files: process_first_name_222.txt (2971) and process_last_name_222.txt (843)
	- submission sub10.csv and sub11.csv
	- result 10: percision=0.9623 (112 of 2971 are not correct)
	- result 11: percision=0.9525 (32 of 843 are not correct)
<br><br>
11. - strategy: collect a set of rules into rule_detail_dob.txt which contains three fields (not dob). The query results based on these rules deduped with previous obtained results. The resulted pairs were processed with dob normailization strategy (considering year, month, day independently).
	- txt file: processed_dob.txt (533 pairs)
	- submission: sub12.csv
	- result 12: percision=0.861 (74 of 533 are not correct)
	- after add result 12, the recall currently is 0.916 (based on 57482)
<br><br>
12. - strategy: collect a set of rules into rule_detail_ssn.txt which contains three fields (not ssn). The query results based on these rules deduped with previous obtained results. The resulted pairs were processed with ssn normalization strategy (jaro-winkler similarity).
	- txt file: normalized_ssn_pairs.txt (646 pairs)
	- submission: sub13.csv
	- result 12: percision=0.983 (11 of 646 are not correct)
	- after add result 12, the recall currently is 0.927 (based on 57482 as total (53713 found, 53310 correct matched))
<br><br>
13. - Combining all current data together (no error check yet) for submission
	- submission: sub14.csv
	- result: recall=0.927; percision=0.992, f-score=0.9587
<br><br>
14. - strategy: deduplication the results from using dedupe with deterministic methods, obtain the dataset that in dedupe but not in deterministic methods, submit this dataset for check
	- txt file: der_vs_pro.txt (8010 pairs)
	- submission: sub15.csv
	- result 14a: percision=0.082 (657 of 8010 are corrected)
	- using previous implemented ssn, dob, address normalization methods process the 8010 pairs obtained 689 pairs left. All the data are collected for submission
	- submission: sub16.csv
	- result 14b: percision=0.81 (558 of 689 are corrected)
	- using previous implemented ssn, dob, address normalization methods and mrn normalization process the 8010 pairs obtained 780 pairs left. All the data are collected for submission
	- submission: sub17.csv
	- result 14c: percision=0.823 (642 of 780 are corrected) (only 15 paris are missing) (the detailed pairs record are stored in pro_vs_der_after_check_pairs.csv)
<br><br>
15. - strategy: collect a set of rules into rule_detail_mrn.txt which contains three fields (not mrn). The query results based on these rules deduped with previous obtained results. The resulted pairs were processed with mrn similarity strategy.
	- txt file: normalized_mrn_pairs.txt (944 pairs)
	- submission: sub18.csv
	- result 15: percision=0.983 (16 of 944 are not correct)
	- after add result 15, the recall currently is 0.9547 (based on 57489 as total (55437 found, 54880 correct matched))
<br><br>
16. - Combining all current data together (no error check yet) for submission
	- submission: sub19.csv
	- result: recall=0.9546; percision=0.99, f-score=0.972
<br><br>
17. - strategy: collect a set of rules into rule_detail_alternative.txt which contains three fields (not first). The query results based on these rules deduped with previous obtained results. The resulted pairs were processed with alternative strategy.
	- txt file: processed_alternative.txt (3419 pairs)
	- submission: sub20.csv
	- result 17: percision=0.183 (625 of 3419 are correct) (need to improve this methods)
	- to imporve percision, quick_check_csv.py used
	- submission sub21b.csv
	- result: perision=0.834, recall no change (625 of 749 are correct)
	- result: after revision, another 83 are removed
	- result: percision=0.99 (625 of 631 are correct)
<br><br>
18. - Combining all current data together (no error check yet) for submission
	- submission: sub22.csv
	- result: recall=0.9655; percision=0.9898, f-score=0.9775 (from pcm)
	- data: (56068 total find, 55505 correct matched)
<br><br>
19. - strategy: collect a set of rules into rule_detail_alter_last.txt which contains three fields (not last). The query results based on these rules deduped with previous obtained results. The resulted pairs were processed with alternative strategy. The total raw data number collected from the database are 363873. After filter, there are 893 pairs left. Submit the raw data got a feed back that comparing to the filtered data, only 26 pairs are missing. we proceed without worring about these pairs.
	- txt file: processed_alter_last.txt (893 pairs)
	- submission: sub23.csv
	- result 17: percision=0.5789 (517 of 893 are correct)
	- revision: manuelly check records to imporve the percision
	- sumission: sub23a.csv -> 21 are correct from 381
	- submission: sub23_revised_1 -> 2 correct are lost and 20 are incorrect still remain
	- manully removed some other dupes -> left 520 records
<br><br>
20. - Combining all current data together (no error check yet) for submission
	- submission: sub24.csv
	- result: recall=0.9744; percision=0.9898, f-score=0.982 (from pcm)
	- data: based on 57489 as total -> 56588 total find and 56013 correct matched
<br><br>
21. - using all the 4-fields-rule that not used before and all the 3-fields rules
	- totally obtained 337497 pairs
	- submission: sub25_raw.csv
	- data: based on pcm -> 232 are correct pairs
	- revision: filter_pipeline
<br><br>
22. - using the 56588 data set apply the negative filter (ssn similarity < 0.9 and mrn distance is larger than 400) to obtain a negative dataset for false pairs check
    - all the filtered pairs are stored in the 56588_neg.txt (7344 pairs)
    - related detail of each records are stored in the 56588_neg.csv
    - submission: sub26_neg.csv
    - result: percision: 0.4522 (3321 are correct which means rest are not (does not make sense at all))
<br><br>

## PCM Answer Key Update
### after update our previous result changed to p=0.996, r=0.91, f=0.951
##### after update the current data set only contains 217 pairs of mismatched records; the total answer key contains 61874 pairs of records
#### start version 2
1.  - result for submission of processed_neg_56588: percision: 0.435; total: 154 (67); number need to filter off: 87
    - result for submission of processed_full_cover: percision: 0.8456; total: 149(126); number need to filter off:19
    - the pairs need to be filtered have been targeted. The results from above are merged with previous result to yield a new file: 56626.txt
    - the submission 27 is created based on 56626.txt yield result: p=0.996 , r=0.911 , f=0.952 (from pcm)
<br><br>
2. - using all the 4-fields-rule that not used before and all the 3-fields rules
	- totally obtained 337373 pairs
	- submission: processed_full_cover_sub.csv
	- data: based on pcm ->  1834 are correct pairs
	- using filter_pipeline to extract these corrected pairs
	- result: extracted 1881 pairs, submit to pcm as sub28.csv, get feed back p=0.112174375332 (211 are corrected)
<br><br>
3. 	- using negative filter pipeline on 56656.txt in order to obtain the error pairs
	- after filter totally ontained 3700 pairs in which 135 pairs are error pairs (result from pmc check)
	- next step: use postive filter or multi-field filter to shrink the file in order for human check or find rules that detaily filter the negative pairs
	- result1: using positive filter, obtained 1849 pairs, only 1 of them are error pairs (files as neg_latest.txt and neg_latest.csv)
	- we can find that 1 pair and remove the left 1848 pairs from the previous filtered results, so that we only need to find 134 pairs from 1852 pairs
<br><br>
4. - using combined rule f+l+dob+addr+m filter the 330000 pairs generate a dataset merged with privious filtered results generated -> filtered_full_cover_comnined_deduped.txt
   - the new combined dataset has 5605 (p=0.0776) total pairs and 435 pairs of corrected pairs
   - exp4a refer to apply methods on whole data set, exp4b refer to methods on combined dataset
<br><br>
5. - from exp4b, 28 pairs are extracted by using f + dob + l and 22 are corrected and all the corrected pairs are selected and added to the latest result set latest.txt
<br><br>
6. - from exp4a, apply feature: same unempty dob and diff of two mrn is positive and the diff/max(mrn1, mrn2) is smaller than 0.005
   - result: 105 pairs are obtained with 23 are correct (check_now_dob.txt)
   - modify feature to diff/max(mrn1, mrn2) is smaller than 0.0012, only 26 are selected, however only 12 of them are correct.
   - exp6a: apply the method3 from exp8 -> 55 pairs got with p=0
   - exp6d: apply the method4 from exp8 -> 15 pairs got with p=0
   - exp6e: apply the method5 from exp8 -> 73 pairs got with p=0.01667
<br><br>
7. - New database table pmac used. create a new rules file: newrules_detail_full_cover.txt in which the address1 is replaced by address and dob is replace by year+month and month+day. perform the deterministic rule pipline on the new rule sets get total: 771842 pairs of data. Deduped with the data from exp2, the number of left distinct pairs is 7200000. Based on pmc judge r=0.02578, it confirms that another about 1500 pairs are targeted.
<br><br>
8. - task: extract correct pairs from the resulted dataset of exp7
   - **method1**: using same ssn feature (ssn must be valid not like 33333333 or 0123456789)
   - result: get 22 pairs (exp8a.txt) p=1
   - **method2**: f_f == s_f and f_f != "" and (s_l == "" or f_l == "") and check_dob(s_dob, f_dob) and f_addr == s_addr
   - result: get 60 pairs p=0.0167
   - **method3**: mm > 0 and (mm / max(int(f_mrn), int(s_mrn))) <= 0.005 and (f_f == s_f and f_f != "" and f_l == s_l and f_l != "") (mm is mrn distance diff)
   - result: get 260 pairs (exp8b.txt) p=0.973
   - **method4**: ssn xxx-xx-xxxx => [xxx,xx,xxxx]; 2 of 3 are matched
   - result: get 118 pairs (exp8d.txt) p=0.457
   - **method5**: same meaningful phone number (not 333-333-3333)
   - result: get 6 pairs p=1
   - **method6**: f_g == s_g and f_g == 'M' and f_l == s_l and f_l != "" and check_dob(f_dob, s_dob) and mm > 0 and (mm / max(int(f_mrn), int(s_mrn))) <= 0.005 and (f_dob == s_dob and f_dob != "") or (f_addr == s_addr and f_addr != "") or (f_f == s_f and f_f != "") or (mm > 0 and mm < 100) or (f_m == s_m and f_m != "") or match_partial_ssn(f_ssn, s_ssn)
   - obtained 39 pairs, p=0.8
   - **method7**: f_g == s_g and f_g == 'F' and f_f == s_f and f_f != "" and check_dob(f_dob, s_dob) and mm > 0 and (mm / max(int(f_mrn), int(s_mrn))) <= 0.005
   - result: got 66 pairs with p=0.985
   - **method8**: if mm > 0 and mm < 500
   - result: got 239 pairs with p=0.874
   - filter the pairs from *method8* with ssn1 != ssn2 and not check_dob(f_dob, s_dob), obtain 29/31 false pairs
   - **method9**: measure_name_distance(f_f, s_f) > 0.9 and measure_name_distance(f_l, s_l) > 0.9 and check_dob(f_dob, s_dob) and (f_m == s_m or f_addr == s_addr or (mm > 0 and mm < 500))
   - result: got 1043 pairs with p=0.281
   - apply rule: measured mrn distance > 0 or at least one of ssn is missing, the original file is seperated into two files _b_ an _c_ with 102/470 and 191/573 respectively
   - **method10**:  f_f == s_f and (check_dob(f_dob, s_dob) or (f_addr==s_addr and f_addr != "") ) and f_email == s_email and f_email != ""
   - result: got 613 pairs with p=0.153
9  - In exp7, the rules containing year_day are not used, collect all pairs including this part in a rule in this exp
   - totally got 39000 pairs with only 225 are correct
<br><br>


#### *************************************************************************************************************

## Final Result: p=0.996, r=0.922, f=0.9578
## Final Rank: 18

#### *************************************************************************************************************

## table of fields combination

| index   | combination   | pairs   | test?(y/n)   | percision   | recall   |
|---|---|---|---|---|---|
| 1  | f+l+dob+ssn+gender  | 10950  | y | 1  | 0.19  |
| 2  | f+l+dob+ssn  | 13960  | n  |   |   |
| 3  | f+l+gender+ssn  | 13235  | n |   |   |
| 4  | f+l+dob+gender  | 33474  | y | 0.9997  | 0.58  |
| 5  | f+dob+gender(F)+ssn  | 2882  | n |   |   |
| 6  | f+l+addr  | 12460   | n  |   |   |


### *******************************************************************************

# deterministic remove wrong record pairs strategy:
## stgy1: we get give some rules as what we did for finding same pairs to find different pairs existed in our current selected result set
### rule such as record1.ssn <> record2.ssn etc.

### *******************************************************************************
1. - process address: use regex to normalize the address to a common and unified format
   - the rules and pairs obtained used in this method are listed in the txt\\3_field_process_address\\*.txt (all records add up to 200000+)
   - without f_l_gender rule after deduplication, only 1000+ records have been extracted in which 235 of them are sure to be true
   - the f_l_gender rule gave 1400000+ pair of records which not feasible to process (I can do it but not for sure how much we can get paid)

2. - process first name and last name with three cases
   - the rules are documented in folders txt\\3_fields_process_name\\first_name and txt\\3_fields_process_name\\last_name  (recordes number of each rule can be found in rules.log)
   - after processed with pipeline and name_normalization, the rules on last name leave 843 records and first name rule leave 2971 records
   - first and last records will be combined and run the test to check the percision, recall and f-score
<br><br>


#### *************************************************************************************************************

note:
TODO List:
1. stgy7.txt test in process_address
2. use name apply simi
3. select * from pmac p1, pmac p2 where p1.ENTERPRISEID <> p2.ENTERPRISEID and p1.MOTHERS_MAIDEN_NAME = p2.MOTHERS_MAIDEN_NAME and p1.FIRST_ = p2.FIRST_ and p1.LAST_ = p2.last_;
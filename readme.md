## dedupe
1. use csvdedupe_modified.py obtained a submission result as submission1 -> precision=0.413 recall=0.3
2. seperate 3 records into two or three pairs -> obtain submission2 -> precision=0.84 recall 0.65
<br><br>
#### *************************************************************************************************************


## Deterministic Experiments:
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

2. - process dob with three cases
<br><br>



note:
TODO List:
1. stgy7.txt test in process_address
2.use name apply similar strategy
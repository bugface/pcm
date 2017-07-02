## dedupe
1.use csvdedupe_modified.py obtained a submission result as submission1 -> precision=0.413 recall=0.3
2.seperate 3 records into two or three pairs -> obtain submission2 -> precision=0.84 recall 0.65 

# all below experiments are related to deterministic method

## strategy:
1.query db for same f+l+dob+ssn+gender
2.query db for same f+l+dob+gender
3.query db for same f+l+dob+ssn
4.query db for same f+l+gender+ssn
5.union results between 2 and 3 as com_ssn_gender
6.union results among 2,3 and 4 as com_ssn_gender_dob (total 38635)
7.union results between 3 and 4 as no_gender_no_dob (16011)
8.query db for same f+l+address
9.union results between 8 and 6 (total:40281)

###### experiment are related to results by index 

## experiment to do:
1. test query 1
2. test query 2 (containing query 1)
3. test strategy 6 (sub3)
4. test strategy 7 (sub5)
5. test	strategy 8 (sub4)


## results:
1.query 1 provides a result as precision=1 and recall=0.19
2.query 2 provides a result as precision=0.9997 and recall=0.58
3.result 6 provides a result as precision=0.9997 and recall=0.64
4.result 7 provides a result as precision=1 and recall=0.27854
5.result 8 provides a result as precision=0.99742 and recall=0.69


## csv files:
1.per1: sql from database with condition as same first + last + gender + ssn + DOB (total:10950)
2.per2: sql from database with condition as same first + last + gender + DOB (total:33474)
3.diff_same_SSN: the records from per2 that a pair of two records have different ssn


## txt files:
1.no_gender: all pairs collected with same first + last + ssn +dob
2.no_ssn: all pairs collected with same first + last + dob + gender
3.no_dob: all pairs collected with same first + last + ssn + gender
4.com_ss_gender: all pairs of union of no_gender and no_ssn
5.com_ss_gender_dob: all pairs of union of com_ss_gender and no_dob
6.no_gender_no_dob: all pairs of union of no_gender and no_dob
7.com_gender_ssn_dob_addr: all pairs of union of com_ssn_gender_dob and only_address
8.fldsg: all pairs with same f+l+ssn+dob+gender. This is the source file of per1
9only_fl: contains all pairs having same f + l (total: 744512) (not fit for test)
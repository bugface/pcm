# update fields by applying normalization
from sqlalchemy import create_engine
import logging
import sys
from datetime import date
from address_normalization import normalize_address
import concurrent.futures
import functools
import threading

# constant
SQLALCHEMY_DATABASE_URI = "oracle://alexgre:alex1988@temp1.clx2hx01phun.us-east-1.rds.amazonaws.com/ORCL"
# create sqlalchemy engine
engine = create_engine(SQLALCHEMY_DATABASE_URI, pool_size=5,
                       pool_recycle=3600, pool_reset_on_return='commit')
# logging setup
FORMAT = '%(asctime)-20s %(name)-5s %(levelname)-10s %(message)s'
logging.basicConfig(filename='update_database.log', level=logging.INFO,
                    format=FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("update")
lock = threading.RLock()


def create_new_sql(**param):
    _id = param.get("_id")
    new_value = param.get("value")
    sql = "insert into pcm_update (enterpriseid, address) values ('{}', '{}')".format(
        _id, new_value)
    return sql


def create_new_sql_dob(**param):
    _id = param.get("_id")
    new_value = param.get("value")
    year = param.get("year")
    month = param.get("month")
    day = param.get("day")
    sql = "insert into pcm_update (enterpriseid, address, year, month, day) values ('{}','{}','{}','{}','{}')".format(
        _id, new_value, year, month, day)
    return sql


def create_update_sql(**param):
    _id = param.get("_id")
    field = param.get("field")
    new_value = param.get("value")
    sql = "update pcm_update set {} = '{}' where ENTERPRISEID = '{}'".format(
        field, new_value, _id)
    return sql


def create_update_sql_dob(**param):
    _id = param.get("_id")
    field = param.get("field")
    new_value = param.get("value")
    year = int(param.get("year"))
    month = int(param.get("month"))
    day = int(param.get("day"))
    sql = "update pcm_update set {} = '{}', year = '{}', month = '{}', day = '{}' where ENTERPRISEID = '{}'".format(
        field, new_value, year, month, day, _id)
    return sql
    # _id = param.get("_id")
    # year = int(param.get("year"))
    # month = int(param.get("month"))
    # day = int(param.get("day"))

    # sql = "update pcm_update set year = '{}', month = '{}', day = '{}' where ENTERPRISEID = '{}'".format( _id)

    # return sql


def alter_table():
    sql_alter_table = '''
		alter table pcm_update add (
			naddr varchar(100),
			year number(4),
			month number(2),
			day number(2)
		)
	'''
    engine = create_engine(SQLALCHEMY_DATABASE_URI, pool_size=5,
                           pool_recycle=3600, pool_reset_on_return='commit')

    with engine.begin() as conn:
        conn.execute(sql_alter_table)
        conn.execute("commit")


def main():
    # sql
    sql_query = "select * from pcm_update"
    # start_pts = 0
    with engine.begin() as conn:
        res = conn.execute(sql_query)
        k = 0
        futures_ = []
        with concurrent.futures.ProcessPoolExecutor(max_workers=4) as excutor:
            for i, each in enumerate(res):
                # commit and logger every 20000 records
                future_ = excutor.submit(execute_sql, each=each, k=k, i=i)
                futures_.append(future_)
            concurrent.futures.wait(futures_)
        conn.execute("commit")
        print("done")


def execute_sql(each, i, k):
    if(i % 1000 == 0):
        print("{}% done".format(i / 1000000))

    with engine.begin() as conn:
        if (i + 1) % 20000 == 0:
            conn.execute("commit")
            logger.info("processed {} records.".format(i))
            print("processed {} records.".format(i))
            k += i
        try:
            _id = each['enterpriseid']
            dob = each['dob']
            addr = each['address1']
            naddr = normalize_address(addr)
            if dob is not None:
                year, month, day = dob.strftime("%Y-%m-%d").split("-")
                conn.execute(create_update_sql_dob(
                    _id=_id, field='naddr', value=naddr, year=year, month=month, day=day))
            else:
                conn.execute(create_update_sql(
                    _id=_id, field='naddr', value=naddr))
        except Exception as e:
            conn.execute("rollback")
            print(e)
            logger.info(
                "Currently porcessing records index: {}, previous commit index is {}, and the EnterprisedID is {}".format(i, k, _id))
            sys.exit(1)


def task_create_update_sql_file():
    sql_query = "select * from pcm"
    out_file = "sql_insert_update_table.txt"
    with open(out_file, "w") as f:
        pass
    # start_pts = 0
    with engine.begin() as conn:
        res = conn.execute(sql_query)
        k = 0
        futures_ = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as excutor:
            for i, each in enumerate(res):
                # commit and logger every 20000 records
                future_ = excutor.submit(create_sql_and_add_to_file, each=each)
                future_.add_done_callback(
                    functools.partial(write_out, file=out_file))
                futures_.append(future_)
            concurrent.futures.wait(futures_)


def write_out(future, file):
    res = future.result()
    lock.acquire()
    try:
        with open(file, "a") as f:
            print(res, file=f, end="\n")
    finally:
        lock.release()


def create_sql_and_add_to_file(each):
    _id = each['enterpriseid']
    dob = each['dob']
    addr = each['address1']
    sql = ""
    naddr = normalize_address(addr)
    if dob is not None:
        year, month, day = dob.strftime("%Y-%m-%d").split("-")
        sql += create_new_sql_dob(_id=_id, field='naddr',
                                  value=naddr, year=year, month=month, day=day)
    else:
        sql += create_new_sql(_id=_id, field='naddr', value=naddr)
    return sql


def test_create_test_update_sql(field1, new_value, field2, condition):
    sql = "update test set {} = '{}' where {} = '{}'".format(
        field1, new_value, field2, condition)
    return sql


def test():
    engine = create_engine(SQLALCHEMY_DATABASE_URI, pool_size=4,
                           pool_recycle=3600, pool_reset_on_return='commit')
    sql_query = "select * from test"
    sql_alter_table = '''
		alter table test add (
			nname varchar2(20),
			npts number(3)
		)
	'''
    with engine.begin() as conn:
        if not engine.dialect.has_table(engine, "test"):
            conn.execute(sql_alter_table)
        res = conn.execute(sql_query)
        for each in res:
            team = each['team']
            name = each['name']
            pts = each['pts']
            conn.execute(test_create_test_update_sql(
                "npts", None, "team", team))
            conn.execute(test_create_test_update_sql(
                "nname", "".join([name, "changed"]), "team", team))
        conn.execute("commit")


if __name__ == '__main__':
    # alter_table()
    # main()
    # test()
    task_create_update_sql_file()
# manipulate table in sql_developer
'''
select * from pcm_update order by ENTERPRISEID;

create table pcm_update as select * from pcm;
drop table pcm_update purge;
commit;
'''

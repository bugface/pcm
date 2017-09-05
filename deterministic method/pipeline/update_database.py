#update fields by applying normalization
from sqlalchemy import create_engine
import logging

#constant
SQLALCHEMY_DATABASE_URI = "oracle://alexgre:alex1988@temp1.clx2hx01phun.us-east-1.rds.amazonaws.com/ORCL"
FORMAT = '%(asctime)-20s %(name)-5s %(levelname)-10s %(message)s'

def create_update_sql(**param):
	_id = param.get("_id")
	field = param.get("field")
	new_value = param.get("value")

	sql = "update pcm set {} = {} where ENTERPRISEID = {}".format(field, new_value, _id)

	return sql

def pre_processing_dob_string(s):
    return dict(zip(['year', 'month', 'day'], s.split(" ")[0].split("-")))

def main():
	#sql
	sql_query = "select * from pcm order by ENTERPRISEID"
	sql_alter_table = '''
		alter table pcm add (
			naddr varchar(100),
			year number(4),
			month number(2),
			day number(2)
		)
	'''

	start_pts = 0

	#logging setup
	logging.basicConfig(filename='update_database.log', level=logging.INFO,
	                    format=FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
	logger = logging.getLogger("update")

	#create sqlalchemy engine
	engine = create_engine(SQLALCHEMY_DATABASE_URI, pool_size=4, pool_recycle=3600, pool_reset_on_return='commit')
	with engine.begin() as conn:
		if not engine.dialect.has_table(engine, "pcm"):
			conn.execute(sql_alter_table)

		res = conn.execute(sql_query)
		k = 0
		for i, each in enumerate(res):
			#commit and logger every 20000 records
			if i % 20000 == 0:
				conn.execute("commit")
				logger.INFO("processed {} records.".format(i))
				k += i
			try:
				_id = each["ENTERPRISEID"]
				dob = each["DOB"]
				if dob is not None:
					pass

			except Exception as e:
				logger.ERROR(e)
				logger.INFO("Currently porcessing records index: {}, previous commit index is {}, and the EnterprisedID is {}".format(i, k, _id))
				print("error happend")
				conn.execute("rollback")
				sys.exit(1)

def test_create_test_update_sql(field1, new_value, field2, condition):
	sql = "update test set {} = '{}' where {} = '{}'".format(field1, new_value, field2, condition)
	return sql

def test():
	engine = create_engine(SQLALCHEMY_DATABASE_URI, pool_size=4, pool_recycle=3600, pool_reset_on_return='commit')
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
			conn.execute(test_create_test_update_sql("npts", pts*3, "team", team))
			conn.execute(test_create_test_update_sql("nname", "".join([name, "changed"]), "team", team))
		conn.execute("commit")


if __name__ == '__main__':
	#main()
	test()
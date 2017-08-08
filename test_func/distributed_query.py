

SQLALCHEMY_DATABASE_URI = "oracle://alexgre:alex1988@temp1.clx2hx01phun.us-east-1.rds.amazonaws.com/ORCL"
engine = create_engine(SQLALCHEMY_DATABASE_URI, pool_size=10, pool_recycle=3600)


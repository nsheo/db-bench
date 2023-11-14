import psycopg2
import random

class PostgresController():
	name = "PostgreSQL"

	def __init__(self, user, password, host, port, db, logger):
		self.conn = psycopg2.connect(user=user, password=password, host=host, port=port, database=db)
		self.conn.autocommit = True
		self.cur = self.conn.cursor()
		# self.logger = logging.getLogger(__name__)
		""" """
		self.logger = logger
		

	def getName(self):
		return self.name
	
	def selectJoinTarget(self, rangeNum, exceptTarget) :
		numbers = [*range(1, rangeNum+1)]
		random.shuffle(numbers)
		targetVal = numbers.pop()
		if targetVal == exceptTarget:
			targetVal = numbers.pop()
		return numbers.pop()
	
    
	def runTest(self, target, tableNum, data):
		result = 0
		if target == "INSERT":
			result = self.insertOne(tableNum, data)
		elif target == "MULTI_INSERT":
			result = self.insertMulti(tableNum, data)
		elif target == "SELECT_SINGLE":
			result = self.selectSingleOne(tableNum, data)
		elif target == "SELECT_JOIN":
			joinTargetNum = self.selectJoinTarget(5, tableNum)
			result = self.selectSingleJoin(tableNum, joinTargetNum, data)
		elif target == "UPDATE":
			result = self.updateSingle(tableNum, data)
		elif target == "DELETE":
			result = self.deleteSingle(tableNum, data)
		else :
			self.logger.warn("No Command")
		
		return result

	def create(self, tableNum, maxKey):
		tableName = "bench_" + str(tableNum)
		maxNum = len(str(maxKey)) + 1
		self.cur.execute(f"DROP TABLE IF EXISTS {tableName};")
		self.cur.execute(f"""
				CREATE TABLE {tableName} (
					COL_KEY varchar({maxNum}) NOT NULL primary key, 
					COL_1 varchar(10) NULL, 
					COL_2 varchar(10) NULL,  
					COL_3 varchar(10) NULL, 
					COL_4 varchar(10) NULL,
					COL_5 varchar(10) NULL,
					COL_6 varchar(10) NULL,
					COL_7 varchar(10) NULL,
					COL_8 varchar(10) NULL
				);""")

	def insertOne(self, tableNum, data):
		tableName = "bench_" + str(tableNum)
		result = 0
		try :
			self.cur.execute(f"""
						INSERT INTO {tableName} (COL_KEY, COL_1, COL_2, COL_3, COL_4, COL_5, COL_6, COL_7, COL_8) 
						values ('{"','".join(data)}');
					""")
			result = self.cur.rowcount
		except psycopg2.IntegrityError as dupKeyError:
			self.logger.exception("Duplicate Key Error")
			result = 0
		except Exception as e:
			self.logger.exception("Other Error")
			result = -1
		return result
	
	def insertMulti(self, tableNum, data):
		tableName = "bench_" + str(tableNum)
		result = 0
		try :
			self.cur.execute(f"""
						INSERT INTO {tableName} (COL_KEY, COL_1, COL_2, COL_3, COL_4, COL_5, COL_6, COL_7, COL_8) 
						values ({"), (".join(data)}) ON CONFLICT DO NOTHING;
					""")
			result = self.cur.rowcount
			# self.logger.info(f"Check MULTI_INSERT RESULT : {result}")
		except psycopg2.IntegrityError as dupKeyError:
			self.logger.exception("Duplicate Key Error")
			# result = 0
		except Exception as e:
			self.logger.exception("Other Error")
			result = -1
		return result
	
	def selectSingleOne(self, tableNum, data):
		tableName = "bench_" + str(tableNum)
		result = 0
		try :
			self.cur.execute(f"""
						SELECT 
							COL_KEY, COL_1, COL_2, COL_3, 
							COL_4, COL_5, COL_6, COL_7, COL_8  
						FROM {tableName} 
						WHERE COL_KEY = '{data["colKey"]}';
					""")
			if self.cur.fetchone() != None :
				result = 1		
		except Exception as e:
			self.logger.exception("Other Error")
			result = -1
		return result

	def selectSingleJoin(self, tableNum, joinTargetNum, data):
		tableNameMain = "bench_" + str(tableNum)
		tableNameJoin = "bench_" + str(joinTargetNum)
		result = 0
		try :
			self.cur.execute(f"""
						SELECT 
							A.COL_KEY, A.COL_1, B.COL_2, A.COL_3, A.COL_4,
							B.COL_5, B.COL_6, B.COL_7, B.COL_8
						FROM {tableNameMain} A
						LEFT JOIN {tableNameJoin} B ON A.COL_KEY = B.COL_KEY
						WHERE A.COL_KEY = '{data["colKey"]}';
					""")
			if self.cur.fetchone() != None :
				result = 1		
		except Exception as e:
			self.logger.exception("Other Error")
			result = -1
		return result

	def updateSingle(self, tableNum, data):
		tableName = "bench_" + str(tableNum)
		self.cur.execute(f"""
					UPDATE {tableName} SET 
						COL_1 = '{data["col1"]}', 
						COL_2 = '{data["col2"]}', 
						COL_3 = '{data["col3"]}', 
						COL_4 = '{data["col4"]}', 
						COL_5 = '{data["col5"]}'
					WHERE COL_KEY = '{data["colKey"]}';
				""")
		return self.cur.rowcount

	def deleteSingle(self, tableNum, data):
		tableName = "bench_" + str(tableNum)
		self.cur.execute(f"DELETE FROM {tableName} WHERE COL_KEY = '{data['colKey']}';")
		return self.cur.rowcount

	def connectionClose(self):
		self.conn.close()
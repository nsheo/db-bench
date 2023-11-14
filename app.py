#!/usr/bin/python
import configparser
import logging, random, time, sys
from gen_dummy_data import gen_dummy_data
from multiprocessing import Process, Queue
from typing import Dict
from datetime import datetime
from decimal import Decimal

# Decimal Formatting 
TWO_PLACES = Decimal("0.01")

def configToDict(config: configparser.ConfigParser) -> Dict[str, Dict[str, str]]:
    return {section_name: dict(config[section_name]) for section_name in config.sections()}


def createTable(controller, tableCount, maxKey) :
	main_logger.info("Create %s tables on %s ", str(tableCount), controller.getName())
	count = 0
	while count < tableCount :
		count = count + 1
		controller.create(count, maxKey)
def testProcess(config, processNum, tableNum, result) :

	testTargetDb = (config["DB"]["TEST_TARGET"]).upper()

	processLogHandler = logging.FileHandler("%s_BENCHTEST_%s_DETAIL_PROCESS_%d.log" % (testTargetDb, datetime.now().strftime('%Y%m%d%H%M%S'), processNum))
	processLogHandler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
	processLogger = logging.getLogger("BENCH_TEST_PROCESS_%d" % (processNum))
	processLogger.setLevel(logging.INFO)
	processLogger.addHandler(processLogHandler)
	
	if "POSTGRES" == testTargetDb :
		try:
			from controllers.PostgresController import PostgresController
			testController = PostgresController(
					config["DB"]["USER"],
					config["DB"]["PASSWORD"],
					config["DB"]["HOST"],
					config["DB"]["PORT"],
					config["DB"]["DB"],
					processLogger, 
				)
		except ImportError:
			main_logger.info("psycopg2 is not installed, skipping PostgreSQL benchmarking")
	else : 
		main_logger.info("DB type is not correct")
	
	nTests = int(config["TEST"]["N_TESTS"])
	nMaxKey = int(config["TEST"]["N_MAX_KEY"])
	pInsert = Decimal(config["TEST"]["P_INSERT"])
	pMultiInsert = Decimal(config["TEST"]["P_MULTI_INSERT"])
	pSelectSingle = Decimal(config["TEST"]["P_SELECT_SINGLE"])
	pSelectJoin = Decimal(config["TEST"]["P_SELECT_JOIN"])
	pUpdate = Decimal(config["TEST"]["P_UPDATE"])
	pDelete = Decimal(config["TEST"]["P_DELETE"])

	maxPercentages = Decimal(100.0)
	if (pInsert + pMultiInsert + pSelectSingle + pSelectJoin + pUpdate + pDelete) > maxPercentages :
		processLogger.info("Test rate over! Rebalance test percentage")
		settingPercentage = (pInsert + pMultiInsert + pSelectSingle + pSelectJoin + pUpdate + pDelete)
		pInsert = 100 * (pInsert/settingPercentage)
		pMultiInsert = 100 * (pMultiInsert/settingPercentage)
		pSelectSingle = 100 * (pSelectSingle/settingPercentage)
		pSelectJoin = 100 * (pSelectJoin/settingPercentage)
		pUpdate = 100 * (pUpdate/settingPercentage)
		pDelete = 100 * (pDelete/settingPercentage)
		
	nInsert = int(nTests * (pInsert/maxPercentages))
	nMultiInsert = int(nTests * (pMultiInsert/maxPercentages))
	nSelectSingle = int(nTests * (pSelectSingle/maxPercentages))
	nSelectJoin = int(nTests * (pSelectJoin/maxPercentages))
	nUpdate = int(nTests * (pUpdate/maxPercentages))
	nDelete = int(nTests * (pDelete/maxPercentages))
	
	startTotal = time.time()
	if nInsert > 0 :
		processLogger.info(f"INSERT TEST {nInsert}s")
		result.put(testRunner("INSERT", processNum, testController, tableNum, nInsert, nMaxKey, processLogger ))

		
	if nMultiInsert > 0 :
		processLogger.info(f"MULTI_INSERT TEST {nMultiInsert}s")
		nMultiInsertRow = int(config["TEST"]["N_MULTI_INSERT_ROW_UNIT"])
		result.put(testRunner("MULTI_INSERT", processNum, testController, tableNum, nMultiInsert, nMaxKey, processLogger, nMultiInsertRow))

	if nSelectSingle > 0 :
		processLogger.info(f"SELECT_SINGLE TEST {nSelectSingle}s")
		result.put(testRunner("SELECT_SINGLE", processNum, testController, tableNum, nSelectSingle, nMaxKey, processLogger))

	if nSelectJoin > 0 :
		processLogger.info(f"SELECT_JOIN TEST {nSelectJoin}s")
		result.put(testRunner("SELECT_JOIN", processNum, testController, tableNum, nSelectJoin, nMaxKey, processLogger))

	if nUpdate > 0 :
		processLogger.info(f"UPDATE TEST {nUpdate}s")
		result.put(testRunner("UPDATE", processNum, testController, tableNum, nUpdate, nMaxKey, processLogger))

	if nDelete > 0 :
		processLogger.info(f"DELETE TEST {nDelete}s")
		result.put(testRunner("DELETE", processNum, testController, tableNum, nDelete, nMaxKey, processLogger))
		
	testTotalTime = time.time() - startTotal
	testController.connectionClose()
	main_logger.debug("Process %s Finished" % (str(processNum)))

	result.put({
		"processNum": processNum,
		"tableNum" : tableNum,
		"successCount": 0,
		"failCount": 0,
		"exceptionCount": 0,
		"testType": "ALL",
		"testTotalTime": testTotalTime,
		"testCount": nTests,
		"averageTime": 0,
	})
	return

def testRunner(testType, processNum, dbController, tableNum, testCount, maxKey, processLogger, multiRowNum=1) :
	start = time.time()
	successCount = 0
	failCount = 0
	exceptionCount = 0
	maxLength = len(str(testCount))
	processLogger.info(f"{'BenchTest':^{maxLength*3}}|{'Test Type':^15}|{'Process':^10}|{'Table':^10}|{'Success':^10}|{'Time(ms)':^10}|")
	for i in range(testCount):
		requestStartTime = time.time()
		data = gen_dummy_data(testType, maxKey, multiRowNum)
		result = dbController.runTest(testType, tableNum, data)
		if result < 0 :
			exceptionCount = exceptionCount + 1
		elif result == 0 :
			failCount = failCount + 1
		else :
			successCount = successCount + result
			if "MULTI_INSERT" == testType :
				failCount = failCount + (multiRowNum - result)
		requestEndTime = time.time()
		requestTime = (requestEndTime - requestStartTime) * 1000
		processLogger.info(f"{str(i+1)+'/'+str(testCount):>{maxLength*3}}|{testType:^15}|{processNum:>10}|{'bench_'+str(tableNum):^10}|{result:>10}|{Decimal(requestTime).quantize(TWO_PLACES):>10}|")
	testTime = time.time() - start
	averageTime = testTime / testCount

	return {
		"processNum": processNum,
		"tableNum" : tableNum,
		"successCount": successCount,
		"failCount": failCount,
		"exceptionCount": exceptionCount,
		"testType": testType,
		"testTotalTime": testTime,
		"testCount": testCount,
		"averageTime": averageTime,
	}

def benchRunner(config, logger) :
	testTargetDb = (config["DB"]["TEST_TARGET"]).upper()
	# nTests = int(config["TEST"]["N_TESTS"])
	nProcess = int(config["TEST"]["N_PROCESS"])
	targetTableCount = int(config["TEST"]["N_TABLE_COUNT"])

	if config["TEST"]["B_CREATE"] == 'True' :
		main_logger.info("Run Table Recreate")
		runCreate = False
		if "POSTGRES" == testTargetDb :
			try:
				from controllers.PostgresController import PostgresController
				controller = PostgresController(
					config["DB"]["USER"],
					config["DB"]["PASSWORD"],
					config["DB"]["HOST"],
					config["DB"]["PORT"],
					config["DB"]["DB"],
					logger,
				)
				runCreate = True
			except ImportError:
				logger.info("psycopg2 is not installed, skipping PostgreSQL benchmarking")
		else : 
			logger.info("DB type is not correct")
		
		if runCreate :
			nMaxKey = int(config["TEST"]["N_MAX_KEY"])
			createTable(controller, targetTableCount, nMaxKey)
			controller.connectionClose()

	try :
		processArr = []
		result = Queue()
		result.put({"testType":"START"})
		strParallelRun = config["TEST"]["B_PARALLEL_RUN"]
		for processCount in range(1, nProcess+1) :
			tableNum = 1
			if int(config["TEST"]["N_PROCESS"]) > 1 :
				if strParallelRun == 'True' : 
					if int(config["TEST"]["N_PROCESS"]) == int(config["TEST"]["N_TABLE_COUNT"]) :
						tableNum = processCount
					elif int(config["TEST"]["N_TABLE_COUNT"]) > 1 :
						tableNum = (processCount % int(config["TEST"]["N_TABLE_COUNT"])) + 1
					else :
						tableNum = 1
			else :
				numbers = range(targetTableCount) 
				random.shuffle(numbers)
				tableNum = numbers.pop() + 1
			logger.debug(f"Check TableNum {tableNum}, ProcessNum {processCount}")
			bufferProcess = Process(target=testProcess, args=(config, processCount, tableNum, result))	
			bufferProcess.start()
			processArr.append(bufferProcess)

		for proc in processArr :
			proc.join()
		
		# Process Queue Finish Check
		result.put({"testType":"EXIT"})
		totalTestCount = 0
		totalTestTime = 0.0
		
		while True:
			checker = result.get()
			if checker["testType"] == "EXIT" : 
				logger.info("Test Total Summary")
				logger.info(f"{'Test Count':^15}|{'Test Time(ms)':^15}|")
				logger.info(f"{totalTestCount:>15}|{Decimal(totalTestTime * 1000).quantize(TWO_PLACES):>15}|")
				# logger.info("Test Finished")
				break
			elif checker["testType"] == "START" : 
				# Print result table header
				logger.info(f"{'Test Type':^15}|{'ProcessNum':^12}|{'TableNum':^10}|{'Total':^10}|{'Success':^10}|{'Failed':^10}|{'Exception':^10}|{'Total Time(ms)':^15}|{'Avg. Time(ms)':^15}|")
			else :
				if checker["testType"] == "ALL" :
					totalTestCount += checker["testCount"]
					totalTestTime += checker["testTotalTime"]
				else :
					logger.info(f"{checker['testType']:^15}|{checker['processNum']:>12}|{'bench_' + str(checker['tableNum']):^10}|{checker['testCount']:>10}|{checker['successCount']:>10}|{checker['failCount']:>10}|{checker['exceptionCount']:>10}|{Decimal(checker['testTotalTime'] * 1000).quantize(TWO_PLACES):>15}|{Decimal(checker['averageTime'] * 1000).quantize(TWO_PLACES):>15}|")
	except Exception as e :
		logger.exception("Error")

# Read .env and run the tests
if __name__ == "__main__":
	config = configparser.ConfigParser() 
	config.optionxform = str  
	configFileName = sys.argv[1]
	#config.read('test.config')
	#config.read('select.config')
	config.read(configFileName)
	dictConfig = configToDict(config)
	testTargetDb = (dictConfig["DB"]["TEST_TARGET"]).upper()
	
	main_logHandler = logging.FileHandler("%s_BENCHTEST_%s.log" % (testTargetDb, datetime.now().strftime('%Y%m%d%H%M%S')))
	main_logHandler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
	main_logger = logging.getLogger(__name__)
	main_logger.setLevel(logging.INFO)
	main_logger.addHandler(main_logHandler)

	main_logger.info("DB-Benchmark Test Start")

	# print(dictConfig["TEST"]["B_CREATE"])
	benchRunner(dictConfig, main_logger)
	
	main_logger.info("Test Finished")

	

	# run_with_parameters(controller, n_tests)

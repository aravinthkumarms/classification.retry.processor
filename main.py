import http.client
from datetime import datetime
import pandas as pd
import requests
from config import *
import time
from DBService import *
from queries import *

url = "https://prod.gke-pyromrc.mrcooper.io/adupiceappmaintmgrjava/retry"
queries = []


def log_info(ice):
    logInfo = {
        "Workflow Step": "",
        "Workflow Status": "",
        "ICE IDs": ""
    }
    for ice_id in ice:
        logInfo["Workflow Step"] = ice_id[1]
        logInfo["Workflow Status"] = ice_id[2]
        logInfo["ICE IDs"] = ice_id[0]
        print(logInfo)


def getProposedTime(blob_limit):
    proposedTime = blob_limit * 3
    return proposedTime


def queryJobOrchestrator(ice_List):
    split = []
    completed = 0
    started = 0
    result = queryExecutorFunc(ice_List)
    for res in result:
        split.append(res)
        if (res[2] == "Completed" and res[1] == "Data Delivered") or res[2] == "Exception":
            completed += res[0]
        elif res[2] == "Started":
            started += res[0]
    with open('query.sql', 'w') as file:
        for query in queries:
            file.write("%s\n\n" % query[0])
    print("Completed -", completed)
    queries.clear()
    return split, started == 0 and completed == limit


def queryExecutorFunc(ice):
    query = [splitCountQuery(ice)]
    queries.append(query)
    resultList = processQuery(query, True)
    return resultList


def runDBQueryJob(ice_List, blob_limit, start_time):
    text, isDone = queryJobOrchestrator(ice_List)
    print(log_info(text))
    if isDone:
        return
    time.sleep(20)
    runDBQueryJob(ice_List, blob_limit, start_time)


def formatIDs(ice):
    with open('iceid', 'w') as iceFile:
        for ice_id in range(len(ice)):
            if ice_id == len(ice) - 1:
                txt = '"{iceid}"'.format(iceid=ice[ice_id])
            else:
                txt = '"{iceid}",'.format(iceid=ice[ice_id])
            iceFile.write("%s\n" % txt)


def postData(retry_limit, data):
    retryLimit = retry_limit
    try:
        response = requests.post(url, json=data)
        if response.status_code == 200:
            print("Sent request Successfully")
        else:
            print("Status Code - ", response.status_code)
            print("JSON Response - ", response.text)
        return True
    except http.client.EXPECTATION_FAILED as exp:
        retryLimit -= 1
        print("Error occurred while sending request ", exp)
        if retryLimit > 0:
            postData(i, data)
        if retryLimit == 0:
            print("Retry scriptLimit exceeded")
        return False


def createPayload(ice_List):
    data = {
        "iceIds":
            ice_List
    }
    return data


def updateCurInd(step, status, ICE_IDs):
    processQuery([updateQuery(step, status, ICE_IDs)], False)


def createQuery(wrkflwStep, ice_List):
    pay_Load = {}
    for idx in range(0, len(wrkflwStep)):
        pay_Load.setdefault(wrkflwStep[idx], []).append(ice_List[idx])
    result = []
    for step in pay_Load:
        result.append(updateQuery(step, 2, pay_Load.get(step)))
    return result,pay_Load


df2 = pd.read_csv(fileName)
ICE_LIST = df2['BLOB_DTL_ID'].tolist()
WRKFLW_STEP = df2['WRKFLW_STEP'].tolist()
# Change the scriptLimit for processing and where to where
limit = limit
fromList = From
toList = To
retry = 3

for i in range(fromList, toList, limit):
    try:
        DB()
        startTime = time.time()
        print("Processing from " + str(i) + " to " + str(i + limit))
        print("StartTime - ", datetime.now().strftime("%H:%M:%S"))
        print("Proposed Time - ", getProposedTime(limit))
        iceList = ICE_LIST[i:i + limit]
        wrkflwList = WRKFLW_STEP[i:i + limit]
        with open('lastbatch', 'w') as f:
            f.write(str(i + limit))
        formatIDs(iceList)
        # payLoads = createPayload(iceList)
        updateCheck = processQuery([updateCheckQuery(iceList)], True)
        if len(updateCheck) != 0:
            print("Could not able to update, some IDs in the batch have some completed, Please Check")
            exit(500)
        queries,steps = createQuery(wrkflwList, iceList)
        for step in steps:
            updateCurInd(step,2,steps.get(step))
        runDBQueryJob(iceList, limit, startTime)
        print("--- %s seconds ---" % (time.time() - startTime))
    except ConnectionError as e:
        exit(500)

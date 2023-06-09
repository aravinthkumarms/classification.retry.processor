from concurrent.futures import ThreadPoolExecutor
from EnvConfig import *
import mysql.connector


def DB():
    ICEe = mysql.connector.connect(
        host="127.0.0.1",
        user="svc_ice",
        password="8251b21b725cf70b",
        database="ICEe",
        port=3333
    )
    return ICEe


def processQuery(queryList, isFetch):
    resultList = []
    with ThreadPoolExecutor(100) as executor:
        running_tasks = [executor.submit(executeQuery, query, isFetch) for query in queryList]
        for running_task in running_tasks:
            resultList.append(running_task.result())
    return resultList[0]


def executeQuery(query, isFetch):
    ICEe = DB()
    cursor = ICEe.cursor()
    cursor.execute(query)
    if isFetch:
        count = cursor.fetchall()
        ICEe.close()
        return count
    else:
        print("Executing Update Query, ", query)
        ICEe.commit()
        ICEe.close()
        return

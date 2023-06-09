def convertToTuple(ice):
    if len(ice) == 1:
        return '("' + ice[0] + '")'
    return tuple(ice)


def splitCountQuery(ice):
    ice = convertToTuple(ice)
    query = f"""select count(I.ICE_ID),STEP.LKP_CD_DESC as WRKFLW_STEP, STS.LKP_CD_DESC as WRKFLW_STS from  WRKFLW_DTL W
        left join INGS_BLOB_DTL I on W.BLOB_DTL_ID = I.BLOB_DTL_ID
        left join (select LKP_CD,LKP_CD_DESC from ICEe.VW_LKP_MAST_VAL where LKP_MAST_NM = 'WRKFLW_STEP_TYPE') STEP on W.WRKFLW_STEP_TYPE_ID = cast(STEP.LKP_CD as UNSIGNED INTEGER)
        left join (select LKP_CD,LKP_CD_DESC from ICEe.VW_LKP_MAST_VAL where LKP_MAST_NM = 'WRKFLW_STS_TYPE') STS on W.WRKFLW_STS_TYPE_ID = cast(STS.LKP_CD as UNSIGNED INTEGER)
        where
        W.CURR_IND =1 
        and 
    I.BLOB_DTL_ID in
    {ice}  group by WRKFLW_STEP,WRKFLW_STS;"""
    return query


def updateQuery(step, status, ice):
    ice = convertToTuple(ice)
    query = f"""UPDATE ICEe.WRKFLW_DTL SET CURR_IND = 1 WHERE BLOB_DTL_ID IN {ice} AND WRKFLW_STEP_TYPE_ID = {step} AND WRKFLW_STS_TYPE_ID = {status};"""
    return query


def updateCheckQuery( ice):
    ice = convertToTuple(ice)
    query = f"""SELECT BLOB_DTL_ID FROM ICEe.WRKFLW_DTL WHERE BLOB_DTL_ID IN {ice} AND WRKFLW_STEP_TYPE_ID = 5 AND  WRKFLW_STS_TYPE_ID = 2 AND  CURR_IND = 1 ;"""
    return query

import pandas as pd
import datetime as dt
import numpy as np
import random as rand
import time
import os

# def getRandomStringOrAfk(prob: float)->str:
#     if rand.random() > prob:
#         return "pentawafk"
#     else:
#         return "x"

# def convertRawFileToDf(path: str ) -> pd.DataFrame:
#     df = pd.DataFrame(pd.date_range(start=dt.datetime.now(), periods=20000, freq='S'), columns=['timestamp'])
    
#     comment = np.empty((len(df.index),1), dtype='<U25')
#     for i in range(0, len(df.index)):
#         comment[i] = getRandomStringOrAfk(0.999)

#     df['comment'] = pd.DataFrame(data=comment, columns=['comment']).values
#     return df

def isKeyword(input, keyword:str)->int:
    if keyword in str(input['comment']).lower():
        return 1
    else:
        return 0

def containsKeyword(df:pd.DataFrame, keyword:str)->pd.DataFrame:
    df[keyword] = np.full((len(df.index), 1), fill_value=0)
    df[keyword] =df.apply(isKeyword,keyword=keyword, axis=1 )
    return df

def bucketizeAndCountKeyword(df:pd.DataFrame, bucketsize:str) -> pd.DataFrame:
    df=df.resample(bucketsize).sum()
    return df

def normalizeCount(x,keyword:str)->int:
    if(x[keyword]>=3):
        return 1
    else:
        return 0

def normalizeOccurence(df:pd.DataFrame, keyword:str)->pd.DataFrame:
    kNorm=keyword+'Normalized'
    df[kNorm] = np.full((len(df.index), 1), fill_value=0)
    df[kNorm] =df.apply(normalizeCount,keyword=keyword, axis=1 )

    return df

def parseChatComment(comment:str) -> tuple[dt.datetime, str]:
    timestampStr = comment[1:20]
    ts = dt.datetime.strptime(timestampStr, "%Y-%m-%d %H:%M:%S")
    i=0
    for i in range(20,len(comment)):
        if comment[i] == ':':
            break
    i=i+2
    text = comment[i:len(comment)]
    return ts, text

def parseRawFileAndSaveCsv(rawPath:str, vodid:str):
    timestamps = []
    comments = []
    with open(rawPath, 'r', encoding='UTF-8') as file:
        while line := file.readline().rstrip():
            parsed = parseChatComment(line)
            timestamps.append(parsed[0])
            comments.append(parsed[1])
    
    df = pd.DataFrame(timestamps, columns=['timestamp'])
    df['comment'] = pd.DataFrame(data=comments, columns=['comment']).values
    df = df.set_index(pd.DatetimeIndex(df['timestamp']))
    convertDfToCsv(df, os.path.join('data',vodid+'.csv'))
        

def parseVodComments(vodId:str)->pd.DataFrame:
    csvPath = os.path.join('data',os.path.splitext(vodId)[0]+'.csv')
    rawPath = os.path.join('data','raw',os.path.splitext(vodId)[0]+'.txt')

    if not os.path.exists(csvPath):
        print("Parse raw file")
        parseRawFileAndSaveCsv(rawPath, vodId)
        parseVodComments(vodId)

    if os.path.exists(csvPath):
        print("Parse csv")
        df = pd.read_csv(csvPath)
        df = df.set_index(pd.DatetimeIndex(df['timestamp']))
        return df
    else:
        raise Exception("No file for VODId")

def getVodDate(vodId:str)->dt.datetime:#np.datetime64:#dt.datetime:
    csvPath = os.path.join('data',os.path.splitext(vodId)[0]+'.csv')

    if os.path.exists(csvPath):
        df = pd.read_csv(csvPath, nrows=1)
        # df['timestamp'] = pd.to_datetime(df['timestamp'])
        return pd.to_datetime(df['timestamp'].values[0])
    else:
        raise Exception("No CSV for VODId")


def countTime(df:pd.DataFrame, keyword:str):
    counts= df.groupby([keyword+'Normalized']).size()
    countKeyword = counts[1]
    countTotal = counts[0]+counts[1]
    return countKeyword, countTotal

def calcKeywordPercentage(df:pd.DataFrame, keyword:str)->float:
    counts = countTime(df, keyword)
    countKeyword = counts[0]
    countTotal = counts[1]
    print(countKeyword)
    print(countTotal)
    return (countKeyword/countTotal)*100

def convertDfToCsv(df:pd.DataFrame, path:str):
    csvPath = os.path.join(path)
    df.to_csv(csvPath)

def analyze(path:str, keyword:str)->pd.DataFrame:
    return normalizeOccurence(bucketizeAndCountKeyword(containsKeyword(parseVodComments(path),keyword), '1Min'),keyword)
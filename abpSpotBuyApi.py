# Includes utilities file called abpUtils
from __future__ import division
import abpUtils as autils
import re
from flask import Flask, jsonify, request
from flask_pymongo import PyMongo
from flask import request
import requests
import json
from collections import OrderedDict
#from flask_restful import abort
import math
import numpy as np
from datetime import *
import pytz
import pandas as pd

from dateutil import relativedelta

# import pymongo
utc = pytz.UTC

app = Flask(__name__)

# Include various databases to connect to based on environment
app.config['MONGO_DBNAME'] = 'ABPDev'
app.config['MONGO_URI'] = "mongodb://abpuser:1MomentInTime@abpcluster-shard-00-00-iothc.mongodb.net:27017, \
                           abpcluster-shard-00-01-iothc.mongodb.net:27017, \
                           abpcluster-shard-00-02-iothc.mongodb.net:27017/ABPDev?ssl=true&replicaSet=ABPCluster-shard-0&authSource=admin"
app.config['MONGO_CONNECT'] = 'FALSE'

app.config['MONGO1_DBNAME'] = 'ABPUat'
app.config['MONGO1_URI'] = "mongodb://abpnonproduser:Hell0Greta@abpnonprod-shard-00-00.fm59u.mongodb.net:27017, \
                           abpnonprod-shard-00-01.fm59u.mongodb.net:27017, \
                           abpnonprod-shard-00-02.fm59u.mongodb.net:27017/ABPUat?replicaSet=atlas-a2pokt-shard-0&ssl=true&authSource=admin"
app.config['MONGO1_CONNECT'] = 'FALSE'

app.config['MONGO2_DBNAME'] = 'ABPProd'
app.config['MONGO2_URI'] = "mongodb://abpuser:1MomentInTime@abpcluster-shard-00-00-iothc.mongodb.net:27017, \
                                           abpcluster-shard-00-01-iothc.mongodb.net:27017, \
                                           abpcluster-shard-00-02-iothc.mongodb.net:27017/ABPProd?ssl=true&replicaSet=ABPCluster-shard-0&authSource=admin"
app.config['MONGO2_CONNECT'] = 'FALSE'

app.config['MONGO3_DBNAME'] = 'ABPSimulation'
app.config['MONGO3_URI'] = "mongodb://abpuser:1MomentInTime@abpcluster-shard-00-00-iothc.mongodb.net:27017,\
                                abpcluster-shard-00-01-iothc.mongodb.net:27017,abpcluster-shard-00-02-iothc.mongodb.net:27017/ABPSimulation?replicaSet=ABPCluster-shard-0&ssl=true&authSource=admin"
app.config['MONGO3_CONNECT'] = 'FALSE'


#mongo = PyMongo(app, config_prefix='MONGO')
#mongo1 = PyMongo(app, config_prefix='MONGO1')
#mongo2 = PyMongo(app, config_prefix='MONGO2')
#mongo3 = PyMongo(app, config_prefix='MONGO3')

mongo = PyMongo(app)
mongo1 = PyMongo(app)
mongo2 = PyMongo(app)
mongo3 = PyMongo(app)
@app.route('/abpapi/ABPbasePrice_Calc_spotBuy', methods=['GET'])
def ABPbasePrice_Calc_spotBuy():
    urlString = request.url
    updateFlag = 'Y'
    if urlString.find('uat') > -1:
        MODE = 'UAT'
        print('***Running UAT mode***')
        db = mongo1.db
    elif urlString.find('prod') > -1:
        MODE = 'PROD'
        print('***Running PROD mode***')
        db = mongo2.db
    elif urlString.find('sim') > -1:
        MODE = 'SIM'
        print('***Running SIM mode***')
        db = mongo3.db
    else:
        MODE = 'DEV'
        print('***Running DEV mode***')
        db = mongo.db
    db=mongo2.db

    channelName = request.args.get('channelName')
    dealId = request.args.get('dealId')

    timebandErrorValues = ['0','0:00','','0:0','00:00',np.nan]
    index, reco, data, dfSpotBuyTimeband, dfPrimarySpotBuys, dfNonFCTMonthly, dfNonFCTExposure = calculateChannelIndex(db, dealId, channelName, 'spotBuy')
    inflationPct, indexfromOutlayAndCategory, region, regionChannelPremium, newAdvertiserIncentive, locPremium, networkIncentive, overallIncentivePct, outLayIncentivePct, channelIncentiveFactor, channelSpecificIncentive, westRegIncentive, multiChannelIncentive, prevDealIndex = index.values()
    startDate, endDate, category, minFCT, spotPremiumFrame, rateRecAgg, rank_frame, advertiserClusterId, year, newComboName, isHighPT, prevSummaryObj, brief = data.values()
    print('prevSummary Object#####')
    print(prevSummaryObj)
    digitalRecoConst, multiChRecoConst, recoPriceFactor = reco.values()
    patchDict = {}
    if 'timeband' in dfSpotBuyTimeband:
        dfSpotBuyTimeband.dropna(subset=["timeband"],inplace=True)
    if 'rateType' in dfSpotBuyTimeband:
        dfSpotBuyTimeband = dfSpotBuyTimeband[~dfSpotBuyTimeband["rateType"].isin(["SPL","GHZ","SPL SHOWS","DIPR","DAVP","CLASSIFIED","EDUCATION","ALLIANCE","PRACHAR"])]
    for i in range(0, len(dfSpotBuyTimeband)):
        timeband = dfSpotBuyTimeband.iloc[i]['timeband']['timeband']
        timebandType = dfSpotBuyTimeband.iloc[i]["timeband"]["type"]
        try:
            restrictFrom = dfSpotBuyTimeband.iloc[i]['restrictFrom']
            restrictTo = dfSpotBuyTimeband.iloc[i]['restrictTo']
        except:
            restrictFrom = timeband.split('-')[0]
            restrictTo = timeband.split('-')[1]
        if (restrictFrom in timebandErrorValues) or (restrictTo in timebandErrorValues):
            restrictFrom = timeband.split('-')[0]
            restrictTo = timeband.split('-')[1]
        if restrictFrom != restrictFrom:
            restrictFrom = timeband.split('-')[0]
            restrictTo = timeband.split('-')[1]
        try:
            rateType = dfSpotBuyTimeband.iloc[i]['rateType']
        except:
            rateType='RODP'
        dow = dfSpotBuyTimeband.iloc[i]['dow']['days']
        try:
            spotPosition = dfSpotBuyTimeband.iloc[i]['spotType']['name']
        except:
            spotPosition = None
        previousPrice, virtualPreviousPrice, recommendedPrice, rateCardPrice, baseRateCardPrice, tvr, recoNoGR, waterFallList, rateCardWaterfall, recoParameter, prevDealIndex = spotBuyTimebandRates(db, channelName, timeband, timebandType, dow, rateType, spotPosition, restrictFrom, restrictTo, index, reco, data, 'spotBuy', None)
        dfSpotBuyTimeband.at[i, 'previousPrice'] = previousPrice
        dfSpotBuyTimeband.at[i, 'virtualPreviousPrice'] = virtualPreviousPrice
        dfSpotBuyTimeband.at[i, 'recommendedPrice'] = float(recommendedPrice)
        dfSpotBuyTimeband.at[i, 'rateCardPrice'] = float(rateCardPrice)
        dfSpotBuyTimeband.at[i, 'tvr'] = tvr
        dfSpotBuyTimeband.at[i, 'recoNoGR'] = float(recoNoGR)
        dfSpotBuyTimeband.at[i, 'prevDealIndex'] = float(prevDealIndex)
        dfSpotBuyTimeband.at[i, 'ratecardWaterFall'] = list(waterFallList)
        dfSpotBuyTimeband.at[i, 'rcParameters'] = ''
        dfSpotBuyTimeband.at[i, 'rcParameters'] = rateCardWaterfall.__dict__
        dfSpotBuyTimeband.at[i, 'recoParameters'] = ''
        dfSpotBuyTimeband.at[i, 'recoParameters'] = recoParameter.__dict__
    if len(dfSpotBuyTimeband) != 0:
        patchDict['channel.$.spotBuy.timebands'] = list(dfSpotBuyTimeband.T.to_dict().values())
        patchDict['channel.$.spotBuy.prevDealSummary.prev'] = prevSummaryObj
        patchDict['channel.$.prevDealIndex'] = prevDealIndex

    if 'timeband' in dfPrimarySpotBuys:
        dfPrimarySpotBuys.dropna(subset=["timeband"],inplace=True)
    if 'rateType' in dfPrimarySpotBuys:
        dfPrimarySpotBuys = dfPrimarySpotBuys[~dfPrimarySpotBuys["rateType"].isin(["SPL","GHZ","SPL SHOWS","DIPR","DAVP","CLASSIFIED","EDUCATION","ALLIANCE","PRACHAR"])]
    for i in range(0, len(dfPrimarySpotBuys)):
        timeband = dfPrimarySpotBuys.iloc[i]['timeband']['timeband']
        timebandType = dfPrimarySpotBuys.iloc[i]["timeband"]["type"]
        try:
            restrictFrom = dfPrimarySpotBuys.iloc[i]['restrictFrom']
            restrictTo = dfPrimarySpotBuys.iloc[i]['restrictTo']
        except:
            restrictFrom = timeband.split('-')[0]
            restrictTo = timeband.split('-')[1]
        if (restrictFrom in timebandErrorValues) or (restrictTo in timebandErrorValues):
            restrictFrom = timeband.split('-')[0]
            restrictTo = timeband.split('-')[1]
        if restrictFrom != restrictFrom:
            restrictFrom = timeband.split('-')[0]
            restrictTo = timeband.split('-')[1]
        try:
            rateType = dfPrimarySpotBuys.iloc[i]['rateType']
        except:
            rateType='RODP'
        dow = dfPrimarySpotBuys.iloc[i]['dow']['days']
        try:
            spotPosition = dfPrimarySpotBuys.iloc[i]['spotType']['name']
        except:
            spotPosition = None
        
        previousPrice, virtualPreviousPrice, recommendedPrice, rateCardPrice, baseRateCardPrice, tvr, recoNoGR, waterFallList, rateCardWaterfall, recoParameter, prevDealIndex = spotBuyTimebandRates(db, channelName, timeband, timebandType, dow, rateType, spotPosition, restrictFrom, restrictTo, index, reco, data, 'primarySpotBuy', None)
        dfPrimarySpotBuys.at[i, 'previousPrice'] = previousPrice
        dfPrimarySpotBuys.at[i, 'virtualPreviousPrice'] = virtualPreviousPrice
        dfPrimarySpotBuys.at[i, 'recommendedPrice'] = float(recommendedPrice)
        dfPrimarySpotBuys.at[i, 'rateCardPrice'] = float(rateCardPrice)
        dfPrimarySpotBuys.at[i, 'tvr'] = tvr
        dfPrimarySpotBuys.at[i, 'recoNoGR'] = float(recoNoGR)
        dfPrimarySpotBuys.at[i, 'prevDealIndex'] = float(prevDealIndex)
        dfPrimarySpotBuys.at[i, 'ratecardWaterFall'] = list(waterFallList)
        dfPrimarySpotBuys.at[i, 'rcParameters'] = rateCardWaterfall.__dict__
        dfPrimarySpotBuys.at[i, 'recoParameters'] = recoParameter.__dict__
    if len(dfPrimarySpotBuys) != 0:
        patchDict['channel.$.primarySpotBuy.timebands'] = list(dfPrimarySpotBuys.T.to_dict().values())
        
    dfSponsorshipTimeband, isSponsorship = autils.getSponsorshipParameters(db.dealGridTxn,dealId,channelName)
    if isSponsorship == True:
        dfSponsorshipTimeband.dropna(subset=['timeband'], inplace=True)
        for i in range(0, len(dfSponsorshipTimeband)):
            timeband = dfSponsorshipTimeband.iloc[i]['timeband']['timeband']
            dow = dfSponsorshipTimeband.iloc[i]['dow']['days']
            showTitle = dfSponsorshipTimeband.iloc[i]['sponsorshipTitle']['name']
            programTimeband = dfSponsorshipTimeband.iloc[i]['programTimeband']
            reco_price, rc_price, baseRateCardPrice, waterFallList = sponsorshipTimebandRates(db, channelName, showTitle, dow, timeband, programTimeband, index, reco, data)
            dfSponsorshipTimeband.at[i, 'recommendedPrice'] = float(reco_price)
            dfSponsorshipTimeband.at[i, 'rateCardPrice'] = float(rc_price)
            dfSponsorshipTimeband.at[i, 'baseRateCardPrice'] = float(baseRateCardPrice)
            dfSponsorshipTimeband.at[i, 'ratecardWaterFall'] = waterFallList
        if len(dfSponsorshipTimeband) != 0:
            patchDict['channel.$.sponsorship.timebands'] = list(dfSponsorshipTimeband.T.to_dict().values())

    if 'timeband' in dfNonFCTMonthly:
        dfNonFCTMonthly.dropna(subset=['timeband'], inplace=True)
    for i in range(0, len(dfNonFCTMonthly)):
        # Changed type of start and end date - Poorva - 25-04-2020
        startDate = str(dfNonFCTMonthly.iloc[i]['startDate'])
        endDate = str(dfNonFCTMonthly.iloc[i]['endDate'])
        timeband = dfNonFCTMonthly.iloc[i]['timeband']['timeband']
        timebandType = dfNonFCTMonthly.iloc[i]['timeband']['type']
        dealProperty = dfNonFCTMonthly.iloc[i]['dealProperty']['name']
        priceType = ''
        reco_price, rc_price, baseRateCardPrice, waterFallList = nonFCTTimebandRates(db, 'MONTHLY', channelName, startDate, endDate, timeband, dealProperty, timebandType, None, None, None, index, reco, data, priceType)
        dfNonFCTMonthly.at[i, 'recommendedPrice'] = float(reco_price)
        dfNonFCTMonthly.at[i, 'rateCardPrice'] = float(rc_price)
        dfNonFCTMonthly.at[i, 'baseRateCardPrice'] = float(baseRateCardPrice)
        dfNonFCTMonthly.at[i, 'ratecardWaterFall'] = waterFallList
    if len(dfNonFCTMonthly) != 0:
        patchDict['channel.$.nonFCTMonthly.timebands'] = list(dfNonFCTMonthly.T.to_dict().values())

    for i in range(0, len(dfNonFCTExposure)):
        # Changed type of start and end date - Poorva - 25-04-2020
        startDate = str(dfNonFCTExposure.iloc[i]['startDate'])
        endDate = str(dfNonFCTExposure.iloc[i]['endDate'])
        try:
            restrictFrom = dfNonFCTExposure.iloc[i]['restrictFrom']
            restrictTo = dfNonFCTExposure.iloc[i]['restrictTo']
        except:
            restrictFrom = timeband.split('-')[0]
            restrictTo = timeband.split('-')[1]
        if (restrictFrom in timebandErrorValues) or (restrictTo in timebandErrorValues):
            restrictFrom = timeband.split('-')[0]
            restrictTo = timeband.split('-')[1]
        if restrictFrom != restrictFrom:
            restrictFrom = timeband.split('-')[0]
            restrictTo = timeband.split('-')[1]
        timeband = dfNonFCTExposure.iloc[i]['timeband']['timeband']
        dow = dfNonFCTExposure.iloc[i]['dow']['days']
        dealProperty = dfNonFCTExposure.iloc[i]['dealProperty']['name']
        priceType = dfNonFCTExposure.iloc[i]['priceType']
        reco_price, rc_price, baseRateCardPrice, waterFallList = nonFCTTimebandRates(db, 'EXPOSURE', channelName, startDate, endDate, timeband, dealProperty, None, restrictFrom, restrictTo, dow, index, reco, data, priceType)
        dfNonFCTExposure.at[i, 'recommendedPrice'] = float(reco_price)
        dfNonFCTExposure.at[i, 'rateCardPrice'] = float(rc_price)
        dfNonFCTExposure.at[i, 'baseRateCardPrice'] = float(baseRateCardPrice)
        dfNonFCTExposure.at[i, 'ratecardWaterFall'] = waterFallList
    if len(dfNonFCTExposure) != 0:
        patchDict['channel.$.nonFCTExposure.timebands'] = list(dfNonFCTExposure.T.to_dict().values())
        
    if category not in ["WINTER BONANZA","IPO"]:
        if patchDict and updateFlag == 'Y':
            db.dealGridTxn.update({'dealId': dealId, 'channel.name': channelName },{'$set': patchDict})

    return jsonify({'success': 'success'})

@app.route('/abpapi/sponsorship', methods=['POST'])
def sponsorship():
    urlString = request.url
    if urlString.find('uat') > -1:
        MODE = 'UAT'
        print('***Running UAT mode***')
        db = mongo1.db
    elif urlString.find('prod') > -1:
        MODE = 'PROD'
        print('***Running PROD mode***')
        db = mongo2.db
    elif urlString.find('sim') > -1:
        MODE = 'SIM'
        print('***Running SIM mode***')
        db = mongo3.db
    else:
        MODE = 'DEV'
        print('***Running DEV mode***')
        db = mongo.db
    #db = mongo1.db
    requestArgs = request.get_json(force=True)
    print("=============Request JSON========================")
    print(requestArgs)
    channel = requestArgs['channelName']
    dealId = requestArgs['id']
    timeband = requestArgs['timeBand']
    dow = requestArgs['dow']
    ratType = requestArgs['rateType']
    showTitle = requestArgs['showTitle']
    startDate = requestArgs['startDate']
    endDate = requestArgs['endDate']
    programTimeband = requestArgs['programTimeband']

    index, reco, data, dfSpotBuyTimeband, dfPrimarySpotBuys, dfNonFCTMonthly, dfNonFCTExposure = calculateChannelIndex(db, dealId, channel, 'spotBuy')

    reco_price, rc_price, baseRateCardPrice, waterFallList = sponsorshipTimebandRates(db, channel, showTitle, dow, timeband, programTimeband, index, reco, data)

    return jsonify({'reco_price': reco_price, 'rc_price': rc_price, 'baseRateCardPrice': baseRateCardPrice, 'waterfall': waterFallList})


@app.route('/abpapi/nonFCT', methods=['POST'])
def nonFCT():
    urlString = request.url
    if urlString.find('uat') > -1:
        MODE = 'UAT'
        print('***Running UAT mode***')
        db = mongo1.db
    elif urlString.find('prod') > -1:
        MODE = 'PROD'
        print('***Running PROD mode***')
        db = mongo2.db
    elif urlString.find('sim') > -1:
        MODE = 'SIM'
        print('***Running SIM mode***')
        db = mongo3.db
    else:
        MODE = 'DEV'
        print('***Running DEV mode***')
        db = mongo.db
    db = mongo2.db
    requestArgs = request.get_json(force=True)
    print("===========Request JSON==========================")
    print(requestArgs)
    try:
        startDate = requestArgs['startDate']
        endDate = requestArgs['endDate']
        dealId = requestArgs['dealId']
        nonFCTtypeFlag = requestArgs['nonFCTtypeFlag']
        channel = requestArgs['channelName']
        nonFCTDow = requestArgs['nonFCTDow']
        nonFCTTimeband = requestArgs['nonFCTTimeband']
        nonFCTDealProperty = requestArgs['nonFCTDealProperty']
        restrictFromTimeband = requestArgs['restrictFromTimeband']
        restrictToTimeband = requestArgs['restrictToTimeband']
        if 'priceType' in requestArgs:
            priceType = requestArgs['priceType']
        else:
            priceType = ""
        if 'nonFCTmonthlyType' in requestArgs:
            nonFCTmonthlyType = requestArgs['nonFCTmonthlyType']
        else:
            nonFCTmonthlyType = ''
    except Exception as e:
        print(e)
        error = "parameters not inserted correctly"
        return jsonify({'error': error})
    index, reco, data, dfSpotBuyTimeband, dfPrimarySpotBuys, dfNonFCTMonthly, dfNonFCTExposure = calculateChannelIndex(db, dealId, channel, 'spotBuy')
    reco_price, rc_price, baseRateCardPrice, waterFallList = nonFCTTimebandRates(db, nonFCTtypeFlag, channel, startDate, endDate, nonFCTTimeband, nonFCTDealProperty, nonFCTmonthlyType, restrictFromTimeband, restrictToTimeband,  nonFCTDow, index, reco, data, priceType)
    result={ 'reco_price': reco_price, 'rc_price': rc_price, 'baseRateCardPrice': baseRateCardPrice, 'waterfall': waterFallList }
    print(result)
    return jsonify({ 'reco_price': reco_price, 'rc_price': rc_price, 'baseRateCardPrice': baseRateCardPrice, 'waterfall': waterFallList })


@app.route('/abpapi/abpSpotBuyOne', methods=['POST'])
def abpSpotBuyOne():
    print('****Request')
    print(request)
    requestArgs = request.get_json(force=True)
    print('------REQUEST JSON------')
    print(requestArgs)
    proposalCategory = requestArgs.get('category')
    if proposalCategory != 'COMBO':
        channelName = requestArgs.get('channelName')
    else:
        channelName = None
    dealId = requestArgs.get('dealId')
    timeband = requestArgs.get('timeband')
    timebandType = requestArgs.get("timebandType")
    restrictFromTimeband = requestArgs.get('restrictFromTimeband')
    restrictToTimeband = requestArgs.get('restrictToTimeband')
    politicalType = requestArgs.get('politicalType')

    dow = requestArgs.get('dow')
    rateType = requestArgs.get('rateType')
    spotPosition = requestArgs.get('spotPosition')
    ghzDate = requestArgs.get('ghzDate')
    inventoryType = requestArgs.get('inventoryType')

    urlString = request.url
    
    if urlString.find('uat') > -1:
        MODE = 'UAT'
        print('***Running UAT mode***')
        db = mongo1.db
    elif urlString.find('prod') > -1:
        MODE = 'PROD'
        print('***Running PROD mode***')
        db = mongo2.db
    elif urlString.find('sim') > -1:
        MODE = 'SIM'
        print('***Running SIM mode***')
        db = mongo3.db
    else:
        MODE = 'DEV'
        print('***Running DEV mode***')
        db = mongo.db
    #db = mongo2.db
    response = spotBuyRates(db, dealId, channelName, proposalCategory, timeband, timebandType, restrictFromTimeband, restrictToTimeband, politicalType, dow, rateType, spotPosition, ghzDate, inventoryType)
    return jsonify(response)

def getPrevDealSummary(dbDealSummary,dealId,channelName):

    summaryRec = dbDealSummary.find_one({'dealId':dealId})
    try:
        summaryDF = pd.DataFrame(list(summaryRec['channels']))
        summaryDF = summaryDF.loc[summaryDF['name']==channelName]
        totalOutlayPrev = summaryDF.iloc[0]['spotBuy']['total']['final']
        PTSkew = summaryDF.iloc[0]['spotBuy']['ptSkew']['final']
        weekendSkew = summaryDF.iloc[0]['spotBuy']['weSkew']['final']
        MTSkew = summaryDF.iloc[0]['spotBuy']['mtSkew']['final']
        ER = summaryDF.iloc[0]['spotBuy']['er']['final']
        #sometimes percentage sometimes whole number stored
        PTSkew = PTSkew/100 if PTSkew>1 else PTSkew
        weekendSkew = weekendSkew/100 if weekendSkew>1 else weekendSkew
        MTSkew = MTSkew/100 if MTSkew >1 else MTSkew

    except:
        totalOutlayPrev, PTSkew, weekendSkew,MTSkew, ER = 0.0,0.0,0.0,0.0,0.0
    print ('***PrevDeal***',totalOutlayPrev, PTSkew, weekendSkew, ER)
    return totalOutlayPrev, PTSkew, MTSkew, weekendSkew, ER

def getOverallIncentPct(dealId):
    r = requests.get("https://hi6xlizotc.execute-api.ap-south-1.amazonaws.com/sim/abpDiscount/" + dealId)
    returnText = (r.text)
    try:
        o = json.loads(returnText)
        overallIncentivePct = o['overallIncentive']
    except:
        overallIncentivePct = 0.0

    print(overallIncentivePct)
    return overallIncentivePct

def spotBuyRates(db, dealId, channel, category, timeband, timebandType, restrictFrom, restrictTo, politicalType, dow, rateType, spotPosition, ghzDate, inventoryType):
        index, reco, data, dfSpotBuyTimeband, dfPrimarySpotBuys, dfNonFCTMonthly, dfNonFCTExposure = calculateChannelIndex(db, dealId, channel, inventoryType)
        
        # Here start the code that is not in loop anymore
        previousPrice, virtualPreviousPrice, recommendedPrice, rateCardPrice, baseRateCardPrice, tvr, recoNoGR, waterFallList, rateCardWaterfall, recoParameter, prevDealIndex = spotBuyTimebandRates(db, channel, timeband, timebandType, dow, rateType, spotPosition, restrictFrom, restrictTo, index, reco, data, inventoryType, politicalType)

        returnObject =  {
            'previousPrice': previousPrice,
            'recommendedPrice': recommendedPrice,
            'rateCardPrice': rateCardPrice,
            'baseRateCardPrice': baseRateCardPrice,
            'tvr': tvr,
            'waterfall': waterFallList,
            'rcParameters': rateCardWaterfall.__dict__ if rateCardWaterfall else '',
            'recoParameters': recoParameter.__dict__ if recoParameter else '',
            'virtualPreviousPrice': virtualPreviousPrice,
            'recoNoGR': recoNoGR
        }
        return returnObject

def calculateChannelIndex(db, dealId, channel, inventoryType):
    print('HERE in this function')
    multichannelIncentConst, digitalIncentConst, premiumForHighPTConst, skewPTPercentThreshold = autils.getRecoConstants(db, channel)
    brief = db.dealBriefTxn.find_one({'dealId': dealId })
    print('BRIEF')
    print(brief)
    try:
        isAmendFlag = brief['isUnderAmendment']
    except:
        isAmendFlag = False
    if inventoryType in ['spotBuy']:
        try:
            newCombo = brief["newCombo"]
            newComboItem = next((item for item in newCombo if item["primaryChannel"] == channel), None)
            newComboName = None if newComboItem is None else newComboItem["name"]
        except:
            newComboName = None
    else:
        newComboName = None

    if 'isHighPT' not in brief or brief['isHighPT'] is None:
        isHighPT = False
    else:
        isHighPT = brief['isHighPT']
    #amendFlag = brief['isUnderAmendment'] - key present when deal is amended, else no key
    # if amendFlag == True:
    #     startDate = brief[]
    # else:
    if isAmendFlag != True:
        startDate = brief['fromDate']
    else:
        startDate = brief['updatedAt']
    print('Amend Flag->', isAmendFlag, startDate)
    endDate = brief['toDate']
    advertiser = brief['advertiser']['name'].upper()
    region = brief['region']['name']
    category = brief['category']['name']
    
    if category == 'COMBO':
        channel = None

    try:
        briefAdvertiserClusterId = brief['advertiser']['clusterId']
    except:
        briefAdvertiserClusterId = ''

    try:
        location = brief['createdFor']['location']
    except:
        location = 'UNMAPPED'

    if 'budgetDigital' not in brief or brief['budgetDigital'] is None:
        budgetDigital = 0.0
    else:
        budgetDigital = brief['budgetDigital']

    financialYear, year = autils.getFY(startDate)

    rolling_no = 4
    rolling_number, tgMarket = autils.createtgMktFrame(brief, channel)
    if rolling_number == '13 - WEEKS':
        rolling_no = 13
    if rolling_number == '26 - WEEKS':
        rolling_no = 26
    if rolling_number == '8 - WEEKS':
        rolling_no = 8
    if rolling_number == '4 - WEEKS':
        rolling_no = 4
    weekListForRatings = autils.getLastNWeeks(db.dsRating, rolling_no)

    if channel != 'ABP GANGA':
        rateRec = pd.DataFrame(list(db.dsRating.find({'channel': channel, 'tgMkt': tgMarket})))
        rateRec = rateRec[rateRec['yearWeek'].isin(weekListForRatings)]
        rateRecAgg = rateRec.groupby(['TimeBand','weekDay']).mean()
    else:
        rateRecAgg = pd.DataFrame()

    try:
        tvr = rateRecAgg.loc[(timeband, dow), 'AverageRatingFinal']
    except:
        tvr = None

    budgetFrame, rolling_number, tgMarket = autils.createBudgetFrame(brief, channel)

    budgetFrame = budgetFrame.fillna(0)

    try:
        totalOutlay = brief['budgetIncentive']['totalBudget']
    except:
        totalOutlay = budgetFrame['spotBuy'].sum() + budgetFrame ['sponsorship'].sum() + budgetFrame['nonFCTMonthly'].sum() \
                + budgetFrame['nonFCTExposure'].sum() + budgetFrame ['spotLight'].sum() + budgetDigital

    dfSpotBuyTimeband, dfPrimarySpotBuys, dfNonFCTMonthly, dfNonFCTExposure, channelAskCurrent, westRegIncentive, channelSpecificIncentive, outLayIncentivePct, overAllIncentive, premiumForHighPT, isHighPT, networkIncentive, advertiserIncentive = autils.getGridParameters(db.dealGridTxn, brief, dealId, channel, multichannelIncentConst)
    indexfromOutlayAndCategory, advertiserClusterId = autils.getIndexFromOutlayAndCategory(db.dsOutlayShouldbeIndex, db.advertiserMstr, db.advertiserCluster, channel, advertiser, totalOutlay, briefAdvertiserClusterId)
    
    if 'multiChannelIncentiveFlag' in brief['budgetIncentive'] and brief['budgetIncentive']['multiChannelIncentiveFlag'] == True:
        multiChRecoConst =  multichannelIncentConst
        multiChannelIncentive = 1-brief["budgetIncentive"]["multiChIncentive"]
    else:
        multiChRecoConst = 1.0
        multiChannelIncentive=1.0

    # Added condition for channel not ABP Ganga for digital incentive - Poorva - 24-04-2020
    if 'digitalIncentInternalFlag' in brief['budgetIncentive'] and brief['budgetIncentive']['digitalIncentInternalFlag'] == True and channel != 'ABP GANGA':
        digitalIncentive = 1 - brief['budgetIncentive']['digitalIncentive']
        digitalRecoConst = digitalIncentConst
    else:
        digitalIncentive = 1.0
        digitalRecoConst = 1.0

    # Old logic - advertiserIncentive at deal level - Commented on 30-04-2020 - Poorva
    '''
    try:
        newAdvertiserIncentive = brief['budgetIncentive']['newAdvertiserIncentive']
    except:
        newAdvertiserIncentive = 0.0
    '''
    overallIncentivePct = overAllIncentive
    print('AdvertiserClusterId->', advertiserClusterId)
    prevDealIndex, prevSummaryObj = autils.getPrevDealIndex(dealId, db.dealBriefTxn, db.dsClusterPrevIndex, advertiserClusterId, channel, '2019-20')
    print('REGION->', region)
    if prevDealIndex != 10000:
        regionChannelPremium = 1.0
    else:
        regionChannelPremium = autils.getRegionChannelPremium(db.dsRegionChannelPremium, region, channel)

    locPremium = 1.15 if ((channel == "ABP MAJHA" and location in ["NAGPUR", "SOLAPUR", "KOLHAPUR", "NASHIK","PUNE", "AURANGABAD"]) or
                        (channel == "ABP ASMITA" and location in ["RAJKOT", "BARODA","AHMEDABAD"])) else 1.0
                    
    print(indexfromOutlayAndCategory, regionChannelPremium, locPremium, multiChRecoConst, digitalRecoConst)
    shouldBeindex_supercategory_outlay = indexfromOutlayAndCategory  * regionChannelPremium * locPremium * multiChRecoConst * digitalRecoConst
    print('shouldBeindex_supercategory_outlay-->', shouldBeindex_supercategory_outlay)

    if prevDealIndex != 10000:
        recoIndex = (prevDealIndex + shouldBeindex_supercategory_outlay) / 2
        if prevDealIndex <= shouldBeindex_supercategory_outlay:
            if abs(recoIndex - prevDealIndex) >= 0.05:
                recoIndex = prevDealIndex + 0.05
    else:
        recoIndex = shouldBeindex_supercategory_outlay
    print('recoIndexxxxx->>', recoIndex)

    inflationPct = autils.getInflation(db.dsInflation, channel)
    # Removing inaugral logic for Ganga - Poorva - 01-05-2020
    '''
    if channel == "ABP GANGA":
        rateCardInaugral = pd.DataFrame(list(db.dsInaugralRateCard.find({'channelName': channel}, {'_id': 0})))
        basePriceInaugral = pd.DataFrame(list(db.dsInaugralBasePrice.find({'channelName': channel}, {'_id': 0})))
        rateCardInaugral.fillna(999999999, inplace=True)
        basePriceInaugral.fillna(999999999, inplace=True)
        minFCT = brief["channel"][0]["budget"]["minFCT"]
        rateCardFrameInaugral = autils.getStandalonePrice(rateCardInaugral, channel, startDate, endDate, minFCT)
        basePriceFrameInaugral = autils.getStandalonePrice(basePriceInaugral, channel, startDate, endDate, minFCT)
    else:
        rateCardFrameInaugral = pd.DataFrame()
        basePriceFrameInaugral = pd.DataFrame()
    '''
    premiumRec = db.dsRateCardRuleMstr.find_one({'channel': channel, 'year':int(year)})
    spotPremiumFrame = autils.getspotPremiumFrame(premiumRec, channel)
    if channel != "ABP GANGA":
        rateRec = db.dsRating.find_one({'baseChannel': channel})
        rankRec = db.dsRanking.find_one({'channel': channel})
        rank_frame = pd.DataFrame(list(rankRec['ranking']))
    else:
        rankRec = pd.DataFrame()
        rank_frame = pd.DataFrame()
    channelIncentiveFactor = (1-channelSpecificIncentive) * (1-westRegIncentive)

    index = OrderedDict()
    index['inflation'] = inflationPct
    index['outlayAndCategory'] = indexfromOutlayAndCategory
    index['region'] = region
    index['regionChannel'] = regionChannelPremium
    index['newAdvertiser'] = advertiserIncentive #newAdvertiserIncentive
    index['location'] = locPremium
    index['networkIncentive'] = networkIncentive
    index['overall'] = overallIncentivePct
    index['outLay'] = outLayIncentivePct
    index['channel'] = channelIncentiveFactor
    index['channelSpecific'] = channelSpecificIncentive
    index['westRegion'] = westRegIncentive
    index['multiChannel'] = multiChannelIncentive
    index['previousDeal'] = prevDealIndex        

    data = OrderedDict()
    data['startDate'] = brief['fromDate']
    data['endDate'] = brief['toDate']
    data['category'] = brief['category']['name']
    data['minFCT'] = brief["channel"][0]["budget"]["minFCT"]
    data['spotPremiumFrame'] = spotPremiumFrame
    data['rateRecAgg'] = rateRecAgg
    data['rank_frame'] = rank_frame
    ''' Removing inaugral for Ganga
    data['basePriceFrameInaugral'] = basePriceFrameInaugral
    data['rateCardFrameInaugral'] = rateCardFrameInaugral
    '''
    data['advertiserClusterId'] = advertiserClusterId
    data['year'] = year
    data['newComboName'] = newComboName
    data['isHighPT'] = isHighPT
    data['prevSummaryObj'] = prevSummaryObj
    data['brief'] = brief

    reco = OrderedDict()
    reco['digital'] = digitalRecoConst
    reco['multiChannel'] = multiChRecoConst
    reco['index'] = recoIndex

    return index, reco, data, dfSpotBuyTimeband, dfPrimarySpotBuys, dfNonFCTMonthly, dfNonFCTExposure


# def spotBuyTimebandRates(db, brief, year, politicalType, inflationPct, indexfromOutlayAndCategory, digitalRecoConst, multiChRecoConst, regionChannelPremium, newAdvertiserIncentive, locPremium, moreThanTwoIncentive, overallIncentivePct, outLayIncentivePct, channelIncentiveFactor, westRegIncentive, channelSpecificIncentive, multiChannelIncentive, newAdvertiserIncentivePct, monthListReco, monthsConsidered, prevDealIndex, advertiserClusterId, recoPriceFactor, spotPremiumFrame, rateRecAgg, rank_frame, basePriceFrame, rateCardFrame, basePriceFrameInaugral, rateCardFrameInaugral, channel, timeband, timebandType, dow, rateType, spotType, restrictFrom, restrictTo, isHighPT, newComboName):
def spotBuyTimebandRates(db, channel, timeband, timebandType, dow, rateType, spotType, restrictFrom, restrictTo, index, reco, data, inventoryType, politicalType):
    inflationPct, indexfromOutlayAndCategory, region, regionChannelPremium, newAdvertiserIncentive, locPremium, networkIncentive, overallIncentivePct, outLayIncentivePct, channelIncentiveFactor, channelSpecificIncentive, westRegIncentive, multiChannelIncentive, prevDealIndex = index.values()
    startDate, endDate, category, minFCT, spotPremiumFrame, rateRecAgg, rank_frame, advertiserClusterId, year, newComboName, isHighPT, prevSummaryObj, brief = data.values()
    digitalRecoConst, multiChRecoConst, recoPriceFactor = reco.values()
    if category == 'POLITICAL':
        rateCardPrice = autils.getPoliticalRates(db.dsPoliticalRateCard, channel, timeband, dow, startDate, endDate, politicalType)
        return None, None, rateCardPrice, rateCardPrice, rateCardPrice, None, None, [], [], None, None
    elif category == 'STANDALONE CHANNEL':
        rateCardRecFrame = pd.DataFrame(list(db.dsStandaloneRateCard.find({'channelName': channel},{'_id': 0})))
        rateCardRecFrame.fillna(999999999,inplace=True)
        rateCardFrame = autils.getStandalonePrice(rateCardRecFrame, channel, startDate, endDate,minFCT)

        basePriceFrameRec = pd.DataFrame(list(db.dsStandaloneBasePrice.find({'channelName': channel},{'_id': 0})))
        basePriceFrameRec.fillna(999999999, inplace=True)
        basePriceFrame = autils.getStandalonePrice(basePriceFrameRec, channel, startDate, endDate,minFCT)

        try:
            basePrice = basePriceFrame[(basePriceFrame["rateType"] == rateType) & (basePriceFrame["dayOfWeek"] == dow) & (
                        basePriceFrame["timeBand"] == timeband)].iloc[0]["rate"]
        except Exception as e:
            print(e)
            return {'error_message': "Standalone rate not found"}

        try:
            rateCardPrice = rateCardFrame[(rateCardFrame["rateType"] == rateType) & (rateCardFrame["dayOfWeek"] == dow) & (
                        rateCardFrame["timeBand"] == timeband)].iloc[0]["rate"]
        except Exception as e:
            print(e)
            return {'error_message': "Standalone rate not found"}  
        return None, None, basePrice, rateCardPrice, basePrice, None, None, [], [], None, None
    else:
        if inventoryType in ['primarySpotBuy']:
            newComboName = None
        if newComboName is None:
            print('here in if--->', inventoryType)
            rateCardRecFrame = pd.DataFrame(list(db.dsSpotBuyRateCard.find({'channelName': channel,"rateType":rateType},
                                                                {'_id': 0, 'channelName': 1, 'dayOfWeek': 1, 'monthYear': 1,
                                                                'rate': 1, 'rateType': 1, 'timeBand': 1})))
            basePriceFrameRec = pd.DataFrame(list(db.dsBasePrice.find({'channelName': channel, "rateType": rateType},
                                                                    {'_id': 0, 'channelName': 1, 'dayOfWeek': 1,
                                                                    'monthYear': 1,
                                                                    'rate': 1, 'rateType': 1, 'timeBand': 1})))
        else:
            print('combo here else function')
            print('newComboName->', newComboName)
            rateCardRecFrame = pd.DataFrame(list(db.dsComboRateCard.find({'comboName': newComboName, "rateType": rateType},
                                                                    {'_id': 0, 'comboName': 1, 'dayOfWeek': 1,
                                                                    'monthYear': 1,
                                                                    'rate': 1, 'rateType': 1, 'timeBand': 1})))

            basePriceFrameRec = pd.DataFrame(list(db.dsComboBasePrice.find({'comboName': newComboName, "rateType": rateType},
                                                                {'_id': 0, 'comboName': 1, 'dayOfWeek': 1,'monthYear': 1,
                                                                    'rate': 1, 'rateType': 1, 'timeBand': 1})))
            basePriceFrameRec.rename(columns={'comboName': 'channelName'}, inplace=True)
            rateCardRecFrame.rename(columns={'comboName': 'channelName'}, inplace=True)
            print('rate Card combo frame')
            print(rateCardRecFrame)
            print('base price frame')
            print(basePriceFrameRec)
        
        if newComboName is not None:
            budgetIncentive = brief['budgetIncentive']
            if 'combo' in budgetIncentive:
                comboBudgetIncentive = next((item for item in budgetIncentive['combo'] if item['name'] == newComboName), None)
                if comboBudgetIncentive:
                    outLayIncentivePct = 1 - comboBudgetIncentive['outlayIncentive']
                    overallIncentivePct = 1 - comboBudgetIncentive['overallIncentive']
                    # Taking advertiser Incentive from brief['budgetIncentive']['combo'] in case of combo - Poorva - 15-05-2020
                    newAdvertiserIncentive = comboBudgetIncentive['advertiserIncentive']
            try:
                newCombo = brief["newCombo"]
                newComboItem = next((item for item in newCombo if item["primaryChannel"] == channel), None)
                newComboName = None if newComboItem is None else newComboItem["name"]
                isHighPT = newComboItem['budget']['highPT']
            except:
                isHighPT = False

        rateCardFrame, monthsConsidered = autils.getRateCardLogic(rateCardRecFrame, startDate, endDate)
        print('monthsConsidered-->>', monthsConsidered)
        print('rate card frame&&&&&')
        print(rateCardFrame)
        basePriceFrame, monthListReco = autils.getRecoLogic(basePriceFrameRec, startDate, endDate)
        print('basePriceFrame')
        print(basePriceFrame)
        print('monthList')
        if not restrictFrom:
            restrictFrom = timeband.split('-')[0]
            restrictTo = timeband.split('-')[1]
        else:
            pass
        
        rateCardFactor = 1.0 

        if rateType == '':
            rateType = 'RODP'
        # old logic as on 18Apr'2020
        #if rateType in ['RODP', 'ASR', 'BRC', 'ROS', 'PREMIUM RODP']:
        # Change - timeSkewPremium applicable to RODP and RODPwYT - Poorva - 18-04-2020
        if rateType in ['RODP', 'RODPwYT']:
            timeSkewPremiumPctReco, timeSkewPremiumPctRateCard, restrictHrs = autils.timeSkewPremium(db,timeband, restrictFrom, restrictTo)
            print('time squeeze premiums', timeSkewPremiumPctReco, timeSkewPremiumPctRateCard, restrictHrs)
        else:
            timeSkewPremiumPctReco, timeSkewPremiumPctRateCard, restrictHrs = 1, 1, 0.0

        # Removing 1000 price set for handling exception
        #try:
        if rateType == 'FPR':
            basePrice = autils.getRCClosestTBFPR(basePriceFrame, timeband, channel, rateType, dow, 1.10, newComboName)
        if rateType != 'FPR':
            basePrice = autils.getRCClosestTBNonFPR(basePriceFrame, timeband, channel, rateType, dow, 1.10, newComboName)
        # except:
        #     basePrice = 1000
        
        #try:
        if rateType == 'FPR':
            rateCardPrice = autils.getRCClosestTBFPR(rateCardFrame, timeband, channel, rateType, dow, 1.15, newComboName)
        if rateType != 'FPR':
            rateCardPrice = autils.getRCClosestTBNonFPR(rateCardFrame, timeband, channel, rateType, dow, 1.15, newComboName)
        # except:
        #     rateCardPrice = 1000
        # Removing inaugral logic for ABP Ganga - Poorva - 01-05-2020
        '''
        if channel == "ABP GANGA":
            try:
                basePrice = basePriceFrameInaugral[(basePriceFrameInaugral["rateType"] == rateType) & (
                                    basePriceFrameInaugral["dayOfWeek"] == dow) & (basePriceFrameInaugral["timeBand"] == timeband)].iloc[0]["rate"]
            except:
                pass
            try:
                rateCardPrice = rateCardFrameInaugral[(rateCardFrameInaugral["rateType"] == rateType) & (
                                    rateCardFrameInaugral["dayOfWeek"] == dow) & (rateCardFrameInaugral["timeBand"] == timeband)].iloc[0]["rate"]
            except:
                pass
        '''

        if spotType:
            spotPositionPremium = spotPremiumFrame.loc[spotType, 'premium']
            spotPositionOrig = spotType
        else:
            spotPositionPremium = 0
            spotPositionOrig = None

        try:
            # Digital incentive removed from ABP Ganga - Poorva - 24-04-2020
            if channel != 'ABP GANGA':
                digitalIncentivePct = 1 - brief['budgetIncentive']['digitalIncentive']
            else:
                digitalIncentivePct = 1.0
        except:
            digitalIncentivePct = 1.0

        inventoryFill = 1.0

        spotPositionPremiumPct = 1 + spotPositionPremium
        print('paramteres for previos price--', channel, advertiserClusterId, timeband, rateType, dow, restrictHrs, spotPositionOrig)
        previousPrice, virtualPreviousPrice = autils.getPreviousPrice(db.dsTimeBandPrecedence, channel, advertiserClusterId, timeband, rateType, dow, restrictHrs, spotPositionOrig)
        print('Previous and virtual price-->', previousPrice, virtualPreviousPrice)
        rankFactor = autils.rank_factor_calc(rank_frame, dow, timeband, channel, db.dsRankRules)

        #premiumForHighPT = 1.05 if isHighPT and rateType in ['RODP', 'PREMIUM RODP', 'FPR'] and timebandType == 'PT' else 1.0
        #High PT premium will not be charged on FPR and Assured Spot Rate (ASR) rates. It will only be charged in RODP Rates and would be as per rate card (5% premuim)
        premiumForHighPT = 1.05 if isHighPT and rateType in ['RODP','RODPwYT'] and timebandType == 'PT' else 1.0
        # Reco Marketshare Incentive logic - Poorva - 07-05-2020
        recoMktSharePct, recoDiscountPct = autils.getRecoMarketShare(channel, advertiserClusterId, db.dsRecoMarketShare, db.dsRecoMktGuardrail)
        print('recoMktSharePct, recoDiscountPct->', recoMktSharePct, recoDiscountPct)
        print('newAdvertiserIncentive->', newAdvertiserIncentive)
        # check if newAdvertiserIncentive not applied
        if newAdvertiserIncentive == 0.0:
            recoMktShareFactor = 1 - recoDiscountPct
        else:
            recoMktShareFactor = 1.0
        print('All factors for recoOVerall factor')
        print(recoPriceFactor,spotPositionPremiumPct,timeSkewPremiumPctReco,inflationPct,rankFactor,inventoryFill,premiumForHighPT,recoMktSharePct, recoDiscountPct,recoMktShareFactor)
        recoOverallFactor = recoPriceFactor * spotPositionPremiumPct * timeSkewPremiumPctReco * inflationPct * rankFactor * inventoryFill * premiumForHighPT * recoMktShareFactor
        print('recoOverallFactor is--->>>', recoOverallFactor)
        print('BASE PRICE is-->>>', basePrice)
        # recoPriceFactoris recoIndex
        recoParameter = autils.RecoParameters(basePrice, recoPriceFactor, prevDealIndex, indexfromOutlayAndCategory,premiumForHighPT,
                                        monthListReco, rankFactor, inflationPct, digitalRecoConst, multiChRecoConst,
                                        timeSkewPremiumPctReco, spotPositionPremiumPct, recoOverallFactor,
                                        regionChannelPremium, newAdvertiserIncentive,inventoryFill,locPremium,networkIncentive,recoMktSharePct,recoMktShareFactor)

        rateCardOverallFactor = rateCardFactor * spotPositionPremiumPct * timeSkewPremiumPctRateCard * overallIncentivePct * channelIncentiveFactor * premiumForHighPT
        print('rateCardPrice -- ', rateCardPrice, 'rateCardOverallFactor -- ', rateCardOverallFactor, 'networkIncentive -- ',networkIncentive)
        print('rateCardFactor ---- ',rateCardFactor, 'spotPositionPremiumPct ---- ',spotPositionPremiumPct, 'timeSkewPremiumPctRateCard ---- ',timeSkewPremiumPctRateCard, 'overallIncentivePct ---- ',overallIncentivePct, 'channelIncentiveFactor ---- ',channelIncentiveFactor, 'premiumForHighPT ---- ',premiumForHighPT)
        westRegionIncentivePct = westRegIncentive
        activityIncentivePct = channelSpecificIncentive
        squeezePremiumPct = timeSkewPremiumPctRateCard - 1
        print('squeezePremiumPct->>', squeezePremiumPct)
        waterFallList = []
        newAdvertiserIncentivePct = 1 - newAdvertiserIncentive
        if rateType != 'ROS':
            rateCardWaterfall = autils.RateCardPrice(rateCardPrice, 1.0, monthsConsidered, 0.0, outLayIncentivePct, digitalIncentivePct,
                                                multiChannelIncentive,
                                                activityIncentivePct, westRegionIncentivePct, squeezePremiumPct,
                                                spotPositionPremium,newAdvertiserIncentivePct,premiumForHighPT,networkIncentive)
        else:
            print('rate type is ROS')
            # Adding newAdvertiser and digital incentives in case of ROS - Poorva - 13-05-2020
            rateCardWaterfall = autils.RateCardPrice(rateCardPrice,1.0,monthsConsidered,0.0,1.0,digitalIncentivePct,
                                                     1.0,0.0,0.0,0.0,0.0,newAdvertiserIncentivePct,1.0,1.0)
        if rateType == 'ROS':
            print('digital and newAdv incentives -- ROS -->',digitalIncentivePct, newAdvertiserIncentivePct)
            baseRateCardPrice = rateCardPrice
            # Adding newAdvertiser and digital incentives in ROS - Poorva - 13-05-2020
            rateCardPrice = rateCardPrice * newAdvertiserIncentivePct * digitalIncentivePct
            # Change - no timeSkewPremium in case of ROS - Poorva - 18-04-2020
            #rateCardPrice = rateCardPrice  * timeSkewPremiumPctRateCard
        else:
            baseRateCardPrice = 0
            rateCardPrice = rateCardPrice * rateCardOverallFactor
        print('baseRC and RC-->', baseRateCardPrice, rateCardPrice)
        # Change in reco guardrail - guardrail basis region and channel - Poorva - 20-04-2020
        recoNoGR = basePrice * recoOverallFactor
        print('reco without guardrail ->', recoNoGR)
        recoPrice = autils.getRecoBasedOnThresholds(db, recoNoGR, previousPrice, rateCardPrice, virtualPreviousPrice,
                                                    prevDealIndex, channel, region)
        # old logic
        #basePrice = basePrice * recoOverallFactor
        #recoNoGR = basePrice
        #basePrice = autils.getRecoBasedOnThresholds(db,recoNoGR, previousPrice, rateCardPrice, virtualPreviousPrice, prevDealIndex)
        for items in rateCardWaterfall.computeWaterfall():
            waterFallList.append(items.__dict__)
        try:
            tvr = rateRecAgg.loc[(timeband, dow), 'AverageRatingFinal']
        except:
            tvr = None
        return previousPrice, virtualPreviousPrice, recoPrice, rateCardPrice,baseRateCardPrice, tvr, recoNoGR, waterFallList, rateCardWaterfall, recoParameter, prevDealIndex


def sponsorshipTimebandRates(db, channel, showTitle, dow, timeband, programTimeband, index, reco, data):
    inflationPct, indexfromOutlayAndCategory, region, regionChannelPremium, newAdvertiserIncentive, locPremium, networkIncentive, overallIncentivePct, outLayIncentivePct, channelIncentiveFactor, channelSpecificIncentive, westRegIncentive, multiChannelIncentive, prevDealIndex = index.values()
    print('Network Incentive', networkIncentive, showTitle)
    startDate, endDate, category, minFCT, spotPremiumFrame, rateRecAgg, rank_frame, advertiserClusterId, year, newComboName, isHighPT, prevSummaryObj, brief = data.values()
    digitalRecoConst, multiChRecoConst, recoPriceFactor = reco.values()
    rate_factor = autils.sponTitleRateFactor(channel, showTitle, db.sponsorshipTitleMstr)
    apiMstrVar=db.dsBasePriceAllApiMstr.find_one()

    if channel != "ABP GANGA":
        rankRec = db.dsRanking.find_one({'channel': channel})
        rank_frame = pd.DataFrame(list(rankRec['ranking']))
        rankFactor = autils.rank_factor_calc(rank_frame, dow, timeband, channel, db.dsRankRules)
    else:
        #rankFactor = 0.98
        rankFactor = apiMstrVar['rankFactor']
    # Removed ratetype condition for Ganga channel - Poorva - 18-06-2020
    #rateType = "RODP" if channel == "ABP GANGA" else "FPR"
    rateType = 'FPR'

    rateCardRecFrame = pd.DataFrame(list(db.dsSpotBuyRateCard.find({'channelName': channel, "rateType":rateType},
                                                   {'_id': 0, 'channelName': 1, 'dayOfWeek': 1, 'monthYear': 1,
                                                    'rate': 1, 'rateType': 1, 'timeBand': 1})))
    print('rateCardRecFrame')
    print(rateCardRecFrame)
    rateCardFrame,monthListRC = autils.getRateCardLogic(rateCardRecFrame,startDate, endDate)
    print('After rate card function')
    print('rateCardFrame')
    print(rateCardFrame)
    print('monthListRC', monthListRC)
    basePriceFrameRec = pd.DataFrame(list(db.dsBasePrice.find({'channelName': channel, "rateType":rateType},
                                                       {'_id': 0, 'channelName': 1, 'dayOfWeek': 1, 'monthYear': 1,
                                                        'rate': 1, 'rateType': 1, 'timeBand': 1})))
    print('basePriceFrameRec')
    print(basePriceFrameRec)
    basePriceFrame,monthListReco = autils.getRecoLogic(basePriceFrameRec,startDate, endDate)
    print('After rate card function')
    print('basePriceFrame')
    print(basePriceFrame)
    print('monthListReco->', monthListReco)
    skewCtgry=apiMstrVar['skew_category']
    reco_price = autils.getSponsorshipRates(basePriceFrameRec,programTimeband,channel, skewCtgry,rateType)

    ####convert weekday and weekend skew into single MON-SUN skew#####
    # DOW = "MON-FRI"

    # reco_price_weekday = autils.getSponsorshipRates(basePriceFrameRec,programTimeband,channel, "MON-FRI",rateType)

    # DOW = "SAT-SUN"
    # reco_price_weekend = autils.getSponsorshipRates(basePriceFrameRec, programTimeband, channel, "SAT-SUN", rateType)

    # try:
    #     reco_price = autils.weekSharePriceSpilt(reco_price_weekday, reco_price_weekend, dow)
    # except:
    #     reco_price = (.72 * reco_price_weekday) + (.28 * reco_price_weekend)   
    rc_price = autils.getSponsorshipRates(rateCardFrame, programTimeband, channel, skewCtgry, rateType)
    # DOW = "MON-FRI"
    # rc_price_weekday = autils.getSponsorshipRates(rateCardFrame, programTimeband, channel, "MON-FRI", rateType)

    # DOW = "SAT-SUN"
    # rc_price_weekend = autils.getSponsorshipRates(rateCardFrame, programTimeband, channel, "SAT-SUN", rateType)
    # try:
    #     rc_price = autils.weekSharePriceSpilt(rc_price_weekday, rc_price_weekend, dow)
    # except:
    #     rc_price = (.72 * rc_price_weekday) + (.28 * rc_price_weekend)
    waterfall_rc_price = rc_price
    baseRateCardPrice = rc_price * rate_factor
    reco_price = reco_price * rate_factor * rankFactor * recoPriceFactor * inflationPct
    channelIncentiveFactor = (1 - channelSpecificIncentive) * (1 - westRegIncentive)
    rc_price = rc_price  * rate_factor * overallIncentivePct * channelIncentiveFactor
    if (reco_price > rc_price):
        reco_price = rc_price
    reco_price = autils.lowerBoundTolerance(reco_price, rc_price)

    try:
        # Digital incentive removed from ABP Ganga - Poorva - 24-04-2020
        if channel != 'ABP GANGA':
            digitalIncentivePct = 1 - brief['budgetIncentive']['digitalIncentive']
        else:
            digitalIncentivePct = 1.0
    except:
        digitalIncentivePct = 1.0
    print('Digital Incentive-->', digitalIncentivePct)

    # squeezePremiumPct = 0.0
    # spotPositionPremium = 0.0
    # premiumForHighPT = 1.0
    squeezePremiumPct = apiMstrVar['squeezePremiumPct']
    spotPositionPremium = apiMstrVar['spotPositionPremium']
    premiumForHighPT = apiMstrVar['premiumForHighPT']

    waterFallList = []
    newAdvertiserIncentivePct = 1 - newAdvertiserIncentive
    rateCardWaterfall = autils.RateCardPrice(waterfall_rc_price, rate_factor, monthListRC, 0.0, outLayIncentivePct, digitalIncentivePct,
                                      multiChannelIncentive,
                                      channelSpecificIncentive, westRegIncentive, squeezePremiumPct,
                                      spotPositionPremium, newAdvertiserIncentivePct, premiumForHighPT, networkIncentive)
    for items in rateCardWaterfall.computeWaterfall():
        waterFallList.append(items.__dict__)
    return reco_price, rc_price, baseRateCardPrice, waterFallList


def nonFCTTimebandRates(db, nonFCTtypeFlag, channel, startDate, endDate, timeband, dealProperty, nonFCTmonthlyType, restrictFromTimeband, restrictToTimeband,  dow, index, reco, data, priceType):
    inflationPct, indexfromOutlayAndCategory, region, regionChannelPremium, newAdvertiserIncentive, locPremium, networkIncentive, overallIncentivePct, outLayIncentivePct, channelIncentiveFactor, channelSpecificIncentive, westRegIncentive, multiChannelIncentive, prevDealIndex = index.values()
    briefStartDate, briefEndDate, category, minFCT, spotPremiumFrame, rateRecAgg, rank_frame, advertiserClusterId, year, newComboName, isHighPT, prevSummaryObj, brief = data.values()
    digitalRecoConst, multiChRecoConst, recoPriceFactor = reco.values()
    monthList=1
    try:
        digitalIncentivePct = 1 - brief['budgetIncentive']['digitalIncentive']
    except:
        #digitalIncentivePct = 0.0
        digitalIncentivePct = 1.0
    priceTypePremium = 0.0
    if nonFCTtypeFlag == "MONTHLY":
        print('monthly')
        print(channel, timeband, dealProperty, nonFCTmonthlyType, year)
        NONfctMonthlyDBframe = pd.DataFrame(list(db.nonFCTMonthlyMstr.find(
            {'channel.name': channel, "timeBand": timeband, 'name': dealProperty,
                'type': nonFCTmonthlyType,'year':year})))

        baseRateCardPrice = NONfctMonthlyDBframe['price'].tolist()[0]
        print(baseRateCardPrice)
        print(startDate,endDate)
        startDate = startDate.split("T")[0]
        endDate = endDate.split("T")[0]
        startDate = datetime.strptime(startDate, '%Y-%m-%d')
        endDate = datetime.strptime(endDate, '%Y-%m-%d')
        print(startDate,endDate)

        # Changed monthList to numDays - new logic - price * numdays/30 - Poorva- 05/05/2020
        numDays = abs((endDate - startDate).days + 1)
        print('numDays->', numDays)
        baseRateCardPrice = baseRateCardPrice * numDays/30
        print('price here ->', baseRateCardPrice)
        # Old logic
        # if startDate.month == endDate.month:
        #     monthList = 1
        # else:
        #     numDays = abs((endDate - startDate).days)
        #     if numDays/30 != 1 or numDays/31 != 1:
        #         monthList = math.ceil(numDays/31)
        #     else:
        #         monthList = 1
        #baseRateCardPrice = baseRateCardPrice * monthList
        # Change - Only spend incentive applicable - Poorva - 23-04-2020
        # No incentives applied if channel is GANGA and property name is logo exposure - Poorva - 09-07-2020
        if dealProperty == 'LOGO EXPOSURE' and channel == 'ABP GANGA':
            print('Spl condition for Ganga and Logo exposure')
            rc_price = baseRateCardPrice
        else:
            # removed spend incentive from monthly - Poorva - 20-07-2020
            rc_price = baseRateCardPrice * (1 - newAdvertiserIncentive)
            #rc_price = baseRateCardPrice * outLayIncentivePct * (1 - newAdvertiserIncentive)
        # Old logic as on 18-04-2020
        #rc_price = baseRateCardPrice * digitalIncentivePct * (1 - newAdvertiserIncentive) * (1 - channelSpecificIncentive)
        reco_price = rc_price
        print('reco and rc price -->>', reco_price, rc_price)
        timeSkewPremiumPctRateCardWaterfall = 0.0
        
    if nonFCTtypeFlag == "EXPOSURE" :
        dow = dow
        dow = dow.upper()
        #change weekend and weekday skew into single MON-SUN ###
        # dow = dow.replace('WEEKDAY', 'MON-FRI')
        # dow = dow.replace('WEEKEND', 'SAT-SUN')
        #dow = dow.replace('WEEKEND', 'SAT-SUN')
        print(startDate,endDate)
        if isinstance(startDate, datetime) == False:
            if 'T' in startDate:
                startDate = startDate.split("T")[0]
                endDate = endDate.split("T")[0]
            else:
                startDate = startDate.split(" ")[0]
                endDate = endDate.split(" ")[0]
            print('date ->', startDate, endDate)
            startDate = datetime.strptime(startDate, '%Y-%m-%d')
            endDate = datetime.strptime(endDate, '%Y-%m-%d')
        if not restrictFromTimeband:
            restrictFromTimeband = timeband.split('-')[0]
            restrictToTimeband = timeband.split('-')[1]
        else:
            pass
        #added time squeeze for non fct rates
        timeSkewPremiumPctReco, timeSkewPremiumPctRateCard, restrictHrs = autils.timeSkewPremium(db,timeband,restrictFromTimeband,restrictToTimeband)
        timeSkewPremiumPctRateCardWaterfall = timeSkewPremiumPctRateCard - 1
        # Adding priceType logic in nonFCT exposure - Poorva 09-05-2020
        print('Price Type, deal Property -->', priceType,dealProperty)
        if priceType != 'FCT':
            print('price type not FCT')
            NONfctExposureyDBframe = pd.DataFrame(list(db.nonFCTExposureMstr.find({'channel.name': channel, "timeBand": timeband,
                        'name': dealProperty, 'dow': dow})))
            baseRateCardPrice, monthList = autils.getNonFCTRateCardNewLogic(NONfctExposureyDBframe, startDate, endDate,year)
        else:
            print('price type is FCT')
            # handling 17-24 timeband in FCT price type - Poorva - 12-05-2020
            if timeband == '17:00-24:00':
                timeband = '17:00-25:00'
            else:
                timeband = timeband
            print('timeband-->', timeband)
            NONfctExposureyDBframe = pd.DataFrame(list(db.dsSpotBuyRateCard.find({'channelName': channel, "rateType": 'RODP',"timeBand": timeband},
                                               {'_id': 0, 'channelName': 1, 'dayOfWeek': 1, 'monthYear': 1,
                                                'rate': 1, 'rateType': 1, 'timeBand': 1})))
            baseRateCardPrice, monthList = autils.getRateCardLogic(NONfctExposureyDBframe, startDate, endDate)
            baseRateCardPrice = baseRateCardPrice["rate"][0]
            if dealProperty == 'L BAND':
                print('deal property is L Band')
                priceTypePremium = 0.50
        print('priceTypePremium-->', priceTypePremium)
        print('outlayIncentive - >', outLayIncentivePct)
        # Changed incentives for NonFCT - Poorva 23-04-2020
        # Adding priceType premium basis dealProperty in rate card price - Poorva - 09-05-2020
        rc_price = baseRateCardPrice * outLayIncentivePct * timeSkewPremiumPctReco * (1 - newAdvertiserIncentive) * (1 + priceTypePremium)
        # Old logic - as on 18-04-2020
        #rc_price = baseRateCardPrice * timeSkewPremiumPctReco * digitalIncentivePct * (1 -newAdvertiserIncentive) * (1 - channelSpecificIncentive)
        reco_price = rc_price

    print('reco->', reco_price)
    print('rc price->', rc_price)
    print('baseRC->', baseRateCardPrice)
    waterFallList = []
    # activity, digital, new advertiser
    newAdvertiserIncentivePct = 1 - newAdvertiserIncentive
    # No incentives applied if channel is GANGA and property name is logo exposure, remove from waterfall also - Poorva - 09-07-2020
    if nonFCTtypeFlag == 'MONTHLY' and dealProperty == 'LOGO EXPOSURE' and channel == 'ABP GANGA':
        print('spl addition for monthly - ganga and logo exposure')
        rateCardWaterfall = autils.RateCardPrice(baseRateCardPrice, 1.0, monthList, priceTypePremium, 1.0, 1.0, 1.0,
                                                 0.0, 0.0, timeSkewPremiumPctRateCardWaterfall, 0.0, 1.0, 1.0, 1.0)
    else:
        rateCardWaterfall = autils.RateCardPrice(baseRateCardPrice, 1.0,monthList, priceTypePremium, outLayIncentivePct, 1.0, 1.0,
                                          0.0, 0.0, timeSkewPremiumPctRateCardWaterfall, 0.0, newAdvertiserIncentivePct, 1.0, 1.0)
    for items in rateCardWaterfall.computeWaterfall():
        waterFallList.append(items.__dict__)

    return reco_price, rc_price, baseRateCardPrice, waterFallList

@app.route('/abpapi/nonFctExposureTheshold', methods=['GET'])
def nonFctTheshold():
    urlString = request.url
    if urlString.find('uat') > -1:
        MODE = 'UAT'
        #print('***Running UAT mode***')
        db = mongo1.db
    elif urlString.find('prod') > -1:
        MODE = 'PROD'
        #print('***Running PROD mode***')
        db = mongo2.db
    else:
        MODE = 'SIM'
        #print('***Running SIM mode***')
        db = mongo3.db
    db = mongo2.db
    ############################################################################################
    # result1={ "channelName": 'ABP NEWS',
    #             "restrictFromTimeband": "",
    #             "restrictToTimeband": "",
    #             "startDate": '2021-03-17T06:30:00.000Z',
    #             "endDate": '2021-03-31T06:30:00.000Z',
    #             "nonFCTtypeFlag": 'EXPOSURE',
    #             "nonFCTDow": 'MON-SUN',
    #             "nonFCTDealProperty": 'L BAND',
    #             "nonFCTTimeband": '17:00-24:00',
    #             "bonus": 'NO',
    #             "dealId": 'BNCM363689_4',
    #             "priceType": 'L BAND',
    #             "specialDate": '' }
    
    # print(result1)
    # #exit()
    # # response = requests.post(url="http://127.0.0.1:5000/abpapi/nonFCT",json=result1)
    # response = requests.post(url="https://qvp2sm8kye.execute-api.ap-south-1.amazonaws.com/prod/abpapi/nonFCT",json=result1)

    # #response = requests.post(url="https://d4tv33zv9c.execute-api.ap-south-1.amazonaws.com/sim/abpapi/abpSpotBuyOne",json=result1) 
    # print(response)
    # #http://127.0.0.1:5000/abpapi/abpSpotBuyOne
    
    # #result1={ 'dealId': dealId, 'channelName': channelName, 'restrictFromTimeband': restrictFromTimeband, 'restrictToTimeband': restrictToTimeband,'dow': dow,'rateType': rateType,'timebandType': timebandType,'timeband': timeband,'spotPosition': spotPosition,'category': 'REGULAR','description': '', 'ghzDate': '','inventoryType': 'spotBuy'  }  
    # #print(result1)
    # #print(row3[['rateCardPrice','recommendedPrice','baseRateCardPrice']])
    # result=response.json()
    # ##print(gridDfMONthlyTimeBands)
    # # reslt1={ 'rateCardPrice': float(result['rateCardPrice']), 'recommendedPrice': float(result['recommendedPrice']), 'baseRateCardPrice': float(result['baseRateCardPrice'])}
    # print(result)
    
    # db.dealGridTxn.update({'dealId': dealId },{'$set': {"channel."+str(index2)+".nonFCTExposure.timebands."+str(indx3)+".recommendedPrice":float(result['reco_price']),
    #             "channel."+str(index2)+".nonFCTExposure.timebands."+str(indx3)+".rateCardPrice":float(result['rc_price']),
    #             "channel."+str(index2)+".nonFCTExposure.timebands."+str(indx3)+".ratecardWaterFall":result['waterfall'],
    #                 "channel."+str(index2)+".nonFCTExposure.timebands."+str(indx3)+".baseRateCardPrice":float(result['baseRateCardPrice'])
                
    #             }})    

    ###########################################################################################
    # exit()
    dealBriefDB = db.dealBriefTxn.find({'dealId':'BCFS383544'},no_cursor_timeout=True).limit(10)
    #dealBriefDB = db.dealBriefTxn.find({'category.name':'REGULAR','createdAt':{'$gte':datetime(2020,5,1)}},no_cursor_timeout=True).limit(10)
    import requests
    import time
    i=0
    start_time = time.time()
    # print(len(dealBriefDB))
    # exit();
    dealList=[]
    for dealBriefDBdata in dealBriefDB:
        deal_id=dealBriefDBdata['dealId']
        category=dealBriefDBdata['category']['name']
        createdAt=dealBriefDBdata['createdAt']
        print(createdAt)
        # exit();
        
        print(deal_id)
        dealList.append(deal_id)
        
        dealBriefChnl=dealBriefDBdata['channel']
        channellength=len(dealBriefDBdata['channel'])
        for rowIndex1 in dealBriefChnl:
            
            #print('briefchanellength',channellength)
            dealId=deal_id
            channelName=rowIndex1['name']
            print(channelName,dealId)
            grid = db.dealGridTxn.find_one({ 'dealId': dealId })
            
            try:
                gridDF = pd.DataFrame(list(grid['channel']))
                #print('gridchannel_length',len(gridDF))
                for index2, row1 in gridDF.iterrows():
                    print(index2)
                    nme=row1['name']
                    print(nme,channelName)
            
                    if str(channelName) == str(nme):

                        print(len(gridDF.iloc[index2]['nonFCTExposure']['timebands']))
                        print("********************************************** non fct Exposure ************************************")
                                            
                        try:
                            if (len(gridDF.iloc[index2]['nonFCTExposure']['timebands']))>0: 
                                gridDfMONthlyTimeBands = pd.DataFrame(list(gridDF.iloc[index2]['nonFCTExposure']['timebands']))
                                #print(gridDfTimeBands)
                                #print(gridDF.iloc[index2]['spotBuy']['timebands'][0])
                                for indx3, row3 in gridDfMONthlyTimeBands.iterrows():
                                # print(row3[['rateCardPrice','recommendedPrice','baseRateCardPrice']])
                        
                                    
                                  
                                    try:
                                        dealProperty=row3['dealProperty']['name']
                                    except:
                                        dealProperty=''
                                    try:
                                        sDate=row3['startDate']
                                    except:
                                        sDate=''
                                    try:
                                        eDate=row3['endDate']
                                    except:
                                        eDate=''

                                    
                                    try:
                                        restrictFromTimeband=row3['restrictFrom']
                                    except:
                                        restrictFromTimeband=''
                                    try:
                                        restrictToTimeband=row3['restrictTo']
                                    except:
                                        restrictToTimeband=''
                                    try:
                                        dow=row3['dow']['days']
                                    except:
                                        dow=''
                                    try:
                                        timeband=row3['timeband']['timeband']
                                    except:
                                        timeband=''
                                    try:
                                        ntype=row3['timeband']['type']
                                    except:
                                        ntype=''
                                    
                                                                                        
                                    
                                    
                                    category= 'REGULAR'
                                    description=''
                                    politicalType=''
                                    #rateType='FPR'
                                    print(sDate,eDate)
                                    sdate=str(sDate).split()
                                    startdate=sdate[0]+str('T')+sdate[1]
                                    edate=str(eDate).split()
                                    enddate=edate[0]+str('T')+edate[1]
                                    print(enddate,startdate)
                                    
                                    # result1={ 'dealId': , 'channelName': channelName, 'restrictFromTimeband': restrictFromTimeband, 
                                    # 'restrictToTimeband': restrictToTimeband,"bonus": "NO",'nonFCTDow': "MON-SUN",'startDate': str(startdate),'endDate': str(enddate),
                                    # 'nonFCTDealProperty': dealProperty,'nonFCTTimeband': timeband,'nonFCTtypeFlag': ntype,'category': 'REGULAR','nonFCTtypeFlag': 'Exposure'  }  
                                    
                                    result1={ "channelName": channelName,
                                                "restrictFromTimeband": "",
                                                "restrictToTimeband": "",
                                                "startDate": str(startdate),
                                                "endDate": str(enddate),
                                                "nonFCTtypeFlag": "EXPOSURE",
                                                "nonFCTDow": row3['dow']['days'],
                                                "nonFCTDealProperty": row3['dealProperty']['name'],
                                                "nonFCTTimeband": row3['timeband']['timeband'],
                                                "bonus": "NO",
                                                "dealId": dealId,
                                                "priceType":  row3['priceType'],
                                                "specialDate": "" }
                                    
                                    print(result1)
                                    #exit()
                                    # response = requests.post(url="http://127.0.0.1:5000/abpapi/nonFCT",json=result1)
                                    response = requests.post(url="https://qvp2sm8kye.execute-api.ap-south-1.amazonaws.com/prod/abpapi/nonFCT",json=result1)

                                    #response = requests.post(url="https://d4tv33zv9c.execute-api.ap-south-1.amazonaws.com/sim/abpapi/abpSpotBuyOne",json=result1) 
                                    print(response)
                                    #http://127.0.0.1:5000/abpapi/abpSpotBuyOne
                                    
                                    #result1={ 'dealId': dealId, 'channelName': channelName, 'restrictFromTimeband': restrictFromTimeband, 'restrictToTimeband': restrictToTimeband,'dow': dow,'rateType': rateType,'timebandType': timebandType,'timeband': timeband,'spotPosition': spotPosition,'category': 'REGULAR','description': '', 'ghzDate': '','inventoryType': 'spotBuy'  }  
                                    #print(result1)
                                    #print(row3[['rateCardPrice','recommendedPrice','baseRateCardPrice']])
                                    result=response.json()
                                    ##print(gridDfMONthlyTimeBands)
                                    # reslt1={ 'rateCardPrice': float(result['rateCardPrice']), 'recommendedPrice': float(result['recommendedPrice']), 'baseRateCardPrice': float(result['baseRateCardPrice'])}
                                    print(result)
                                    
                                    db.dealGridTxn.update({'dealId': dealId },{'$set': {"channel."+str(index2)+".nonFCTExposure.timebands."+str(indx3)+".recommendedPrice":float(result['reco_price']),
                                                "channel."+str(index2)+".nonFCTExposure.timebands."+str(indx3)+".rateCardPrice":float(result['rc_price']),
                                                "channel."+str(index2)+".nonFCTExposure.timebands."+str(indx3)+".ratecardWaterFall":result['waterfall'],
                                                  "channel."+str(index2)+".nonFCTExposure.timebands."+str(indx3)+".baseRateCardPrice":float(result['baseRateCardPrice'])
                                                
                                                }})     
                            else:
                                pass
                        except Exception as e:
                            print(e)
                            pass
                            #return jsonify({'error': error})

                        
                        
                        
                    else:
                        pass
            except Exception as e:
                            print(e)
                            pass            
        

        i=i+1;
        print('no of records',i)
    duration = time.time() - start_time
    print(duration)
    print("Total no of", i ,"Records are Successfully execute ")
    print(dealList)
    #cursor.close()
    #return jsonify({'INDEX':index1, 'reco_price': 'reco_price', 'rc_price': 'rc_price', 'baseRateCardPrice': 'baseRateCardPrice', 'waterfall': 'waterFallList' })
    return jsonify({"status":"success"})

@app.route('/abpapi/spotBuyTheshold', methods=['GET'])
def spotBuyTheshold():
    urlString = request.url
    if urlString.find('uat') > -1:
        MODE = 'UAT'
        print('***Running UAT mode***')
        db = mongo1.db
    elif urlString.find('prod') > -1:
        MODE = 'PROD'
        print('***Running PROD mode***')
        db = mongo2.db
    else:
        MODE = 'SIM'
        print('***Running SIM mode***')
        db = mongo3.db
    db = mongo2.db
    
    # dealBriefDB = db.dealBriefTxn.find({'status':'BOOKED','category.name':'REGULAR','fromDate':{'$gt':datetime(2019,4,1)}, 'toDate':{'$lt':datetime(2020,3,31)}},no_cursor_timeout=True)
    #'status':'BOOKED', 'fromDate':{'$gt':datetime(2019,4,1)}, 'toDate':{'$lt':datetime(2020,4,30)}
    #print(dealBriefDB)
    # deal_report = pd.read_excel('dealids.xlsx')
    # deal_list = list(deal_report['dealId'].drop_duplicates())
    # print(len(deal_list))
    # print(deal_list)
    # dealBriefDB = db.dealBriefTxn.find({'dealId':'QHAZ242921'})
    deal_list=['RROF622578']

    dealBriefDB = db.dealBriefTxn.find({'dealId':{'$in': deal_list }})
    # dealBriefDB = db.dealBriefTxn.find({'dealId':'EJAF447476'})
    i=0
    for dealBriefDBdata in dealBriefDB:
        deal_id=dealBriefDBdata['dealId']
        category=dealBriefDBdata['category']['name']
        # print(category)
        # exit();
        
        print(deal_id)
        
        dealBriefChnl=dealBriefDBdata['channel']
        channellength=len(dealBriefDBdata['channel'])
        for rowIndex1 in dealBriefChnl:
            
            #print('briefchanellength',channellength)
            dealId=deal_id
            channelName=rowIndex1['name']
            print(channelName)
            # channelName='ABP MAJHA'
            grid = db.dealGridTxn.find_one({ 'dealId': dealId })
            
            try:
                gridDF = pd.DataFrame(list(grid['channel']))
                #print('gridchannel_length',len(gridDF))
                for index2, row1 in gridDF.iterrows():
                    #print(index2)
                    nme=row1['name']
                    #print(nme)
            
                    if channelName == nme:
                                            
                        try:
                            print(len(gridDF.iloc[index2]['spotBuy']['timebands']))
                            gridDfTimeBands = pd.DataFrame(list(gridDF.iloc[index2]['spotBuy']['timebands']))
                            #print(gridDfTimeBands)
                            #print(gridDF.iloc[index2]['spotBuy']['timebands'][0])
                            for indx3, row3 in gridDfTimeBands.iterrows():
                                #print(indx3)
                                dealId= dealId
                                channel=channelName
                                try:
                                    spotPosition=row3['spotType']['name']
                                except:
                                    spotPosition=''
                                try:
                                    restrictFromTimeband=row3['restrictFrom']
                                except:
                                    restrictFromTimeband=''
                                try:
                                    restrictToTimeband=row3['restrictTo']
                                except:
                                    restrictToTimeband=''
                                try:
                                    dow=row3['dow']['days']
                                except:
                                    dow=''
                                try:
                                    timeband=row3['timeband']['timeband']
                                except:
                                    timeband=''
                                try:
                                    timebandType=row3['timeband']['type']
                                except:
                                    timebandType=''
                                try:
                                    rateType=row3['rateType']
                                except:
                                    rateType=''
                                # if dow in ['MON-FRI', 'SAT-SUN']: 
                                #     dow= 'MON-SUN'
                                    
                                # if rateType in ['RODP', 'ASR', 'BRC', 'ROS', 'PREMIUM RODP']:                                                    
                                #     ghzDate=''
                                    # splDate=''
                                    #spotPosition='LAST SPOT'
                                inventoryType= 'spotBuy'
                                category= 'REGULAR'
                                description=''
                                politicalType=''
                                
                                result1={ 'dealId': dealId, 'channelName': channelName, 'restrictFromTimeband': restrictFromTimeband, 'restrictToTimeband': restrictToTimeband,'dow': dow,'rateType': rateType,'timebandType': timebandType,'timeband': timeband,'spotPosition': spotPosition,'category': 'REGULAR','description': '', 'ghzDate': '','inventoryType': 'spotBuy'  }  
                                print(result1)
                                print("++++++++++++++++++++++++++++++++++++++++++")
                                print("++++++++++++++++++++++++++++++++++++++++++")
                                # response = requests.post(url="https://6vxy3jslia.execute-api.ap-south-1.amazonaws.com/uat/abpapi/abpSpotBuyOne",json=result1) 

                                response = requests.post(url="https://qvp2sm8kye.execute-api.ap-south-1.amazonaws.com/prod/abpapi/abpSpotBuyOne",json=result1) 
                                #print(response)https://d4tv33zv9c.execute-api.ap-south-1.amazonaws.com/sim/abpapi/abpSpotBuyOne
                                
                                
                                result=response.json()
                                print(result)
                                
                                db.dealGridTxn.update({'dealId': dealId },{'$set': {"channel."+str(index2)+".spotBuy.timebands."+str(indx3)+".recommendedPrice":float(result['recommendedPrice']),
                                            "channel."+str(index2)+".spotBuy.timebands."+str(indx3)+".rateCardPrice":float(result['rateCardPrice']),
                                            "channel."+str(index2)+".spotBuy.timebands."+str(indx3)+".ratecardWaterFall":result['waterfall'],
                                            "channel."+str(index2)+".spotBuy.timebands."+str(indx3)+".virtualPreviousPrice":result['virtualPreviousPrice'],
                                            "channel."+str(index2)+".spotBuy.timebands."+str(indx3)+".recoNoGR":float(result['recoNoGR']),
                                            "channel."+str(index2)+".spotBuy.timebands."+str(indx3)+".rcParameters":result['rcParameters'],
                                            "channel."+str(index2)+".spotBuy.timebands."+str(indx3)+".baseRateCardPrice":float(result['baseRateCardPrice']),
                                            "channel."+str(index2)+".spotBuy.timebands."+str(indx3)+".tvr":result['tvr'],
                                            "channel."+str(index2)+".spotBuy.timebands."+str(indx3)+".recoParameters":result['recoParameters'],
                                            "channel."+str(index2)+".spotBuy.timebands."+str(indx3)+".previousPrice":result['previousPrice']
                                            }})     
                                # else:
                                #     print('rateType',rateType)
                                #     pass
                                
                                #print({'channel':channel,'restrictFromTimeband':restrictFromTimeband,'restrictToTimeband':restrictToTimeband,'startDate':startDate,'endDate':endDate,'nonFCTtypeFlag':nonFCTtypeFlag,'nonFCTDow':nonFCTDow,'nonFCTDealProperty':nonFCTDealProperty,'nonFCTTimeband':nonFCTTimeband,'dealId':dealId}) 
                                # index, reco, data, dfSpotBuyTimeband, dfPrimarySpotBuys, dfNonFCTMonthly, dfNonFCTExposure = calculateChannelIndex(db, dealId, channel, 'spotBuy')
                                # reco_price, rc_price, baseRateCardPrice, waterFallList = nonFCTTimebandRates(db, nonFCTtypeFlag, channel, startDate, endDate, nonFCTTimeband, nonFCTDealProperty, nonFCTmonthlyType, restrictFromTimeband, restrictToTimeband,  nonFCTDow, index, reco, data)
                        except Exception as e:
                            print(e)
                            
                            pass
                            #return jsonify({'error': error})
                        
                        
                    else:
                        pass
            except Exception as e:
                            print(e)
                            pass            
        

        i=i+1;
        print('no of records',i)
    print("Total no of", i ,"Records are Successfully execute ")
    #cursor.close()
    #return jsonify({'INDEX':index1, 'reco_price': 'reco_price', 'rc_price': 'rc_price', 'baseRateCardPrice': 'baseRateCardPrice', 'waterfall': 'waterFallList' })
    return jsonify({"status":"success"})




if __name__ == '__main__':
    app.run(debug=True)

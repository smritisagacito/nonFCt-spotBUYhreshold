#author : Poorva
#objective : computes the summary of the deal and generates alerts

from __future__ import division
from datetime import *
import re
from pymongo import MongoClient
import pandas as pd
import numpy as np
import math
import re
from flask import Flask, jsonify, request
from flask_pymongo import PyMongo
from flask import request
import requests
import json
import math
import pytz
import operator
from pandas.io.json import json_normalize
from bson import json_util, ObjectId
import sys

app = Flask(__name__)
utc = pytz.UTC

app.config['MONGO_DBNAME'] = 'ABPDev'
app.config['MONGO_URI'] = "mongodb://abpuser:1MomentInTime@abpcluster-shard-00-00-iothc.mongodb.net:27017, \
                           abpcluster-shard-00-01-iothc.mongodb.net:27017, \
                           abpcluster-shard-00-02-iothc.mongodb.net:27017/ABPDev?ssl=true&replicaSet=ABPCluster-shard-0&authSource=admin"
app.config['MONGO_CONNECT'] = 'FALSE'

app.config['MONGO1_DBNAME'] = 'ABPUat'
app.config['MONGO1_URI'] = "mongodb://abpnonproduser:Hell0Greta@abpnonprod-shard-00-00.fm59u.mongodb.net:27017,abpnonprod-shard-00-01.fm59u.mongodb.net:27017,abpnonprod-shard-00-02.fm59u.mongodb.net:27017/ABPUat?replicaSet=atlas-a2pokt-shard-0&ssl=true&authSource=admin"
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




#comment added to check deployment
mongo = PyMongo(app, config_prefix='MONGO')
mongo1 = PyMongo(app, config_prefix='MONGO1')
mongo2 = PyMongo(app, config_prefix='MONGO2')
mongo3 = PyMongo(app, config_prefix='MONGO3')
@app.route('/abpapi/discountTheshold', methods=['GET'])
def discountTheshold():
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

    deal_report = pd.read_excel('dealids.xlsx')
    deal_list = list(deal_report['dealId'].drop_duplicates())
    print(len(deal_list))
    print(deal_list)
    # dealBriefDB = db.dealBriefTxn.find({'dealId':'QHAZ242921'})
    # deal_list=['UILP263012','TONA789861']

    dealBriefDB = db.dealBriefTxn.find({'dealId':{'$in': deal_list }})
    
    # dealBriefDB = db.dealBriefTxn.find({'dealId':'QHAZ242921'},no_cursor_timeout=True)
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
        try:
                response = requests.get(url="https://d4vha1bwc2.execute-api.ap-south-1.amazonaws.com/prod/abpDiscount/"+str(deal_id))
                print(deal_id)
                # response = requests.get(url="https://cbtz89fm93.execute-api.ap-south-1.amazonaws.com/uat/abpDiscount/"+str(deal_id))
                # https://cbtz89fm93.execute-api.ap-south-1.amazonaws.com/uat
                # https://cbtz89fm93.execute-api.ap-south-1.amazonaws.com/uat
                #response = requests.post(url="https://d4tv33zv9c.execute-api.ap-south-1.amazonaws.com/sim/abpapi/abpSpotBuyOne",json=result1) 
                # print(response)
                #http://127.0.0.1:5000/abpapi/abpSpotBuyOne
                
                #result1={ 'dealId': dealId, 'channelName': channelName, 'restrictFromTimeband': restrictFromTimeband, 'restrictToTimeband': restrictToTimeband,'dow': dow,'rateType': rateType,'timebandType': timebandType,'timeband': timeband,'spotPosition': spotPosition,'category': 'REGULAR','description': '', 'ghzDate': '','inventoryType': 'spotBuy'  }  
                #print(result1)
                #print(row3[['rateCardPrice','recommendedPrice','baseRateCardPrice']])
                result=response.json()
                print(result)
                print(deal_Id)
                ##print(gridDfMONthlyTimeBands)
                # reslt1={ 'rateCardPrice': float(result['rateCardPrice']), 'recommendedPrice': float(result['recommendedPrice']), 'baseRateCardPrice': float(result['baseRateCardPrice'])}
                                    
                        
        except Exception as e:
                        print(e,"last",deal_id)
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


@app.route('/abpapi/summaryTheshold', methods=['GET'])


def summaryTheshold():
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

    # deal_report = pd.read_excel('dealids.xlsx')
    # deal_list = list(deal_report['dealId'].drop_duplicates())
    # print(len(deal_list))
    # print(deal_list)
    # dealBriefDB = db.dealBriefTxn.find({'dealId':'EJAF447476'})
    # deal_list=['UILP263012','TONA789861']
    deal_list=['SONF267672','RROF622578']


    dealBriefDB = db.dealBriefTxn.find({'dealId':{'$in': deal_list }})
    
    # dealBriefDB = db.dealBriefTxn.find({'dealId':'QHAZ242921'},no_cursor_timeout=True)
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
        try:
                # response = requests.get(url="https://jdabizg783.execute-api.ap-south-1.amazonaws.com/prod/abpSummary/"+str(deal_id))
                print(deal_id)
                # response = requests.get(url="https://dqir5aegd9.execute-api.ap-south-1.amazonaws.com/uat/abpSummary/"+str(deal_id))
                response = requests.get(url="http://127.0.0.1:5000/abpSummary/"+str(deal_id))



                #response = requests.post(url="https://d4tv33zv9c.execute-api.ap-south-1.amazonaws.com/sim/abpapi/abpSpotBuyOne",json=result1) 
                # print(response)
                #http://127.0.0.1:5000/abpapi/abpSpotBuyOne
                
                #result1={ 'dealId': dealId, 'channelName': channelName, 'restrictFromTimeband': restrictFromTimeband, 'restrictToTimeband': restrictToTimeband,'dow': dow,'rateType': rateType,'timebandType': timebandType,'timeband': timeband,'spotPosition': spotPosition,'category': 'REGULAR','description': '', 'ghzDate': '','inventoryType': 'spotBuy'  }  
                #print(result1)
                #print(row3[['rateCardPrice','recommendedPrice','baseRateCardPrice']])
                result=response.json()
                print(result)
                print(dealId)
                ##print(gridDfMONthlyTimeBands)
                # reslt1={ 'rateCardPrice': float(result['rateCardPrice']), 'recommendedPrice': float(result['recommendedPrice']), 'baseRateCardPrice': float(result['baseRateCardPrice'])}
                                    
                        
        except Exception as e:
                        print(e,"last",deal_id)
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




@app.route('/abpSummary/<dealId>', methods=['GET'])
def abpSummary(dealId):
    # channelName = request.args.get('channelName')
    # dealId = request.args.get('dealId')
    urlString = request.url
    print(urlString)
    if urlString.find('uat') > -1:
        MODE = 'UAT'
        print('***Running UAT mode***')
    elif urlString.find('prod') > -1:
        MODE = 'PROD'
        print('***Running PROD mode***')
    elif urlString.find('sim') > -1:
        MODE = 'SIM'
        print('***Running PROD mode***')
    else:
        MODE = 'DEV'
        print('***Running DEV mode***')
    MODE = 'PROD'
    if MODE == 'UAT':
        dbGrid = mongo1.db.dealGridTxn
        dbBrief = mongo1.db.dealBriefTxn
        dbSummary = mongo1.db.dealSummaryTxn
        dbAlertsRuleMstr = mongo1.db.dsAlertRulesMstr
        dbComboMstr = mongo1.db.comboMstr
        dbRateRules = mongo1.db.dsRateCardRuleMstr
        dbCombo = mongo1.db.comboMstr
        dbDiscounts = mongo1.db.dsDiscounts
    elif MODE == 'PROD':
        dbGrid = mongo2.db.dealGridTxn
        dbBrief = mongo2.db.dealBriefTxn
        dbSummary = mongo2.db.dealSummaryTxn
        dbAlertsRuleMstr = mongo2.db.dsAlertRulesMstr
        dbComboMstr = mongo2.db.comboMstr
        dbRateRules = mongo2.db.dsRateCardRuleMstr
        dbCombo = mongo2.db.comboMstr
        dbDiscounts = mongo2.db.dsDiscounts
    elif MODE == 'SIM':
        dbGrid = mongo3.db.dealGridTxn
        dbBrief = mongo3.db.dealBriefTxn
        dbSummary = mongo3.db.dealSummaryTxn
        dbAlertsRuleMstr = mongo3.db.dsAlertRulesMstr
        dbComboMstr = mongo3.db.comboMstr
        dbRateRules = mongo3.db.dsRateCardRuleMstr
        dbCombo = mongo3.db.comboMstr
        dbDiscounts = mongo3.db.dsDiscounts
    else:
        dbGrid = mongo.db.dealGridTxn
        dbBrief = mongo.db.dealBriefTxn
        dbSummary = mongo.db.dealSummaryTxn
        dbAlertsRuleMstr = mongo.db.dsAlertRulesMstr
        dbComboMstr = mongo.db.comboMstr
        dbRateRules = mongo.db.dsRateCardRuleMstr
        dbCombo = mongo.db.comboMstr
        dbDiscounts = mongo.db.dsDiscounts

    print('***Parameters**', dealId)
    brief = dbBrief.find_one({'dealId': dealId})
 
    try:
        excludeDigitalFromBilling = brief["excludeDigitalFromBilling"]
    except:
        excludeDigitalFromBilling = True
    gridRec = dbGrid.find_one({'dealId': dealId})
    # askDF = pd.DataFrame(json_normalize(json.loads(json_util.dumps(dbBrief.find({"dealId": dealId},
    #                                                                             {"budgetIncentive.primaryChannel": 1,
    #                                                                              "totalOutlay": 1,
    #                                                                              "parentDealId": 1})))))

    askDF = pd.DataFrame(json_normalize(json.loads(json_util.dumps(brief))))
    #modified for amendments
    parentDealId = askDF.loc[0,"parentDealId"] if "parentDealId" in askDF.columns else "NA"
    preSummaryDF = dbSummary.find_one({"dealId":parentDealId},{"deal":1,"channels":1})
    isAmend = True if preSummaryDF != None else False
    if isAmend == True:
        preSummaryDealLevel = preSummaryDF["deal"]
        preSummaryChannelDF = pd.DataFrame((json_normalize(json.loads(json_util.dumps(preSummaryDF["channels"])))))
        #renaming columns so as to use df.get.column syntax
        #to create a bridge between old deals and deals with ticker struct etc
        if "retailBR"  not in preSummaryDealLevel:
            preSummaryDealLevel["retailBR"] = {"final":0.0}
        if "retailSpotBuy"  not in preSummaryDealLevel:
            preSummaryDealLevel["retailSpotBuy"] = {"final": 0.0}
        if "retail"  not in preSummaryDealLevel:
            preSummaryDealLevel["retail"] = {"final":0.0}
        if "spotlight"  not in preSummaryDealLevel:
            preSummaryDealLevel["spotlight"] = {"final":0.0}

        if "ticker.final"  not in preSummaryChannelDF.columns:
            preSummaryChannelDF["ticker.final"] = 0.0
        if "politicalLBand.final"  not in preSummaryChannelDF.columns:
            preSummaryChannelDF["politicalLBand.final"] = 0.0
        if "politicalAston.final"  not in preSummaryChannelDF.columns:
            preSummaryChannelDF["politicalAston.final"] = 0.0
        if "retailBRLBand.final"  not in preSummaryChannelDF.columns:
            preSummaryChannelDF["retailBRLBand.final"] = 0.0
        if "retailBRAston.final"  not in preSummaryChannelDF.columns:
            preSummaryChannelDF["retailBRAston.final"] = 0.0
        if "retailBRSpotBuy.final"  not in preSummaryChannelDF.columns:
            preSummaryChannelDF["retailBRSpotBuy.final"] = 0.0
        if "retailSpotBuy.final"  not in preSummaryChannelDF.columns:
            preSummaryChannelDF["retailSpotBuy.final"] = 0.0
        if "retailBR.final" not in preSummaryChannelDF.columns:
            preSummaryChannelDF["retailBR.final"] = 0.0
        if "spotlight.final" not in preSummaryChannelDF.columns:
            preSummaryChannelDF["spotlight.final"] = 0.0


        if "retail.final" not in preSummaryChannelDF.columns:
            preSummaryChannelDF["retail.final"] = 0.0
        preSummaryChannelDF.rename(columns={"nonFCT.final": "nonFCTFinal", "nonFCTExposure.final":"nonFCTExpFinal",
                                        "nonFCTMonthly.final":"nonFCTMonFinal", "ticker.final":"tickerFinal",\
                                        "politicalLBand.final":"lBandFinal","politicalAston.final":"astonFinal",\
                                        "spotBuy.total.final":"spotBuyTotFinal",
                                        "spotBuy.weSkew.final":"spotBuyWEFinal","total.final":"totFinal",
                                        "spotBuy.mtSkew.final":"spotBuyMTFinal","spotBuy.ptSkew.final":"spotBuyPTFinal",
                                        "spotBuy.er.final":"spotBuyERFinal","sponsorship.final":"sponsorshipFinal",
                                        "retailBRLBand.final":"brLBandFinal", "retailBRAston.final":"brAstonFinal",\
                                        "retailSpotBuy.final":"retailSpotBuyFinal","retailBRSpotBuy.final":"brSpotBuyFinal",\
                                        "retail.final":"retailFinal", "retailBR.final":"brFinal","spotlight.final":"spotLightFinal"}, inplace=True)
        preSummaryChannelDF=preSummaryChannelDF.set_index("name")

    elif isAmend == False:
        preSummaryChannelDF = pd.DataFrame(columns=['name','nonFCTFinal','nonFCTExpFinal','nonFCTMonFinal',"tickerFinal",\
                                                    "lBandFinal","astonFinal",'spotBuyTotFinal',
                                                    'spotBuyWEFinal','spotBuyMTFinal','spotBuyPTFinal','spotBuyERFinal',
                                                    'sponsorshipFinal','totFinal','brLBandFinal','brAstonFinal','retailSpotBuyFinal',\
                                                     'brSpotBuyFinal','retailFinal','brFinal','spotLightFinal'])
        preSummaryDealLevel = {'total': {'final': None},'nonFCT': {'final': None},'spotBuy': {'ptSkew': {'final': None}, 'weSkew': {'final': None}, \
                               'mtSkew': {'final': None}, 'total': {'final': None}, 'er': {'final': None}}, 'sponsorship': {'final': None},\
                               'retailBR':{'final':None},'retailSpotBuy':{'final':None},'retail':{'final':None},'spotlight':{'final':None}}

    sponsorshipFrame = getChannelFrameSponsorship(gridRec).fillna(0)
    tempSponsorshipFrame = sponsorshipFrame.reset_index()
    nonFCTFrame, nonFCTMonFrame, nonFCTExpFrame,tickerFrame, lBandFrame, astonFrame = getChannelFrameNonFCT(gridRec)
    nonFCTFrame = nonFCTFrame.fillna(0)
    nonFCTMonFrame = nonFCTMonFrame.fillna(0)
    nonFCTExpFrame = nonFCTExpFrame.fillna(0)
    tickerFrame = tickerFrame.fillna(0)
    lBandFrame = lBandFrame.fillna(0)
    astonFrame = astonFrame.fillna(0)
    tempNonFCTFrame = nonFCTFrame.reset_index()

    #for retail
    retailFrame, brFrame, retailSpotBuyFrame, brAstonFrame, brLBandFrame, brSpotBuyFrame = getChannelFrameRetail(gridRec)



    budgetFrame = createBudgetFrame(brief)
    budgetFrame = budgetFrame.fillna(0)
    #spotLightTotal = budgetFrame['spotLight'].sum()
    try:
        budgetDigital = brief['budgetDigital']
    except:
        budgetDigital = 0.0

    if budgetDigital is None:
        budgetDigital = 0.0

    ##get list of alert rule objects
    listAlertRules = getAlertRules(dbAlertsRuleMstr)

    channelFrameSpotBuy, erChannelFrameSpotBuy, channelList, ptSpotBuy, weSpotBuy, mtSpotBuy = getChannelFrameSpotBuy(gridRec, listAlertRules, dealId, dbGrid)
    # new dataframe for reading primary spotbuy
    channelFramePrimarySpotBuy = getChannelFramePrimarySpotBuy(gridRec, listAlertRules, dealId, dbGrid)
    channelFrameIPO3Channel = getIPOFrame3Channels(gridRec)
    channelFrameIPO4Channel = getIPOFrame4Channels(gridRec)
    # Adding combo spotbuy proportion
    newCombo = brief["newCombo"]
    isNewCombo = False if len(newCombo) == 0 else True
    newCombo = pd.DataFrame(newCombo)
    if isNewCombo == True:
        comboDF = newCombo.set_index(['name']).channels.apply(pd.Series).stack().reset_index(name='channels')
        combo_df = newCombo[['name', 'primaryChannel']]
        comboDF = comboDF.merge(combo_df, how='left', on=['name'])
        comboDF = comboDF[['name', 'channels', 'primaryChannel']]
        comboDF.rename(columns={"name": "comboName", "channels": "channelName"}, inplace=True)
        propDf = pd.DataFrame(
            list(dbCombo.find({'name': {'$in': newCombo['name'].tolist()}}, {'name': 1, 'proportion': 1})))
        for combo in range(0, len(comboDF)):
            df_combo = propDf[propDf['name'] == comboDF['comboName'][combo]].reset_index()
            for n in range(0, len(df_combo['proportion'][0])):
                if comboDF['channelName'][combo] == df_combo['proportion'][0][n]['channel']:
                    comboDF.loc[combo, 'proportionValue'] = df_combo['proportion'][0][n]['value']
        print('comboDF->',comboDF)
    channelFrameSpotBuy = channelFrameSpotBuy.reset_index()
    # calculating channel wise values from spotbuy basis proportion value and adding them into primary spotbuy df
    cols_list = ['recommended', 'rateCard', 'final', 'recommendedBilling', 'rateCardBilling', 'finalBilling']
    for i in range(len(channelFrameSpotBuy)):
        if isNewCombo == True:
            channelComboData = comboDF[comboDF['channelName'] == channelFrameSpotBuy['channelName'][i]].reset_index()
        else:
            channelComboData = pd.DataFrame()
        for s in range(len(cols_list)):
            if len(channelComboData) != 0:
                if channelFrameSpotBuy.loc[i, 'channelName'] == channelComboData.loc[0, 'primaryChannel']:
                    channelFrameSpotBuy.loc[i, cols_list[s] + 'Prop'] = channelComboData.loc[0, 'proportionValue'] * \
                                                                        channelFrameSpotBuy.loc[i, cols_list[s]]
                else:
                    primaryChannelComboData = channelFrameSpotBuy[
                        channelFrameSpotBuy['channelName'] == channelComboData.loc[0, 'primaryChannel']].reset_index()
                    channelFrameSpotBuy.loc[i, cols_list[s] + 'Prop'] = channelFrameSpotBuy.loc[i, cols_list[s]] + (
                                channelComboData.loc[0, 'proportionValue'] * primaryChannelComboData.loc[
                            0, cols_list[s]])
            else:
                channelFrameSpotBuy.loc[i, cols_list[s] + 'Prop'] = 0
    if isNewCombo == True:
        for i in range(len(channelFrameSpotBuy)):
            for s in range(len(cols_list)):
                if channelFrameSpotBuy.loc[i, cols_list[s] + 'Prop'] != 0:
                    channelFrameSpotBuy.loc[i, cols_list[s]] = channelFrameSpotBuy.loc[i, cols_list[s] + 'Prop']
                else:
                    channelFrameSpotBuy.loc[i, cols_list[s]] = channelFrameSpotBuy.loc[i, cols_list[s]]

    print(channelFrameSpotBuy[['channelName','regionORBilling','nationalORBilling']])
    channelFrameSpotBuy = channelFrameSpotBuy[['channelName', 'recommended', 'rateCard', 'final', 'precedence', 'fct','regionOR','nationalOR',
                                               'recommendedBilling','rateCardBilling', 'finalBilling', 'precedenceBilling', 'fctBilling',
                                               'regionORBilling','nationalORBilling']]
    channelFramePrimarySpotBuy = channelFramePrimarySpotBuy.reset_index()
    channelFrameIPO3Channel = channelFrameIPO3Channel.reset_index()
    channelFrameIPO4Channel = channelFrameIPO4Channel.reset_index()
    if len(channelFramePrimarySpotBuy)!= 0:
        for s in range(len(cols_list)):
            channelFrameSpotBuy[cols_list[s]] = channelFrameSpotBuy[cols_list[s]] + channelFramePrimarySpotBuy[cols_list[s]]

    if len(channelFrameIPO3Channel)!= 0:
        for s in range(len(cols_list)):
            channelFrameSpotBuy[cols_list[s]] = channelFrameSpotBuy[cols_list[s]] + channelFrameIPO3Channel[cols_list[s]]

    if len(channelFrameIPO4Channel)!= 0:
        for s in range(len(cols_list)):
            channelFrameSpotBuy[cols_list[s]] = channelFrameSpotBuy[cols_list[s]] + channelFrameIPO4Channel[cols_list[s]]

    channelFrameSpotBuy = channelFrameSpotBuy.set_index('channelName')
    print(channelFrameSpotBuy)
    #print("TESTSETSTES", channelFrameSpotBuy.fct.get("ABP NEWS", 0.0))
    #print("channelSpot", channelFrameSpotBuy.index.values)
    ptSpotBuy = ptSpotBuy.fillna(0)
    weSpotBuy = weSpotBuy.fillna(0)
    mtSpotBuy = mtSpotBuy.fillna(0)
    #print("pt", ptSpotBuy)

    tempChannelFrameSpotBuy = channelFrameSpotBuy.reset_index()
    #print(ptSpotBuy.index.values)
    #print("withoutIndex", tempChannelFrameSpotBuy.index.values)
    erChannelFrameSpotBuy = erChannelFrameSpotBuy.fillna(0)
    spotBuyFinalTotal = channelFrameSpotBuy['final'].sum()
    spotBuyPrecedencePriceTotal = channelFrameSpotBuy['precedence'].sum()
    spotBuyRecommendedPriceTotal = channelFrameSpotBuy['recommended'].sum()
    spotBuyRateCardPriceTotal = channelFrameSpotBuy['rateCard'].sum()
    spotBuyRegionORTotal = channelFrameSpotBuy['regionOR'].sum()
    spotBuyNationalORTotal = channelFrameSpotBuy['nationalOR'].sum()
    erSpotBuyFinalTotal = erChannelFrameSpotBuy["final"].sum()
    erSpotBuyRateCardTotal = erChannelFrameSpotBuy["rateCard"].sum()
    erSpotBuyRecommendedTotal = erChannelFrameSpotBuy["recommended"].sum()
    erSpotBuyPrecedenceTotal = erChannelFrameSpotBuy["precedence"].sum()
    ptSkewFinalTotal = ptSpotBuy["final"].sum()
    ptSkewPrecedenceTotal = ptSpotBuy["precedence"].sum()
    #mtSkewFinalTotal = mtSpotBuy["final"].sum()
    weSkewFinalTotal = weSpotBuy["final"].sum()
    weSkewPrecedenceTotal = weSpotBuy["precedence"].sum()
    mtSkewFinalTotal = mtSpotBuy["final"].sum()
    mtSkewPrecedenceTotal = mtSpotBuy["precedence"].sum()
    sponsorshipPrecedenceTotal = sponsorshipFrame["precedence"].sum()
    sponsorshipFinalTotal = sponsorshipFrame["final"].sum()
    sponsorshipRateCardTotal = sponsorshipFrame["rateCard"].sum()
    sponsorshipRecommendedTotal = sponsorshipFrame["recommended"].sum()
    nonFCTPrecedenceTotal = nonFCTFrame["precedence"].sum()
    nonFCTFinalTotal = nonFCTFrame["final"].sum()
    nonFCTRateCardTotal = nonFCTFrame["rateCard"].sum()
    nonFCTRecommendedTotal = nonFCTFrame["recommended"].sum()
    #billing
    spotBuyFinalTotalBilling = channelFrameSpotBuy['finalBilling'].sum()
    spotBuyPrecedencePriceTotalBilling = channelFrameSpotBuy['precedenceBilling'].sum()
    spotBuyRecommendedPriceTotalBilling = channelFrameSpotBuy['recommendedBilling'].sum()
    spotBuyRateCardPriceTotalBilling = channelFrameSpotBuy['rateCardBilling'].sum()
    spotBuyRegionORTotalBilling = channelFrameSpotBuy['regionORBilling'].sum()
    spotBuyNationalORTotalBilling = channelFrameSpotBuy['nationalORBilling'].sum()
    erSpotBuyFinalTotalBilling = erChannelFrameSpotBuy["finalBilling"].sum()
    erSpotBuyRateCardTotalBilling = erChannelFrameSpotBuy["rateCardBilling"].sum()
    erSpotBuyRecommendedTotalBilling = erChannelFrameSpotBuy["recommendedBilling"].sum()
    erSpotBuyPrecedenceTotalBilling = erChannelFrameSpotBuy["precedenceBilling"].sum()
    sponsorshipPrecedenceTotalBilling = sponsorshipFrame["precedenceBilling"].sum()
    sponsorshipFinalTotalBilling = sponsorshipFrame["finalBilling"].sum()
    sponsorshipRateCardTotalBilling = sponsorshipFrame["rateCardBilling"].sum()
    sponsorshipRecommendedTotalBilling = sponsorshipFrame["recommendedBilling"].sum()
    nonFCTPrecedenceTotalBilling = nonFCTFrame["precedenceBilling"].sum()
    nonFCTFinalTotalBilling = nonFCTFrame["finalBilling"].sum()
    nonFCTRateCardTotalBilling = nonFCTFrame["rateCardBilling"].sum()
    nonFCTRecommendedTotalBilling = nonFCTFrame["recommendedBilling"].sum()

    #FOR RETAIL

    brFinalTotal = brFrame["final"].sum()
    brPrecedenceTotal = brFrame["precedence"].sum()
    brRateCardTotal = brFrame["rateCard"].sum()
    brRecommendedTotal = brFrame["recommended"].sum()
    retailSpotFinalTotal = retailSpotBuyFrame["final"].sum()
    retailSpotPrecedenceTotal = retailSpotBuyFrame["precedence"].sum()
    retailSpotRecommendedTotal = retailSpotBuyFrame["recommended"].sum()
    retailSpotRateCardTotal = retailSpotBuyFrame["rateCard"].sum()
    retailFinalTotal = retailFrame["final"].sum()
    retailPrecedenceTotal = retailFrame["precedence"].sum()
    retailRateCardTotal = retailFrame["rateCard"].sum()
    retailRecommendedTotal = retailFrame["recommended"].sum()
    #billing
    brFinalTotalBilling = brFrame["finalBilling"].sum()
    brPrecedenceTotalBilling = brFrame["precedenceBilling"].sum()
    brRateCardTotalBilling = brFrame["rateCardBilling"].sum()
    brRecommendedTotalBilling = brFrame["recommendedBilling"].sum()
    retailSpotFinalTotalBilling = retailSpotBuyFrame["finalBilling"].sum()
    retailSpotPrecedenceTotalBilling = retailSpotBuyFrame["precedenceBilling"].sum()
    retailSpotRecommendedTotalBilling = retailSpotBuyFrame["recommendedBilling"].sum()
    retailSpotRateCardTotalBilling = retailSpotBuyFrame["rateCardBilling"].sum()
    retailFinalTotalBilling = retailFrame["finalBilling"].sum()
    retailPrecedenceTotalBilling = retailFrame["precedenceBilling"].sum()
    retailRateCardTotalBilling = retailFrame["rateCardBilling"].sum()
    retailRecommendedTotalBilling = retailFrame["recommendedBilling"].sum()


    #for spotlight packages by ravinder (2019-05-02)
    spotLightFrameAgg = getChannelFrameSpotLight(gridRec)
    spotLightFrame = spotLightFrameAgg.reset_index()
    #print("spotLight", spotLightFrame)
    spotLightTotal = spotLightFrame["final"].sum()
    spotLightRecommendedTotal = spotLightFrame["recommended"].sum()
    spotLightRateCardTotal = spotLightFrame["rateCard"].sum()
    spotLightPrecedenceTotal = spotLightFrame["precedence"].sum()
    #billing
    spotLightTotalBilling = spotLightFrame["finalBilling"].sum()
    spotLightRecommendedTotalBilling = spotLightFrame["recommendedBilling"].sum()
    spotLightRateCardTotalBilling = spotLightFrame["rateCardBilling"].sum()
    spotLightPrecedenceTotalBilling = spotLightFrame["precedenceBilling"].sum()

    #channelLevelTotal (nonFCT+sponsorship+spotBuy + BR)
    tempRetailFrame = retailFrame.reset_index()
    channelLevelTotalDf = tempSponsorshipFrame.append([tempNonFCTFrame,tempChannelFrameSpotBuy, tempRetailFrame,spotLightFrame])

    #channelLevelTotalDf = channelLevelTotalDf.groupby("channelName").sum()
    #print("Afterchanges",channelLevelTotalDf.columns.values)
    #print("spot",budgetFrame.columns.values)
    channelLevelTotalDf = channelLevelTotalDf.groupby("channelName").sum()
    channelLevelTotalDf = channelLevelTotalDf.reset_index()
    channelLevelTotalDf = pd.merge(channelLevelTotalDf, budgetFrame, on=['channelName'], how='inner')
    #print("totalone",channelLevelTotalDf)
    #channelLevelTotalDf["final"] = channelLevelTotalDf["final"] + channelLevelTotalDf["spotLight"]

    # channelLevelTotalDf["recommended"] = channelLevelTotalDf["recommended"] + channelLevelTotalDf["spotLight"]
    # channelLevelTotalDf["rateCard"] = channelLevelTotalDf["rateCard"] + channelLevelTotalDf["spotLight"]
    #print("indexbefore", channelLevelTotalDf.index.values)
    channelLevelTotalDf = channelLevelTotalDf.groupby("channelName").sum()
    #for total at highest level
    #print("index", channelLevelTotalDf.index.values)
    totalDF = channelLevelTotalDf.sum()
    #print("totaltaol",totalDF)
    totalFinal = totalDF["final"] + budgetDigital
    totalRecommended = totalDF["recommended"] +budgetDigital
    totalPrecedence = totalDF["precedence"]
    totalRateCard = totalDF["rateCard"] +budgetDigital

    #billing
    if excludeDigitalFromBilling:
        totalFinalBilling = totalDF["finalBilling"] + budgetDigital
        totalRecommendedBilling = totalDF["recommendedBilling"] + budgetDigital
        totalRateCardBilling = totalDF["rateCardBilling"] + budgetDigital

    else:
        totalFinalBilling = totalDF["finalBilling"]
        totalRecommendedBilling = totalDF["recommendedBilling"]
        totalRateCardBilling = totalDF["rateCardBilling"]

    totalPrecedenceBilling = totalDF["precedenceBilling"]
    #print("totalfinal",totalFinal)

       #spotBuyEr = (spotBuyFinalTotal/ channelFrameSpotBuy['finalFCT'].sum()) * 10 not a valid number

    ##for alerts
    alertsOutputList=[]
    for i in range(len(erChannelFrameSpotBuy)):

        channel = erChannelFrameSpotBuy["channelName"][i]
        channelLevelAlerts = [x for x in listAlertRules if x.channel in [channel]]
        for j in range(len(channelLevelAlerts)):
            #channel = channelLevelAlerts[j].channel
            metric = channelLevelAlerts[j].metric
            type = channelLevelAlerts[j].type
            compareValue = channelLevelAlerts[j].compareValue
            comparisonOperator = channelLevelAlerts[j].comparisonOperator
            compareTo = channelLevelAlerts[j].compareTo
            threshold = channelLevelAlerts[j].threshold
            standard = channelLevelAlerts[j].standard

            if type == "DEAL":
                if ((compareValue == "SPOT BUYS" or "PT" or "WE") and erChannelFrameSpotBuy["final"][i] != 0):

                    if metric == "ER":
                        finalAmount = erChannelFrameSpotBuy.loc[channel, 'final']
                        compareToAmount = standard if compareTo == 'standard' else erChannelFrameSpotBuy.loc[channel,compareTo]
                        differencePercent = abs(safe_div((finalAmount - compareToAmount)* 100.0, compareToAmount))
                        #print("differece", differencePercent)
                        #print("thres", threshold)
                        if (compareToAmount!=0.0 and  ops[comparisonOperator](finalAmount, compareToAmount) and
                                differencePercent > threshold):
                            alerts = cAlerts(channel, dealId, metric, comparisonOperator, compareValue, compareTo,
                                             threshold, differencePercent, finalAmount, compareToAmount )
                            alertsOutputList.append({"for":channel,'metric':metric, "compareValue":compareValue, \
                                                     "compareTo":compareTo,"content":alerts.returnContent()})
                            #alertsOutputList.append(alerts.__dict__)

                    if metric == "SKEW %":

                        dfName = ptSpotBuy if compareValue == "PT" else weSpotBuy
                        finalAmount = dfName.loc[channel, "final"]*100
                        compareToAmount = standard if compareTo == 'standard' else dfName.loc[channel, compareTo]*100
                        differencePercent = abs(finalAmount - compareToAmount)
                        #print(dfName["final"][i],comparisonOperator, dfName[compareTo][i])
                        #print(ops[comparisonOperator](dfName["final"][i],dfName[compareTo][i]))
                        if (compareToAmount!=0  and ops[comparisonOperator](finalAmount, compareToAmount) and
                                differencePercent > threshold):
                            alerts = cAlerts(channel, dealId, metric, comparisonOperator, compareValue, compareTo,
                                             threshold, differencePercent, finalAmount, compareToAmount)
                            alertsOutputList.append({"for": channel, 'metric': metric, "compareValue": compareValue, \
                                                     "compareTo": compareTo, "content": alerts.returnContent()})
                            #alertsOutputList.append(alerts.__dict__)

                if (compareValue == "TOTAL"):
                    # finalAmount = channelLevelTotalDf["final"][i]
                    # compareToAmount = channelLevelTotalDf[compareTo][i]
                    finalAmount = channelFrameSpotBuy.loc[channel, 'final']
                    #finalAmount = channelFrameSpotBuy["final"][i]
                    compareToAmount = standard if compareTo == 'standard' else channelFrameSpotBuy.loc[channel, compareTo]
                    #print(finalAmount, compareToAmount, channel)
                    #print("compareTo",compareTo, "compareToAmount", compareToAmount,"finalAmount", finalAmount)
                    differencePercent = abs(safe_div((finalAmount - compareToAmount) * 100.0,compareToAmount))
                    if (compareToAmount!=0 and ops[comparisonOperator](finalAmount, compareToAmount) and
                                differencePercent > threshold):
                        alerts = cAlerts(channel, dealId, metric, comparisonOperator, compareValue, compareTo,
                                         threshold, differencePercent, finalAmount, compareToAmount)

                        alertsOutputList.append({"for": channel, 'metric': metric, "compareValue": compareValue, \
                                                 "compareTo": compareTo, "content": alerts.returnContent()})
                        #alertsOutputList.append(alerts.__dict__)
    #for total outlay level alerts
    channelLevelAlerts = [x for x in listAlertRules if x.channel in ["TOTAL"]]
    for i in range(len(channelLevelAlerts)):
        channel = channelLevelAlerts[i].channel
        metric = channelLevelAlerts[i].metric
        type = channelLevelAlerts[i].type
        compareValue = channelLevelAlerts[i].compareValue
        comparisonOperator = channelLevelAlerts[i].comparisonOperator
        compareTo = channelLevelAlerts[i].compareTo
        threshold = channelLevelAlerts[i].threshold

        if compareValue == "TOTAL":
            finalAmount = totalFinal
            #for dynamic rules if in total comparison rule changes
            compareToAmount = totalRecommended if compareTo == "recommended" else (totalPrecedence if compareTo \
                                 == "precedence" else totalRateCard)
            #print("COM",compareValue,"compare:",compareToAmount)

            differencePercent = abs(safe_div((finalAmount - compareToAmount) * 100.0, compareToAmount))
            if (compareToAmount!=0 and ops[comparisonOperator](finalAmount, compareToAmount) and
                                differencePercent > threshold):
                alerts = cAlerts(channel, dealId, metric, comparisonOperator, compareValue, compareTo,
                                 threshold, differencePercent, finalAmount, compareToAmount)

                alertsOutputList.append({"for": "TOTAL", 'metric': metric, "compareValue": compareValue, \
                                         "compareTo": compareTo, "content": alerts.returnContent()})

    ##single additional alert for ask outlay and final outlay after optimisation
    category = askDF.loc[0, "category.name"]
    if category in ["POLITICAL","CLASSIFIED","ALLIANCE","PRACHAR","AD WISER"]:
        finalOutlayBucket = "0+"
        budgetOutlayBucket ="0+"
    elif category == "COMBO":
        comboName = askDF.loc[0, "combo.comboName"]
        primaryChannelDF = pd.DataFrame(list(dbComboMstr.find({"name": comboName}, {"primaryChannel": 1})))
        primaryChannel = primaryChannelDF.loc[0, "primaryChannel"]
        totalFinalExcRetail = totalFinal - brFinalTotal - retailSpotFinalTotal
        finalOutlayBucket = getDealOutlayBucket(totalFinalExcRetail, primaryChannel, category,dbRateRules)
        budgetOutlayBucket = askDF.loc[0, "budgetIncentive.dealOutlayBucket"]

    else:
        primaryChannel = askDF.loc[0, "budgetIncentive.primaryChannel"]
        budgetOutlay = askDF.loc[0, "totalOutlay"]
        print("totalOutlay", budgetOutlay)
        totalFinalExcRetail = totalFinal - brFinalTotal - retailSpotFinalTotal
        finalOutlayBucket = getDealOutlayBucket(totalFinalExcRetail, primaryChannel, category,dbRateRules)
        budgetOutlayBucket = getDealOutlayBucket(budgetOutlay, primaryChannel, category,dbRateRules)

    if finalOutlayBucket != budgetOutlayBucket:
        alertsOutputList.append({"for": "TOTAL", 'metric': "OUTLAY", "compareValue": "TOTAL",
                                 "compareTo": "BUDGET OUTLAY", "content": "BUDGET OUTLAY" + "(" + budgetOutlayBucket + \
                                ") AND FINAL OUTLAY (" + finalOutlayBucket + ") BUCKETS ARE DIFFERENT"})

    # new changes made
    totalRateCardExcSpotBudg = totalRateCard  - budgetDigital
    totalOutlayExcSpotBudg = totalFinal - budgetDigital
    fromDate = brief['fromDate']
    print('fromDate->', fromDate)
    discounts = dbDiscounts.find_one({'fromDate': {"$lte": fromDate},'toDate':{"$gt":fromDate}})
    print('deviation discounts here')
    print(discounts)
    try:
        TL = discounts['discounts']['TL']
        RD = discounts['discounts']['RD']
        VH = discounts['discounts']['VH']
        CRO = discounts['discounts']['CRO']
    except:
        TL = 5
        RD = 7
        VH = 7
        CRO = 0
    if spotLightTotal > 0:
        VH = 7
    else:
        VH = discounts['discounts']['VH']
    discTL = TL/100
    discRD = RD/100
    discVH = VH/100
    discCRO = CRO/100
    print('TL RD VH CRO', discTL,discRD,discVH,discCRO)
    discTLPer =(1 - discTL)
    discRDPer = (1 - discRD)
    discVHPer = (1 - discVH)
    discCROPer = (1 - discCRO)
    rateCardTL = totalRateCardExcSpotBudg * discTLPer
    rateCardRD = totalRateCardExcSpotBudg * discTLPer * discRDPer
    rateCardVH = totalRateCardExcSpotBudg * discTLPer * discRDPer * discVHPer
    rateCardCRO = totalRateCardExcSpotBudg * discTLPer * discRDPer * discVHPer * discCROPer
    deviationTL = safe_div((totalRateCardExcSpotBudg - totalOutlayExcSpotBudg), totalRateCardExcSpotBudg)
    deviationRD = safe_div((rateCardTL - totalOutlayExcSpotBudg), rateCardTL)
    deviationVH = safe_div((rateCardRD - totalOutlayExcSpotBudg), rateCardRD)
    deviationCRO = safe_div((rateCardVH - totalOutlayExcSpotBudg), rateCardVH)
    if (deviationTL < 0) :
        discountTL = 0
    elif (deviationTL > discTL):
        discountTL = discTL
    else:
        discountTL = deviationTL

    if (deviationRD < 0):
        discountRD = 0
    elif (deviationRD > discRD):
        discountRD = discRD
    else:
        discountRD = deviationRD

    if (deviationVH < 0):
        discountVH = 0
    elif (deviationVH > discVH):
        discountVH = discVH
    else:
        discountVH = deviationVH

    if (deviationCRO < 0):
        discountCRO = 0
    else:
        discountCRO = deviationCRO
    
    print(discountTL,discountRD,discountVH,discountCRO)

    #added for discount values - old code
    '''
    totalRateCardExcSpotBudg = totalRateCard  - budgetDigital
    totalOutlayExcSpotBudg = totalFinal - budgetDigital
    discTL = 0.07
    discRD = 0.1
    discVH = 0.1
    discTLPer =(1 - discTL)
    discRDPer = (1 - discRD)
    discVHPer = (1 - discVH)
    rateCardTL = totalRateCardExcSpotBudg * discTLPer
    rateCardRD = totalRateCardExcSpotBudg * discTLPer * discRDPer
    rateCardVH = totalRateCardExcSpotBudg * discTLPer * discRDPer * discVHPer
    deviationTL = safe_div((totalRateCardExcSpotBudg - totalOutlayExcSpotBudg), totalRateCardExcSpotBudg)
    deviationRD = safe_div((rateCardTL - totalOutlayExcSpotBudg), rateCardTL)
    deviationVH = safe_div((rateCardRD - totalOutlayExcSpotBudg), rateCardRD)
    if (deviationTL < 0) :
        discountTL = 0
    elif (deviationTL > discTL):
        discountTL = discTL
    else:
        discountTL = deviationTL

    if (deviationRD < 0):
        discountRD = 0
    elif (deviationRD > discRD):
        discountRD = discRD
    else:
        discountRD = deviationRD

    if (deviationVH < 0):
        discountVH = 0
    else:
        discountVH = deviationVH
    '''



    successflag = dbSummary.update({'dealId':dealId},
                                   {'$set': {
         'deal.digital':budgetDigital,
         'deal.spotBuy.total.final':spotBuyFinalTotal,
         'deal.spotBuy.total.precedence':spotBuyPrecedencePriceTotal,
         'deal.spotBuy.total.rateCard': spotBuyRateCardPriceTotal,
         'deal.spotBuy.total.recoPrice':spotBuyRecommendedPriceTotal,
         'deal.spotBuy.total.regionOR': spotBuyRegionORTotal,
         'deal.spotBuy.total.nationalOR': spotBuyNationalORTotal,
         'deal.spotBuy.total.prevApproved': preSummaryDealLevel["spotBuy"]["total"]["final"],
         'deal.spotBuyBill.total.final': spotBuyFinalTotalBilling,
        'deal.spotBuyBill.total.precedence': spotBuyPrecedencePriceTotalBilling,
        'deal.spotBuyBill.total.rateCard': spotBuyRateCardPriceTotalBilling,
        'deal.spotBuyBill.total.recoPrice': spotBuyRecommendedPriceTotalBilling,
        'deal.spotBuyBill.total.regionOR': spotBuyRegionORTotalBilling,
        'deal.spotBuyBill.total.nationalOR': spotBuyNationalORTotalBilling,
        'deal.spotBuyBill.total.prevApproved': preSummaryDealLevel["spotBuy"]["total"]["final"],
         'deal.total.final' : totalFinal,
         'deal.total.recoPrice' : totalRecommended,
         'deal.total.rateCard' : totalRateCard,
         'deal.total.precedence' : totalPrecedence,
         'deal.total.prevApproved': preSummaryDealLevel["total"]["final"],
         'deal.total.discountTL' : discountTL,
         'deal.total.discountRD': discountRD,
         'deal.total.discountVH': discountVH,
         'deal.total.discountCRO': discountCRO,
        'deal.totalBill.final': totalFinalBilling,
       'deal.totalBill.recoPrice': totalRecommendedBilling,
       'deal.totalBill.rateCard': totalRateCardBilling,
       'deal.totalBill.precedence': totalPrecedenceBilling,
         'deal.spotBuy.er.final' : erSpotBuyFinalTotal,
         'deal.spotBuy.er.precedence': erSpotBuyPrecedenceTotal,
         'deal.spotBuy.er.recoPrice': erSpotBuyRecommendedTotal,
         'deal.spotBuy.er.rateCard': erSpotBuyRateCardTotal,
         'deal.spotBuy.er.prevApproved': preSummaryDealLevel["spotBuy"]["er"]["final"],
       'deal.spotBuyBill.er.final': erSpotBuyFinalTotalBilling,
       'deal.spotBuyBill.er.precedence': erSpotBuyPrecedenceTotalBilling,
       'deal.spotBuyBill.er.recoPrice': erSpotBuyRecommendedTotalBilling,
       'deal.spotBuyBill.er.rateCard': erSpotBuyRateCardTotalBilling,
       'deal.spotBuyBill.er.prevApproved': preSummaryDealLevel["spotBuy"]["er"]["final"],
         'deal.spotBuy.ptSkew.final' : ptSkewFinalTotal,
         'deal.spotBuy.ptSkew.precedence' : ptSkewPrecedenceTotal,
         'deal.spotBuy.ptSkew.preApproved' : preSummaryDealLevel["spotBuy"]["ptSkew"]["final"],
         'deal.spotBuy.weSkew.final' : weSkewFinalTotal,
         'deal.spotBuy.weSkew.precedence' : weSkewPrecedenceTotal,
         'deal.spotBuy.weSkew.prevApproved': preSummaryDealLevel["spotBuy"]["weSkew"]["final"],
         'deal.spotBuy.mtSkew.final': mtSkewFinalTotal,
         'deal.spotBuy.mtSkew.precedence': mtSkewPrecedenceTotal,
         'deal.spotBuy.mtSkew.prevApproved': preSummaryDealLevel["spotBuy"]["mtSkew"]["final"],
         'deal.sponsorship.final' : sponsorshipFinalTotal,
         'deal.sponsorship.recoPrice': sponsorshipRecommendedTotal,
         'deal.sponsorship.rateCard': sponsorshipRateCardTotal,
         'deal.sponsorship.precedence': sponsorshipPrecedenceTotal,
       'deal.sponsorshipBill.final': sponsorshipFinalTotalBilling,
       'deal.sponsorshipBill.recoPrice': sponsorshipRecommendedTotalBilling,
       'deal.sponsorshipBill.rateCard': sponsorshipRateCardTotalBilling,
       'deal.sponsorshipBill.precedence': sponsorshipPrecedenceTotalBilling,
         'deal.sponsorshipBill.prevApproved': preSummaryDealLevel["sponsorship"]["final"],
         'deal.nonFCT.final' : nonFCTFinalTotal,
         'deal.nonFCT.recoPrice' : nonFCTRecommendedTotal,
         'deal.nonFCT.rateCard' : nonFCTRateCardTotal,
         'deal.nonFCT.precedence' : nonFCTPrecedenceTotal,
         'deal.nonFCT.prevApproved': preSummaryDealLevel["nonFCT"]["final"],
       'deal.nonFCTBill.final': nonFCTFinalTotalBilling,
       'deal.nonFCTBill.recoPrice': nonFCTRecommendedTotalBilling,
       'deal.nonFCTBill.rateCard': nonFCTRateCardTotalBilling,
       'deal.nonFCTBill.precedence': nonFCTPrecedenceTotalBilling,
       'deal.nonFCTBill.prevApproved': preSummaryDealLevel["nonFCT"]["final"],
         'deal.retailBR.final' : brFinalTotal,
         'deal.retailBR.recoPrice' : brRecommendedTotal,
         'deal.retailBR.rateCard' : brRateCardTotal,
         'deal.retailBR.precedence' : brPrecedenceTotal,
         'deal.retailBR.prevApproved':preSummaryDealLevel["retailBR"]["final"],
       'deal.retailBRBill.final': brFinalTotalBilling,
       'deal.retailBRBill.recoPrice': brRecommendedTotalBilling,
       'deal.retailBRBill.rateCard': brRateCardTotalBilling,
       'deal.retailBRBill.precedence': brPrecedenceTotalBilling,
       'deal.retailBRBill.prevApproved': preSummaryDealLevel["retailBR"]["final"],
         'deal.retailSpotBuy.final' : retailSpotFinalTotal,
         'deal.retailSpotBuy.recoPrice' : retailSpotRecommendedTotal,
         'deal.retailSpotBuy.rateCard' : retailSpotRateCardTotal,
         'deal.retailSpotBuy.precedence' : retailSpotPrecedenceTotal,
         'deal.retailSpotBuy.prevApproved' : preSummaryDealLevel["retailSpotBuy"]["final"],
       'deal.retailSpotBuyBill.final': retailSpotFinalTotalBilling,
       'deal.retailSpotBuyBill.recoPrice': retailSpotRecommendedTotalBilling,
       'deal.retailSpotBuyBill.rateCard': retailSpotRateCardTotalBilling,
       'deal.retailSpotBuyBill.precedence': retailSpotPrecedenceTotalBilling,
       'deal.retailSpotBuyBill.prevApproved': preSummaryDealLevel["retailSpotBuy"]["final"],
         'deal.retail.final':retailFinalTotal,
         'deal.retail.recoPrice': retailRecommendedTotal,
         'deal.retail.rateCard': retailRateCardTotal,
         'deal.retail.precedence': retailPrecedenceTotal,
         'deal.retail.prevApproved': preSummaryDealLevel["retail"]["final"],
       'deal.retailBill.final': retailFinalTotalBilling,
       'deal.retailBill.recoPrice': retailRecommendedTotalBilling,
       'deal.retailBill.rateCard': retailRateCardTotalBilling,
       'deal.retailBill.precedence': retailPrecedenceTotalBilling,
       'deal.retailBill.prevApproved': preSummaryDealLevel["retail"]["final"],
       'deal.spotlight.final': spotLightTotal,
       'deal.spotlight.recoPrice': spotLightRecommendedTotal,
       'deal.spotlight.rateCard': spotLightRateCardTotal,
       'deal.spotlight.precedence': spotLightPrecedenceTotal,
       'deal.spotlight.prevApproved': preSummaryDealLevel["spotlight"]["final"],
       'deal.spotlightBill.final': spotLightTotalBilling,
       'deal.spotlightBill.recoPrice': spotLightRecommendedTotalBilling,
       'deal.spotlightBill.rateCard': spotLightRateCardTotalBilling,
       'deal.spotlightBill.precedence': spotLightPrecedenceTotalBilling,
       'deal.spotlight.prevApproved': preSummaryDealLevel["spotlight"]["final"],
        "channels":[], "alerts": alertsOutputList}},upsert=True)

    #update summaryOutlay in briefTxn
    dbBrief.update({'dealId': dealId},
                     {'$set': {
                         'summaryOutlay': totalFinal,
                         'prevApprovedOutlay': preSummaryDealLevel["total"]["final"]}})

    for i in range(len(channelList)):

        dbGrid.update({'dealId': dealId},
                     {'$set': {
                          "channel."+str(i) + ".spotBuy.final.discountTL" : discountTL,
                          "channel."+str(i) + ".spotBuy.final.discountRD" : discountRD,
                           "channel."+str(i) + ".spotBuy.final.discountVH" : discountVH,
                            "channel."+str(i) + ".spotBuy.final.discountCRO" : discountCRO
                        # "channel."+str(i) + ".ticker.discountTL" : discountTL,
                        #   "channel."+str(i) + ".ticker.discountRD" : discountRD,
                        #    "channel."+str(i) + ".ticker.discountVH" : discountVH,
                        #     "channel."+str(i) + ".ticker.discountCRO" : discountCRO
                         }})

    #for channel in channelList:
    #     print ('****',dealId, channel,channelLevelTotalDf.loc[channel,"final"])

    #
        dbSummary.update({"dealId": dealId},
                           {'$set': {"channels."+str(i)+".name": channelList[i],
                               "channels."+str(i)+".total.final" : channelLevelTotalDf.final.get(channelList[i],0.0),
                                      "channels."+str(i) + ".total.precedence" : channelLevelTotalDf.precedence.get(channelList[i],0.0),
                                      "channels."+str(i) + ".total.recoPrice": channelLevelTotalDf.recommended.get(channelList[i],0.0),
                                     "channels." + str(i) + ".total.rateCard": channelLevelTotalDf.rateCard.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".total.prevApproved": preSummaryChannelDF.totFinal.get(channelList[i],None),
                                     "channels." + str(i) + ".totalBill.final": channelLevelTotalDf.finalBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".totalBill.precedence": channelLevelTotalDf.precedenceBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".totalBill.recoPrice": channelLevelTotalDf.recommendedBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".totalBill.rateCard": channelLevelTotalDf.rateCardBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".totalBill.prevApproved": preSummaryChannelDF.totFinal.get(channelList[i], None),
                                     "channels."+str(i) + ".spotBuy.total.final" : channelFrameSpotBuy.final.get(channelList[i],0.0),
                                     "channels."+str(i) + ".spotBuy.total.precedence": channelFrameSpotBuy.precedence.get(channelList[i],0.0),
                                     "channels."+str(i) + ".spotBuy.total.recoPrice": channelFrameSpotBuy.recommended.get(channelList[i],0.0),
                                     "channels."+str(i) + ".spotBuy.total.rateCard": channelFrameSpotBuy.rateCard.get(channelList[i],0.0),
                                     "channels." + str(i) + ".spotBuy.total.regionOR": channelFrameSpotBuy.regionOR.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".spotBuy.total.nationalOR": channelFrameSpotBuy.nationalOR.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".spotBuy.total.prevApproved": preSummaryChannelDF.spotBuyTotFinal.get(channelList[i], None),
                                     "channels." + str(i) + ".spotBuyBill.total.final": channelFrameSpotBuy.finalBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".spotBuyBill.total.precedence": channelFrameSpotBuy.precedenceBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".spotBuyBill.total.recoPrice": channelFrameSpotBuy.recommendedBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".spotBuyBill.total.rateCard": channelFrameSpotBuy.rateCardBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".spotBuyBill.total.regionOR": channelFrameSpotBuy.regionORBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".spotBuyBill.total.nationalOR": channelFrameSpotBuy.nationalORBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".spotBuyBill.total.prevApproved": preSummaryChannelDF.spotBuyTotFinal.get(channelList[i], None),
                                      "channels." + str(i) + ".spotBuy.er.final": erChannelFrameSpotBuy.final.get(channelList[i],0.0),
                                      "channels." + str(i) + ".spotBuy.er.precedence": erChannelFrameSpotBuy.precedence.get(channelList[i],0.0),
                                      "channels." + str(i) + ".spotBuy.er.recoPrice": erChannelFrameSpotBuy.recommended.get(channelList[i],0.0),
                                      "channels." + str(i) + ".spotBuy.er.rateCard": erChannelFrameSpotBuy.rateCard.get(channelList[i],0.0),
                                     "channels." + str(i) + ".spotBuy.er.prevApproved": preSummaryChannelDF.spotBuyERFinal.get(channelList[i], None),
                                     "channels." + str(i) + ".spotBuyBill.er.final": erChannelFrameSpotBuy.finalBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".spotBuyBill.er.precedence": erChannelFrameSpotBuy.precedenceBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".spotBuyBill.er.recoPrice": erChannelFrameSpotBuy.recommendedBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".spotBuyBill.er.rateCard": erChannelFrameSpotBuy.rateCardBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".spotBuyBill.er.prevApproved": preSummaryChannelDF.spotBuyERFinal.get(channelList[i], None),
                                      "channels." + str(i) + ".spotBuy.ptSkew.final": ptSpotBuy.final.get(channelList[i],0.0),
                                     "channels." + str(i) + ".spotBuy.ptSkew.precedence": ptSpotBuy.precedence.get(channelList[i],0.0),
                                     "channels." + str(i) + ".spotBuy.ptSkew.prevApproved": preSummaryChannelDF.spotBuyPTFinal.get(channelList[i], None),
                                      "channels." + str(i) + ".spotBuy.weSkew.final": weSpotBuy.final.get(channelList[i],0.0),
                                     "channels." + str(i) + ".spotBuy.weSkew.precedence": weSpotBuy.precedence.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".spotBuy.weSkew.prevApproved": preSummaryChannelDF.spotBuyWEFinal.get(channelList[i], None),
                                     "channels." + str(i) + ".spotBuy.mtSkew.final": mtSpotBuy.final.get(channelList[i],0.0),
                                     "channels." + str(i) + ".spotBuy.mtSkew.precedence": mtSpotBuy.precedence.get(channelList[i],0.0),
                                     "channels." + str(i) + ".spotBuy.mtSkew.prevApproved": preSummaryChannelDF.spotBuyMTFinal.get(channelList[i], None),
                                      "channels." + str(i) + ".sponsorship.final" : sponsorshipFrame.final.get(channelList[i],0.0),
                                      "channels." + str(i) + ".sponsorship.precedence": sponsorshipFrame.precedence.get(channelList[i],0.0),
                                      "channels." + str(i) + ".sponsorship.recoPrice": sponsorshipFrame.recommended.get(channelList[i],0.0),
                                     "channels." + str(i) + ".sponsorship.rateCard": sponsorshipFrame.rateCard.get(channelList[i],0.0),
                                     "channels." + str(i) + ".sponsorship.prevApproved": preSummaryChannelDF.sponsorshipFinal.get(channelList[i], None),
                                     "channels." + str(i) + ".sponsorshipBill.final": sponsorshipFrame.finalBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".sponsorshipBill.precedence": sponsorshipFrame.precedenceBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".sponsorshipBill.recoPrice": sponsorshipFrame.recommendedBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".sponsorshipBill.rateCard": sponsorshipFrame.rateCardBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".sponsorshipBill.prevApproved": preSummaryChannelDF.sponsorshipFinal.get(channelList[i], None),
                                      "channels." + str(i) + ".nonFCT.final": nonFCTFrame.final.get(channelList[i],0.0),
                                      "channels." + str(i) + ".nonFCT.precedence": nonFCTFrame.precedence.get(channelList[i],0.0),
                                      "channels." + str(i) + ".nonFCT.recoPrice": nonFCTFrame.recommended.get(channelList[i],0.0),
                                      "channels." + str(i) + ".nonFCT.rateCard": nonFCTFrame.rateCard.get(channelList[i],0.0),
                                     "channels." + str(i) + ".nonFCT.prevApproved": preSummaryChannelDF.nonFCTFinal.get(channelList[i], None),
                                     "channels." + str(i) + ".nonFCTBill.final": nonFCTFrame.finalBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".nonFCTBill.precedence": nonFCTFrame.precedenceBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".nonFCTBill.recoPrice": nonFCTFrame.recommendedBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".nonFCTBill.rateCard": nonFCTFrame.rateCardBilling.get(channelList[i],0.0),
                                     "channels." + str(i) + ".nonFCTBill.prevApproved": preSummaryChannelDF.nonFCTFinal.get(channelList[i], None),
                                     "channels." + str(i) + ".nonFCTMonthly.rateCard": nonFCTMonFrame.rateCard.get(channelList[i],0.0),
                                     "channels." + str(i) + ".nonFCTMonthly.precedence": nonFCTMonFrame.precedence.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".nonFCTMonthly.recoPrice": nonFCTMonFrame.recommended.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".nonFCTMonthly.final": nonFCTMonFrame.final.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".nonFCTMonthly.prevApproved": preSummaryChannelDF.nonFCTMonFinal.get(channelList[i], None),
                                     "channels." + str(i) + ".nonFCTMonthlyBill.rateCard": nonFCTMonFrame.rateCardBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".nonFCTMonthlyBill.precedence": nonFCTMonFrame.precedenceBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".nonFCTMonthlyBill.recoPrice": nonFCTMonFrame.recommendedBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".nonFCTMonthlyBill.final": nonFCTMonFrame.finalBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".nonFCTMonthlyBill.prevApproved": preSummaryChannelDF.nonFCTMonFinal.get(channelList[i], None),
                                     "channels." + str(i) + ".nonFCTExposure.rateCard": nonFCTExpFrame.rateCard.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".nonFCTExposure.precedence": nonFCTExpFrame.precedence.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".nonFCTExposure.recoPrice": nonFCTExpFrame.recommended.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".nonFCTExposure.final": nonFCTExpFrame.final.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".nonFCTExposure.prevApproved": preSummaryChannelDF.nonFCTExpFinal.get(channelList[i], None),
                                     "channels." + str(i) + ".nonFCTExposureBill.rateCard": nonFCTExpFrame.rateCardBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".nonFCTExposureBill.precedence": nonFCTExpFrame.precedenceBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".nonFCTExposureBill.recoPrice": nonFCTExpFrame.recommendedBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".nonFCTExposureBill.final": nonFCTExpFrame.finalBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".nonFCTExposureBill.prevApproved": preSummaryChannelDF.nonFCTExpFinal.get(channelList[i], None),
                                     "channels." + str(i) + ".ticker.rateCard": tickerFrame.rateCard.get(channelList[i],0.0),
                                     "channels." + str(i) + ".ticker.precedence": tickerFrame.precedence.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".ticker.recoPrice": tickerFrame.recommended.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".ticker.final": tickerFrame.final.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".ticker.prevApproved": preSummaryChannelDF.tickerFinal.get(channelList[i], None),
                                     "channels." + str(i) + ".tickerBill.rateCard": tickerFrame.rateCardBilling.get(channelList[i],0.0),
                                     "channels." + str(i) + ".tickerBill.precedence": tickerFrame.precedenceBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".tickerBill.recoPrice": tickerFrame.recommendedBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".tickerBill.final": tickerFrame.finalBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".tickerBill.prevApproved": preSummaryChannelDF.tickerFinal.get(channelList[i], None),
                                     "channels." + str(i) + ".politicalLBand.rateCard": lBandFrame.rateCard.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".politicalLBand.precedence": lBandFrame.precedence.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".politicalLBand.recoPrice": lBandFrame.recommended.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".politicalLBand.final": lBandFrame.final.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".politicalLBand.prevApproved": preSummaryChannelDF.lBandFinal.get(channelList[i], None),
                                     "channels." + str(i) + ".politicalLBandBill.rateCard": lBandFrame.rateCardBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".politicalLBandBill.precedence": lBandFrame.precedenceBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".politicalLBandBill.recoPrice": lBandFrame.recommendedBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".politicalLBandBill.final": lBandFrame.finalBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".politicalLBandBill.prevApproved": preSummaryChannelDF.lBandFinal.get(channelList[i], None),
                                     "channels." + str(i) + ".politicalAston.rateCard": astonFrame.rateCard.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".politicalAston.precedence": astonFrame.precedence.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".politicalAston.recoPrice": astonFrame.recommended.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".politicalAston.final": astonFrame.final.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".politicalAston.prevApproved": preSummaryChannelDF.astonFinal.get(channelList[i], None),
                                      "channels." + str(i) + ".spotBuy.totalFCT": channelFrameSpotBuy.fct.get(channelList[i],0.0),
                                     "channels." + str(i) + ".sponsorship.totalFCT": sponsorshipFrame.fct.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".politicalAstonBill.rateCard": astonFrame.rateCardBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".politicalAstonBill.precedence": astonFrame.precedenceBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".politicalAstonBill.recoPrice": astonFrame.recommendedBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".politicalAstonBill.final": astonFrame.finalBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".politicalAstonBill.prevApproved": preSummaryChannelDF.astonFinal.get(channelList[i], None),
                                     "channels." + str(i) + ".spotBuyBill.totalFCT": channelFrameSpotBuy.fctBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".sponsorshipBill.totalFCT": sponsorshipFrame.fctBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRLBand.rateCard": brLBandFrame.rateCard.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRLBand.precedence": brLBandFrame.precedence.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRLBand.recoPrice": brLBandFrame.recommended.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRLBand.final": brLBandFrame.final.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRLBand.prevApproved": preSummaryChannelDF.brLBandFinal.get(channelList[i], None),
                                     "channels." + str(i) + ".retailBRLBandBill.rateCard": brLBandFrame.rateCardBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRLBandBill.precedence": brLBandFrame.precedenceBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRLBandBill.recoPrice": brLBandFrame.recommendedBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRLBandBill.final": brLBandFrame.finalBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRLBandBill.prevApproved": preSummaryChannelDF.brLBandFinal.get(channelList[i], None),
                                     "channels." + str(i) + ".retailBRAston.rateCard": brAstonFrame.rateCard.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRAston.precedence": brAstonFrame.precedence.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRAston.recoPrice": brAstonFrame.recommended.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRAston.final": brAstonFrame.final.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRAston.prevApproved": preSummaryChannelDF.brAstonFinal.get(channelList[i], None),
                                     "channels." + str(i) + ".retailBRAstonBill.rateCard": brAstonFrame.rateCardBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRAstonBill.precedence": brAstonFrame.precedenceBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRAstonBill.recoPrice": brAstonFrame.recommendedBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRAstonBill.final": brAstonFrame.finalBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRAstonBill.prevApproved": preSummaryChannelDF.brAstonFinal.get(channelList[i], None),
                                     "channels." + str(i) + ".retailBRSpotBuy.rateCard": brSpotBuyFrame.rateCard.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRSpotBuy.precedence": brSpotBuyFrame.precedence.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRSpotBuy.recoPrice": brSpotBuyFrame.recommended.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRSpotBuy.final": brSpotBuyFrame.final.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRSpotBuy.prevApproved": preSummaryChannelDF.brSpotBuyFinal.get(channelList[i], None),
                                     "channels." + str(i) + ".retailBRSpotBuyBill.rateCard": brSpotBuyFrame.rateCardBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRSpotBuyBill.precedence": brSpotBuyFrame.precedenceBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRSpotBuyBill.recoPrice": brSpotBuyFrame.recommendedBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRSpotBuyBill.final": brSpotBuyFrame.finalBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRSpotBuyBill.prevApproved": preSummaryChannelDF.brSpotBuyFinal.get(channelList[i], None),
                                     "channels." + str(i) + ".retailSpotBuy.rateCard": retailSpotBuyFrame.rateCard.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailSpotBuy.precedence": retailSpotBuyFrame.precedence.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailSpotBuy.recoPrice": retailSpotBuyFrame.recommended.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailSpotBuy.final": retailSpotBuyFrame.final.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailSpotBuy.prevApproved": preSummaryChannelDF.retailSpotBuyFinal.get(channelList[i], None),
                                     "channels." + str(i) + ".retailSpotBuyBill.rateCard": retailSpotBuyFrame.rateCardBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailSpotBuyBill.precedence": retailSpotBuyFrame.precedenceBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailSpotBuyBill.recoPrice": retailSpotBuyFrame.recommendedBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailSpotBuyBill.final": retailSpotBuyFrame.finalBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailSpotBuyBill.prevApproved": preSummaryChannelDF.retailSpotBuyFinal.get(channelList[i], None),
                                     "channels." + str(i) + ".retailBR.rateCard": brFrame.rateCard.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBR.precedence": brFrame.precedence.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBR.recoPrice": brFrame.recommended.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBR.final": brFrame.final.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBR.prevApproved": preSummaryChannelDF.brFinal.get(channelList[i], None),
                                     "channels." + str(i) + ".retailBRBill.rateCard": brFrame.rateCardBilling.get(channelList[i],0.0),
                                     "channels." + str(i) + ".retailBRBill.precedence": brFrame.precedenceBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRBill.recoPrice": brFrame.recommendedBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRBill.final": brFrame.finalBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBRBill.prevApproved": preSummaryChannelDF.brFinal.get(channelList[i],None),
                                     "channels." + str(i) + ".retail.rateCard": retailFrame.rateCard.get(channelList[i],0.0),
                                     "channels." + str(i) + ".retail.precedence": retailFrame.precedence.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retail.recoPrice": retailFrame.recommended.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retail.final": retailFrame.final.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retail.prevApproved": preSummaryChannelDF.retailFinal.get(channelList[i], None),
                                     "channels." + str(i) + ".retailBill.rateCard": retailFrame.rateCardBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBill.precedence": retailFrame.precedenceBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBill.recoPrice": retailFrame.recommendedBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBill.final": retailFrame.finalBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".retailBill.prevApproved": preSummaryChannelDF.retailFinal.get(channelList[i], None),
                                     "channels." + str(i) + ".spotlight.rateCard": spotLightFrameAgg.rateCard.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".spotlight.precedence": spotLightFrameAgg.precedence.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".spotlight.recoPrice": spotLightFrameAgg.recommended.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".spotlight.final": spotLightFrameAgg.final.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".spotlight.prevApproved": preSummaryChannelDF.spotLightFinal.get(channelList[i], None),
                                     "channels." + str(i) + ".spotlightBill.rateCard": spotLightFrameAgg.rateCardBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".spotlightBill.precedence": spotLightFrameAgg.precedenceBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".spotlightBill.recoPrice": spotLightFrameAgg.recommendedBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".spotlightBill.final": spotLightFrameAgg.finalBilling.get(channelList[i], 0.0),
                                     "channels." + str(i) + ".spotlightBill.prevApproved": preSummaryChannelDF.spotLightFinal.get(channelList[i], None)
                                     }})

    # for alerts in alertsOutputList:
    #     dbAlertsTxn.update({"dealId":i.dealId},{"$set":\
    #                                                 {"channel.$.name":i.channel}})

    print("*******Success*******")
    return jsonify({'success': 'success'})


def safe_div(x,y):
    #print("billing",y)
    if y in [0,None]:
        return 0
    return round((x / y),7)

# for IPO category for NEWSASMITAGANGA spotBuy
def getIPOFrame3Channels(gridRec):
    channelList = []
    channelFrameIPO3ChannelAgg = pd.DataFrame()
    channelFrame = pd.DataFrame(list(gridRec['channel'])).fillna(0.0)
    for i in range(len(channelFrame)):
        channelFrameIPO3Channel = pd.DataFrame()
        channelName = channelFrame.iloc[i]['name']
        channelList.append(channelName)
        timebandFrame_ps = pd.DataFrame(list(channelFrame.iloc[i]['NEWSASMITAGANGAspotBuy']['timebands']))
        timebandFrame_ps["valueAdd"] = False if "valueAdd" not in timebandFrame_ps.columns else timebandFrame_ps["valueAdd"]
        if (len(timebandFrame_ps) == 0 or len(timebandFrame_ps[timebandFrame_ps["valueAdd"] != True]) == 0):
            channelFrameIPO3Channel.loc[(channelName + str(0)), 'channelName'] = channelName
            channelFrameIPO3Channel.loc[(channelName + str(0)), 'recommended'] = 0.0
            channelFrameIPO3Channel.loc[(channelName + str(0)), 'rateCard'] = 0.0
            channelFrameIPO3Channel.loc[(channelName + str(0)), 'final'] = 0.0
            channelFrameIPO3Channel.loc[(channelName + str(0)), 'precedence'] = 0.0
            channelFrameIPO3Channel.loc[(channelName + str(0)), 'fct'] = 0.0
            channelFrameIPO3Channel.loc[(channelName + str(0)), 'recommendedBilling'] = 0.0
            channelFrameIPO3Channel.loc[(channelName + str(0)), 'rateCardBilling'] = 0.0
            channelFrameIPO3Channel.loc[(channelName + str(0)), 'finalBilling'] = 0.0
            channelFrameIPO3Channel.loc[(channelName + str(0)), 'precedenceBilling'] = 0.0
            channelFrameIPO3Channel.loc[(channelName + str(0)), 'fctBilling'] = 0.0
        for j in range(len(timebandFrame_ps)):
            channelFrameIPO3Channel.loc[(channelName + str(j)), 'channelName'] = channelName
            if timebandFrame_ps.iloc[j]['valueAdd'] != True:
                try:
                    finalFCT = timebandFrame_ps.iloc[j]['finalFCT'] or 0.0
                except:
                    finalFCT = 0.0
                try:
                    channelFrameIPO3Channel.loc[(channelName + str(j)), 'recommended'] = timebandFrame_ps.iloc[j][
                                                                                          'recommendedPrice'] * finalFCT/10.0
                except:
                    channelFrameIPO3Channel.loc[(channelName + str(j)), 'recommended'] = 0.0
                try:
                    channelFrameIPO3Channel.loc[(channelName + str(j)), 'rateCard'] = timebandFrame_ps.iloc[j][
                                                                                       'rateCardPrice'] * finalFCT/10.0
                except:
                    channelFrameIPO3Channel.loc[(channelName + str(j)), 'rateCard'] = 0.0
                try:
                    channelFrameIPO3Channel.loc[(channelName + str(j)), 'final'] = timebandFrame_ps.iloc[j]['finalPrice'] * finalFCT/10.0
                except:
                    channelFrameIPO3Channel.loc[(channelName + str(j)), 'final'] = 0.0
                try:
                    channelFrameIPO3Channel.loc[(channelName + str(j)), 'precedence'] = timebandFrame_ps.iloc[j]['previousPrice'] * finalFCT/10.0
                except:
                    channelFrameIPO3Channel.loc[(channelName + str(j)), 'precedence'] = 0.0

                channelFrameIPO3Channel.loc[(channelName + str(j)), 'fct'] = finalFCT
            # Changed value add condition for billing - no impact if value add is true/false
            try:
                billingFCT = timebandFrame_ps.iloc[j]['billingFCT'] or 0.0
            except:
                billingFCT = 0.0
            try:
                channelFrameIPO3Channel.loc[(channelName + str(j)), 'recommendedBilling'] = timebandFrame_ps.iloc[j][
                                                                                                'recommendedPrice'] * billingFCT / 10.0
            except:
                channelFrameIPO3Channel.loc[(channelName + str(j)), 'recommendedBilling'] = 0.0
            try:
                channelFrameIPO3Channel.loc[(channelName + str(j)), 'rateCardBilling'] = timebandFrame_ps.iloc[j][
                                                                                             'rateCardPrice'] * billingFCT / 10.0
            except:
                channelFrameIPO3Channel.loc[(channelName + str(j)), 'rateCardBilling'] = 0.0
            try:
                channelFrameIPO3Channel.loc[(channelName + str(j)), 'finalBilling'] = timebandFrame_ps.iloc[j][
                                                                                          'billingPrice'] * billingFCT / 10.0
            except:
                channelFrameIPO3Channel.loc[(channelName + str(j)), 'finalBilling'] = 0.0
            try:
                channelFrameIPO3Channel.loc[(channelName + str(j)), 'precedenceBilling'] = timebandFrame_ps.iloc[j][
                                                                                               'previousPrice'] * billingFCT / 10.0
            except:
                channelFrameIPO3Channel.loc[(channelName + str(j)), 'precedenceBilling'] = 0.0

            channelFrameIPO3Channel.loc[(channelName + str(j)), 'fctBilling'] = billingFCT

        channelFrameIPO3ChannelAgg = channelFrameIPO3ChannelAgg.append(channelFrameIPO3Channel.groupby(['channelName']).sum())
    return channelFrameIPO3ChannelAgg

# for IPO category for NEWSASMITAMAJHAGANGAspotBuy spotBuy
def getIPOFrame4Channels(gridRec):
    channelList = []
    channelFrameIPO4ChannelAgg = pd.DataFrame()
    channelFrame = pd.DataFrame(list(gridRec['channel'])).fillna(0.0)
    for i in range(len(channelFrame)):
        channelFrameIPO4Channel = pd.DataFrame()
        channelName = channelFrame.iloc[i]['name']
        channelList.append(channelName)
        timebandFrame_ps = pd.DataFrame(list(channelFrame.iloc[i]['NEWSASMITAMAJHAGANGAspotBuy']['timebands']))
        timebandFrame_ps["valueAdd"] = False if "valueAdd" not in timebandFrame_ps.columns else timebandFrame_ps["valueAdd"]
        if (len(timebandFrame_ps) == 0 or len(timebandFrame_ps[timebandFrame_ps["valueAdd"] != True]) == 0):
            channelFrameIPO4Channel.loc[(channelName + str(0)), 'channelName'] = channelName
            channelFrameIPO4Channel.loc[(channelName + str(0)), 'recommended'] = 0.0
            channelFrameIPO4Channel.loc[(channelName + str(0)), 'rateCard'] = 0.0
            channelFrameIPO4Channel.loc[(channelName + str(0)), 'final'] = 0.0
            channelFrameIPO4Channel.loc[(channelName + str(0)), 'precedence'] = 0.0
            channelFrameIPO4Channel.loc[(channelName + str(0)), 'fct'] = 0.0
            channelFrameIPO4Channel.loc[(channelName + str(0)), 'recommendedBilling'] = 0.0
            channelFrameIPO4Channel.loc[(channelName + str(0)), 'rateCardBilling'] = 0.0
            channelFrameIPO4Channel.loc[(channelName + str(0)), 'finalBilling'] = 0.0
            channelFrameIPO4Channel.loc[(channelName + str(0)), 'precedenceBilling'] = 0.0
            channelFrameIPO4Channel.loc[(channelName + str(0)), 'fctBilling'] = 0.0
        for j in range(len(timebandFrame_ps)):
            channelFrameIPO4Channel.loc[(channelName + str(j)), 'channelName'] = channelName
            if timebandFrame_ps.iloc[j]['valueAdd'] != True:
                try:
                    finalFCT = timebandFrame_ps.iloc[j]['finalFCT'] or 0.0
                except:
                    finalFCT = 0.0
                try:
                    channelFrameIPO4Channel.loc[(channelName + str(j)), 'recommended'] = timebandFrame_ps.iloc[j][
                                                                                          'recommendedPrice'] * finalFCT/10.0
                except:
                    channelFrameIPO4Channel.loc[(channelName + str(j)), 'recommended'] = 0.0
                try:
                    channelFrameIPO4Channel.loc[(channelName + str(j)), 'rateCard'] = timebandFrame_ps.iloc[j][
                                                                                       'rateCardPrice'] * finalFCT/10.0
                except:
                    channelFrameIPO4Channel.loc[(channelName + str(j)), 'rateCard'] = 0.0
                try:
                    channelFrameIPO4Channel.loc[(channelName + str(j)), 'final'] = timebandFrame_ps.iloc[j]['finalPrice'] * finalFCT/10.0
                except:
                    channelFrameIPO4Channel.loc[(channelName + str(j)), 'final'] = 0.0
                try:
                    channelFrameIPO4Channel.loc[(channelName + str(j)), 'precedence'] = timebandFrame_ps.iloc[j]['previousPrice'] * finalFCT/10.0
                except:
                    channelFrameIPO4Channel.loc[(channelName + str(j)), 'precedence'] = 0.0

                channelFrameIPO4Channel.loc[(channelName + str(j)), 'fct'] = finalFCT
            # Changed value add condition for billing - no impact if value add is true/false
            try:
                billingFCT = timebandFrame_ps.iloc[j]['billingFCT'] or 0.0
            except:
                billingFCT = 0.0
            try:
                channelFrameIPO4Channel.loc[(channelName + str(j)), 'recommendedBilling'] = timebandFrame_ps.iloc[j][
                                                                                                'recommendedPrice'] * billingFCT / 10.0
            except:
                channelFrameIPO4Channel.loc[(channelName + str(j)), 'recommendedBilling'] = 0.0
            try:
                channelFrameIPO4Channel.loc[(channelName + str(j)), 'rateCardBilling'] = timebandFrame_ps.iloc[j][
                                                                                             'rateCardPrice'] * billingFCT / 10.0
            except:
                channelFrameIPO4Channel.loc[(channelName + str(j)), 'rateCardBilling'] = 0.0
            try:
                channelFrameIPO4Channel.loc[(channelName + str(j)), 'finalBilling'] = timebandFrame_ps.iloc[j][
                                                                                          'billingPrice'] * billingFCT / 10.0
            except:
                channelFrameIPO4Channel.loc[(channelName + str(j)), 'finalBilling'] = 0.0
            try:
                channelFrameIPO4Channel.loc[(channelName + str(j)), 'precedenceBilling'] = timebandFrame_ps.iloc[j][
                                                                                               'previousPrice'] * billingFCT / 10.0
            except:
                channelFrameIPO4Channel.loc[(channelName + str(j)), 'precedenceBilling'] = 0.0

            channelFrameIPO4Channel.loc[(channelName + str(j)), 'fctBilling'] = billingFCT

        channelFrameIPO4ChannelAgg = channelFrameIPO4ChannelAgg.append(channelFrameIPO4Channel.groupby(['channelName']).sum())
    return channelFrameIPO4ChannelAgg

# for primary spotbuy
def getChannelFramePrimarySpotBuy(gridRec, listAlertRules, dealId, dbGrid):
    channelList = []
    channelFramePrimarySpotBuyAgg = pd.DataFrame()
    channelFrame = pd.DataFrame(list(gridRec['channel'])).fillna(0.0)
    for i in range(len(channelFrame)):
        channelFramePrimarySpotBuy = pd.DataFrame()
        channelName = channelFrame.iloc[i]['name']
        channelList.append(channelName)
        timebandFrame_ps = pd.DataFrame(list(channelFrame.iloc[i]['primarySpotBuy']['timebands']))
        timebandFrame_ps["valueAdd"] = False if "valueAdd" not in timebandFrame_ps.columns else timebandFrame_ps["valueAdd"]
        if (len(timebandFrame_ps) == 0 or len(timebandFrame_ps[timebandFrame_ps["valueAdd"] != True]) == 0):
            channelFramePrimarySpotBuy.loc[(channelName + str(0)), 'channelName'] = channelName
            channelFramePrimarySpotBuy.loc[(channelName + str(0)), 'recommended'] = 0.0
            channelFramePrimarySpotBuy.loc[(channelName + str(0)), 'rateCard'] = 0.0
            channelFramePrimarySpotBuy.loc[(channelName + str(0)), 'final'] = 0.0
            channelFramePrimarySpotBuy.loc[(channelName + str(0)), 'precedence'] = 0.0
            channelFramePrimarySpotBuy.loc[(channelName + str(0)), 'fct'] = 0.0
            channelFramePrimarySpotBuy.loc[(channelName + str(0)), 'recommendedBilling'] = 0.0
            channelFramePrimarySpotBuy.loc[(channelName + str(0)), 'rateCardBilling'] = 0.0
            channelFramePrimarySpotBuy.loc[(channelName + str(0)), 'finalBilling'] = 0.0
            channelFramePrimarySpotBuy.loc[(channelName + str(0)), 'precedenceBilling'] = 0.0
            channelFramePrimarySpotBuy.loc[(channelName + str(0)), 'fctBilling'] = 0.0
        for j in range(len(timebandFrame_ps)):
            channelFramePrimarySpotBuy.loc[(channelName + str(j)), 'channelName'] = channelName
            if timebandFrame_ps.iloc[j]['valueAdd'] != True:
                try:
                    finalFCT = timebandFrame_ps.iloc[j]['finalFCT'] or 0.0
                except:
                    finalFCT = 0.0
                try:
                    channelFramePrimarySpotBuy.loc[(channelName + str(j)), 'recommended'] = timebandFrame_ps.iloc[j][
                                                                                          'recommendedPrice'] * finalFCT/10.0
                except:
                    channelFramePrimarySpotBuy.loc[(channelName + str(j)), 'recommended'] = 0.0
                try:
                    channelFramePrimarySpotBuy.loc[(channelName + str(j)), 'rateCard'] = timebandFrame_ps.iloc[j][
                                                                                       'rateCardPrice'] * finalFCT/10.0
                except:
                    channelFramePrimarySpotBuy.loc[(channelName + str(j)), 'rateCard'] = 0.0
                try:
                    channelFramePrimarySpotBuy.loc[(channelName + str(j)), 'final'] = timebandFrame_ps.iloc[j]['finalPrice'] * finalFCT/10.0
                except:
                    channelFramePrimarySpotBuy.loc[(channelName + str(j)), 'final'] = 0.0
                try:
                    channelFramePrimarySpotBuy.loc[(channelName + str(j)), 'precedence'] = timebandFrame_ps.iloc[j]['previousPrice'] * finalFCT/10.0
                except:
                    channelFramePrimarySpotBuy.loc[(channelName + str(j)), 'precedence'] = 0.0

                channelFramePrimarySpotBuy.loc[(channelName + str(j)), 'fct'] = finalFCT
            # Changed value add condition for billing - no impact if value add is true/false
            try:
                billingFCT = timebandFrame_ps.iloc[j]['billingFCT'] or 0.0
            except:
                billingFCT = 0.0
            try:
                channelFramePrimarySpotBuy.loc[(channelName + str(j)), 'recommendedBilling'] = timebandFrame_ps.iloc[j][
                                                                                                   'recommendedPrice'] * billingFCT / 10.0
            except:
                channelFramePrimarySpotBuy.loc[(channelName + str(j)), 'recommendedBilling'] = 0.0
            try:
                channelFramePrimarySpotBuy.loc[(channelName + str(j)), 'rateCardBilling'] = timebandFrame_ps.iloc[j][
                                                                                                'rateCardPrice'] * billingFCT / 10.0
            except:
                channelFramePrimarySpotBuy.loc[(channelName + str(j)), 'rateCardBilling'] = 0.0
            try:
                channelFramePrimarySpotBuy.loc[(channelName + str(j)), 'finalBilling'] = timebandFrame_ps.iloc[j][
                                                                                             'billingPrice'] * billingFCT / 10.0
            except:
                channelFramePrimarySpotBuy.loc[(channelName + str(j)), 'finalBilling'] = 0.0
            try:
                channelFramePrimarySpotBuy.loc[(channelName + str(j)), 'precedenceBilling'] = timebandFrame_ps.iloc[j][
                                                                                                  'previousPrice'] * billingFCT / 10.0
            except:
                channelFramePrimarySpotBuy.loc[(channelName + str(j)), 'precedenceBilling'] = 0.0

            channelFramePrimarySpotBuy.loc[(channelName + str(j)), 'fctBilling'] = billingFCT
        channelFramePrimarySpotBuyAgg = channelFramePrimarySpotBuyAgg.append(channelFramePrimarySpotBuy.groupby(['channelName']).sum())
    return channelFramePrimarySpotBuyAgg

def getChannelFrameSpotBuy(gridRec, listAlertRules, dealId, dbGrid):
    channelList = []
    channelFrameSpotBuy = pd.DataFrame()
    channelFrameSpotBuyAgg = pd.DataFrame()
    erChannelFrameSpotBuy = pd.DataFrame()
    ptSpotBuy = pd.DataFrame()
    weSpotBuy = pd.DataFrame()
    mtSpotBuy = pd.DataFrame()
    channelFrame = pd.DataFrame(list(gridRec['channel'])).fillna(0.0)
    for i in range(len(channelFrame)):
        print('channelName->', channelFrame.iloc[i]['name'])
        channelName = channelFrame.iloc[i]['name']
        channelList.append(channelName)
        timebandFrame = pd.DataFrame(list(channelFrame.iloc[i]['spotBuy']['timebands']))
        print(timebandFrame)
        timebandFrame["valueAdd"] = False if "valueAdd" not in timebandFrame.columns else timebandFrame["valueAdd"]
        #print("kkkkkkkk",timebandFrame["valueAdd"])
        if (len(timebandFrame) == 0 or len(timebandFrame[timebandFrame["valueAdd"] != True]) == 0):
            channelFrameSpotBuy.loc[(channelName + str(0)), 'channelName'] = channelName
            channelFrameSpotBuy.loc[(channelName + str(0)), 'recommended'] = 0.0
            channelFrameSpotBuy.loc[(channelName + str(0)), 'rateCard'] = 0.0
            channelFrameSpotBuy.loc[(channelName + str(0)), 'final'] = 0.0
            channelFrameSpotBuy.loc[(channelName + str(0)), 'precedence'] = 0.0
            channelFrameSpotBuy.loc[(channelName + str(0)), 'fct'] = 0.0
            channelFrameSpotBuy.loc[(channelName + str(0)), 'recommendedBilling'] = 0.0
            channelFrameSpotBuy.loc[(channelName + str(0)), 'rateCardBilling'] = 0.0
            channelFrameSpotBuy.loc[(channelName + str(0)), 'finalBilling'] = 0.0
            channelFrameSpotBuy.loc[(channelName + str(0)), 'precedenceBilling'] = 0.0
            channelFrameSpotBuy.loc[(channelName + str(0)), 'fctBilling'] = 0.0
            # changes made for regional and national OR
            channelFrameSpotBuy.loc[(channelName + str(0)), 'regionOR'] = 0.0
            channelFrameSpotBuy.loc[(channelName + str(0)), 'nationalOR'] = 0.0
            channelFrameSpotBuy.loc[(channelName + str(0)), 'regionORBilling'] = 0.0
            channelFrameSpotBuy.loc[(channelName + str(0)), 'nationalORBilling'] = 0.0

        for j in range(len(timebandFrame)):
            channelFrameSpotBuy.loc[(channelName + str(j)), 'channelName'] = channelName
            if timebandFrame.iloc[j]['valueAdd'] != True:
                #for timeband level alerts
                channelLevelAlerts = [x for x in listAlertRules if x.channel == channelName and x.metric == "TIMEBAND ER"]
                # channelFrameSpotBuy.loc[(channelName + str(j)), 'finalFCT'] = timebandFrame.iloc[j]['previousPrice']

                try:
                    finalFCT = timebandFrame.iloc[j]['finalFCT'] or 0.0
                except:
                    finalFCT = 0.0

                try:
                    channelFrameSpotBuy.loc[(channelName + str(j)), 'recommended'] = timebandFrame.iloc[j][
                                                                                          'recommendedPrice'] * finalFCT/10.0
                except:
                    channelFrameSpotBuy.loc[(channelName + str(j)), 'recommended'] = 0.0

                try:
                    channelFrameSpotBuy.loc[(channelName + str(j)), 'rateCard'] = timebandFrame.iloc[j][
                                                                                       'rateCardPrice'] * finalFCT/10.0
                except:
                    channelFrameSpotBuy.loc[(channelName + str(j)), 'rateCard'] = 0.0
                try:
                    channelFrameSpotBuy.loc[(channelName + str(j)), 'final'] = timebandFrame.iloc[j]['finalPrice'] * finalFCT/10.0
                except:
                    channelFrameSpotBuy.loc[(channelName + str(j)), 'final'] = 0.0

                try:
                    channelFrameSpotBuy.loc[(channelName + str(j)), 'precedence'] = timebandFrame.iloc[j]['previousPrice'] * finalFCT/10.0
                except:
                    channelFrameSpotBuy.loc[(channelName + str(j)), 'precedence'] = 0.0

                channelFrameSpotBuy.loc[(channelName + str(j)), 'fct'] = finalFCT
                try:
                    dow = timebandFrame.iloc[j]['dow']['days']
                except:
                    dow= "NA"
                try:
                    timeband = timebandFrame.iloc[j]['timeband']['type']
                except:
                    timeband = "NA"
                try:
                    spotType = timebandFrame.iloc[j]['spotType']['name']
                except:
                    spotType = "NA"
                try:
                    rateType = timebandFrame.iloc[j]['rateType']
                except:
                    rateType = "NA"
                try:
                    timebandId = timebandFrame.iloc[j]['id']
                except:
                    timebandId = "NA"

                # changes made for regional and national OR - final
                try:
                    channelFrameSpotBuy.loc[(channelName + str(j)), 'nationalOR'] = timebandFrame.iloc[j]['nationalOR']* finalFCT/10.0
                except:
                    channelFrameSpotBuy.loc[(channelName + str(j)), 'nationalOR'] = 0.0

                print('VALUE-->>', timebandFrame.iloc[j]['regionOR'],timebandFrame.iloc[j]['nationalOR'])

                if pd.isnull(timebandFrame.iloc[j]['regionOR']) == False:
                    channelFrameSpotBuy.loc[(channelName + str(j)), 'regionOR'] = timebandFrame.iloc[j]['regionOR'] * finalFCT / 10.0
                elif pd.isnull(timebandFrame.iloc[j]['regionOR']) == True and pd.isnull(timebandFrame.iloc[j]['nationalOR']) == False:
                    channelFrameSpotBuy.loc[(channelName + str(j)), 'regionOR'] = timebandFrame.iloc[j]['nationalOR'] * finalFCT / 10.0
                else:
                    channelFrameSpotBuy.loc[(channelName + str(j)), 'regionOR'] = 0.0

                '''    
                if timebandFrame.iloc[j]['regionOR'] != None:
                    channelFrameSpotBuy.loc[(channelName + str(j)), 'regionOR'] = timebandFrame.iloc[j]['regionOR'] * finalFCT / 10.0
                elif timebandFrame.iloc[j]['regionOR'] == None and timebandFrame.iloc[j]['nationalOR'] != None:
                    channelFrameSpotBuy.loc[(channelName + str(j)), 'regionOR'] = timebandFrame.iloc[j]['nationalOR'] * finalFCT / 10.0
                else:
                    channelFrameSpotBuy.loc[(channelName + str(j)), 'regionOR'] = 0.0
                '''
                print('nationalOR and regionOR')
                print(channelFrameSpotBuy.loc[(channelName + str(j)), 'nationalOR'],channelFrameSpotBuy.loc[(channelName + str(j)), 'regionOR'])

                '''    
                if timebandFrame.iloc[j]['regionOR'] != None:
                    channelFrameSpotBuy.loc[(channelName + str(j)), 'regionORBilling'] = timebandFrame.iloc[j]['regionOR'] * billingFCT / 10.0
                elif timebandFrame.iloc[j]['regionOR'] == None and timebandFrame.iloc[j]['nationalOR'] != None:
                    channelFrameSpotBuy.loc[(channelName + str(j)), 'regionORBilling'] = timebandFrame.iloc[j]['nationalOR'] * billingFCT / 10.0
                else:
                    channelFrameSpotBuy.loc[(channelName + str(j)), 'regionORBilling'] = 0.0
                '''

                #checking for alerts
                timebandAlertsList = []
                for k in range(len(channelLevelAlerts)):
                    metric = channelLevelAlerts[k].metric
                    type = channelLevelAlerts[k].type
                    compareValue = channelLevelAlerts[k].compareValue
                    comparisonOperator = channelLevelAlerts[k].comparisonOperator
                    compareTo = channelLevelAlerts[k].compareTo
                    threshold = channelLevelAlerts[k].threshold
                    finalAmount = safe_div(channelFrameSpotBuy.loc[(channelName + str(j)), 'final']*10,finalFCT)
                    print(metric,type,compareValue,comparisonOperator,compareTo,threshold,finalAmount)
                    print('CHANNEL FRAME SPOTBUY values')
                    print(channelFrameSpotBuy.loc[(channelName + str(j)), 'recommended'])
                    print(channelFrameSpotBuy.loc[(channelName + str(j)), 'precedence'])
                    print(channelFrameSpotBuy.loc[(channelName + str(j)), 'rateCard'])
                    compareToAmount = channelFrameSpotBuy.loc[(channelName + str(j)), 'recommended']  if compareTo == "recommended" \
                                 else(channelFrameSpotBuy.loc[(channelName + str(j)), 'precedence']  if compareTo == "precedence"\
                                 else channelFrameSpotBuy.loc[(channelName + str(j)), 'rateCard'])
                    #print("precedence",compareToAmount)
                    #print("channelFrameSpot",channelFrameSpotBuy)
                    compareToAmount = safe_div(compareToAmount * 10 ,finalFCT)
                    differencePercent = abs(safe_div((finalAmount - compareToAmount) * 100.0, finalAmount))
                    print(compareToAmount,differencePercent)
                    if (ops[comparisonOperator](finalAmount, compareToAmount) and differencePercent > threshold):
                        #timeBandAlerts = cTimeBandAlerts(channelName, metric, comparisonOperator, compareValue, compareTo,
                        #             threshold, differencePercent, finalAmount, compareToAmount, dow, rateType, spotType,timeband)
                        timebandAlerts = cAlerts(channelName,dealId, metric, comparisonOperator, compareValue, compareTo, \
                                                  threshold, differencePercent, finalAmount, compareToAmount)
                        #print("timeband content",timeBandAlerts.returnTimeBandContent())
                        timebandAlertsList.append({"for": channelName, 'metric': metric, "compareValue": compareValue, \
                                                 "compareTo": compareTo, "content": timebandAlerts.returnContent()})
                        #timeBandAlertsList.append(timeBandAlerts.__dict__)
                # dbGrid.update({"channel.spotBuy.timebands.id": timebandId}, \
                #               {"$set": {"channel." + str(i) + ".spotBuy.timebands." + str(j) + ".alerts": timebandAlertsList}})
                try:

                    dbGrid.update({"channel.spotBuy.timebands.id": timebandId },\
                                  {"$set" : {"channel.$.spotBuy.timebands." + str(j) + ".alerts": timebandAlertsList}})
                except:
                    pass

            # Changed value add condition for billing - no impact if value add is true/false
            try:
                billingFCT = timebandFrame.iloc[j]['billingFCT'] or 0.0
            except:
                billingFCT = 0.0
            try:
                channelFrameSpotBuy.loc[(channelName + str(j)), 'recommendedBilling'] = timebandFrame.iloc[j][
                                                                                            'recommendedPrice'] * billingFCT / 10.0
            except:
                channelFrameSpotBuy.loc[(channelName + str(j)), 'recommendedBilling'] = 0.0
            try:
                channelFrameSpotBuy.loc[(channelName + str(j)), 'rateCardBilling'] = timebandFrame.iloc[j][
                                                                                         'rateCardPrice'] * billingFCT / 10.0
            except:
                channelFrameSpotBuy.loc[(channelName + str(j)), 'rateCardBilling'] = 0.0
            try:
                channelFrameSpotBuy.loc[(channelName + str(j)), 'finalBilling'] = timebandFrame.iloc[j][
                                                                                      'billingPrice'] * billingFCT / 10.0
            except:
                channelFrameSpotBuy.loc[(channelName + str(j)), 'finalBilling'] = 0.0
            try:
                channelFrameSpotBuy.loc[(channelName + str(j)), 'precedenceBilling'] = timebandFrame.iloc[j][
                                                                                           'previousPrice'] * billingFCT / 10.0
            except:
                channelFrameSpotBuy.loc[(channelName + str(j)), 'precedenceBilling'] = 0.0

            channelFrameSpotBuy.loc[(channelName + str(j)), 'fctBilling'] = billingFCT
            # changes made for regional and national OR - billing
            try:
                channelFrameSpotBuy.loc[(channelName + str(j)), 'nationalORBilling'] = timebandFrame.iloc[j][
                                                                                           'nationalOR'] * billingFCT / 10.0
            except:
                channelFrameSpotBuy.loc[(channelName + str(j)), 'nationalORBilling'] = 0.0
            if pd.isnull(timebandFrame.iloc[j]['regionOR']) == False:
                channelFrameSpotBuy.loc[(channelName + str(j)), 'regionORBilling'] = timebandFrame.iloc[j][
                                                                                         'regionOR'] * billingFCT / 10.0
            elif pd.isnull(timebandFrame.iloc[j]['regionOR']) == True and pd.isnull(
                    timebandFrame.iloc[j]['nationalOR']) == False:
                channelFrameSpotBuy.loc[(channelName + str(j)), 'regionORBilling'] = timebandFrame.iloc[j][
                                                                                         'nationalOR'] * billingFCT / 10.0
            else:
                channelFrameSpotBuy.loc[(channelName + str(j)), 'regionORBilling'] = 0.0

        #for ER Component
        erChannelFrameSpotBuy.loc[channelName, 'channelName'] = channelName
        ptSpotBuy.loc[channelName, 'channelName'] = channelName
        weSpotBuy.loc[channelName, 'channelName'] = channelName
        mtSpotBuy.loc[channelName, 'channelName'] = channelName
        try:
            erTotalFCT = channelFrame.iloc[i]['spotBuy']['final']['totalFCT']
        except:
            erTotalFCT = 0.0
        try:
            erChannelFrameSpotBuy.loc[channelName, 'final'] = channelFrame.iloc[i]['spotBuy']['final']['er']
        except:
            erChannelFrameSpotBuy.loc[channelName, 'final'] =0.0

        try:
            erChannelFrameSpotBuy.loc[channelName, 'rateCard'] = channelFrame.iloc[i]['spotBuy']['final']['erRateCard'] #* erTotalFCT/10
        except:
            erChannelFrameSpotBuy.loc[channelName, 'rateCard'] = 0.0
        try:
            erChannelFrameSpotBuy.loc[channelName, 'recommended'] = channelFrame.iloc[i]['spotBuy']['final']['erRecommended'] #* erTotalFCT/10
        except:
            erChannelFrameSpotBuy.loc[channelName, 'recommended'] = 0.0

        try:
            erChannelFrameSpotBuy.loc[channelName, 'precedence'] = channelFrame.iloc[i]['spotBuy']['prevDealSummary']\
                                                                                          ['prev']['er']
        except:
            erChannelFrameSpotBuy.loc[channelName, 'precedence'] = 0.0
        try:
            erChannelFrameSpotBuy.loc[channelName, 'finalBilling'] = channelFrame.iloc[i]['spotBuy']['final']['erBilling']
        except:
            erChannelFrameSpotBuy.loc[channelName, 'finalBilling'] =0.0

        try:
            erChannelFrameSpotBuy.loc[channelName, 'rateCardBilling'] = channelFrame.iloc[i]['spotBuy']['final']['erRateCardBilling'] #* erTotalFCT/10
        except:
            erChannelFrameSpotBuy.loc[channelName, 'rateCardBilling'] = 0.0
        try:
            erChannelFrameSpotBuy.loc[channelName, 'recommendedBilling'] = channelFrame.iloc[i]['spotBuy']['final']['erRecommendedBilling'] #* erTotalFCT/10
        except:
            erChannelFrameSpotBuy.loc[channelName, 'recommendedBilling'] = 0.0

        try:
            erChannelFrameSpotBuy.loc[channelName, 'precedenceBilling'] = channelFrame.iloc[i]['spotBuy']['prevDealSummary']\
                                                                                          ['prev']['erBilling']
        except:
            erChannelFrameSpotBuy.loc[channelName, 'precedenceBilling'] = 0.0

        try:
            ptSpotBuy.loc[channelName, 'final'] = channelFrame.iloc[i]['spotBuy']['final']['skewPTPercent']
        except:
            ptSpotBuy.loc[channelName, 'final'] = 0.0
        try:
            ptSpotBuy.loc[channelName, 'precedence'] = channelFrame.iloc[i]['spotBuy']['prevDealSummary']\
                                                                                          ['prev']['skewPT']
        except:
            ptSpotBuy.loc[channelName, 'precedence'] = 0.0

        try:
            weSpotBuy.loc[channelName, 'precedence'] = channelFrame.iloc[i]['spotBuy']['prevDealSummary']\
                                                                                          ['prev']['skewWE']
        except:
            weSpotBuy.loc[channelName, 'precedence'] = 0.0

        try:
            weSpotBuy.loc[channelName, 'final'] = channelFrame.iloc[i]['spotBuy']['final']['skewWEPercent']
        except:
            weSpotBuy.loc[channelName, 'final'] = 0.0
        try:
            mtSpotBuy.loc[channelName, 'final'] = channelFrame.iloc[i]['spotBuy']['final']['skewMTPercent']
        except:
            mtSpotBuy.loc[channelName, 'final'] = 0.0
        try:
            mtSpotBuy.loc[channelName, 'precedence'] = channelFrame.iloc[i]['spotBuy']['prevDealSummary'] \
                                                                                            ['prev']['skewMT']
        except:
            mtSpotBuy.loc[channelName, 'precedence'] = 0.0

        ##added this only to get precedence outlay for timeband level different and final level from grid directly
        channelFrameSpotBuyAgg = channelFrameSpotBuyAgg.append(channelFrameSpotBuy.groupby(['channelName']).sum())
        #print('%%%%%% channel Frame spotbuy agg')
        #print(channelFrameSpotBuyAgg)
        #channelFrameSpotBuyAgg.to_csv('D:\Work\ABP\channelFrameSpotbuyAGG.csv')
        channelFrameSpotBuy = channelFrameSpotBuy.iloc[0:0]
        try:
            channelFrameSpotBuyAgg.loc[channelName, 'precedence'] = channelFrame.iloc[i]['spotBuy']['prevDealSummary'] \
                                                                    ['prev']['outlay']
        except:
            channelFrameSpotBuyAgg.loc[channelName, 'precedence'] = 0.0
            # channelFrameSpotBuy.loc[(channelName, j), 'recommendedPrice'] = channelName
            # channelFrameSpotBuy.loc[(channelName, j), 'channelName'] = channelName
            # channelFrameSpotBuy.loc[(channelName, j), 'channelName'] = channelName
            # channelFrameSpotBuy.loc[(channelName, j), 'channelName'] = channelName

    return channelFrameSpotBuyAgg, erChannelFrameSpotBuy, channelList, ptSpotBuy, weSpotBuy, mtSpotBuy

#added for spotlight package by ravinder (2019-02-05)
def getChannelFrameSpotLight(gridRec):
    channelSpotPckg = pd.DataFrame()
    channelAdvtTape = pd.DataFrame()
    channelTeleshopping = pd.DataFrame()
    channelAFPSlots = pd.DataFrame()
    channelFrame = pd.DataFrame(list(gridRec['channel'])).fillna(0.0)
    # for sponsorship packages added by ravinder (2019-02-05)
    for i in range(len(channelFrame)):
        channelName = channelFrame.iloc[i]['name']
        packageFrame = pd.DataFrame(list(channelFrame.iloc[i]['spotlightPackage']['packages']))
        if (len(packageFrame) == 0):
            channelSpotPckg.loc[(channelName + str(0)), 'channelName'] = channelName
            channelSpotPckg.loc[(channelName + str(0)), 'recommended'] = 0.0
            channelSpotPckg.loc[(channelName + str(0)), 'rateCard'] = 0.0
            channelSpotPckg.loc[(channelName + str(0)), 'final'] = 0.0
            channelSpotPckg.loc[(channelName + str(0)), 'precedence'] = 0.0
            channelSpotPckg.loc[(channelName + str(0)), 'fct'] = 0.0
            channelSpotPckg.loc[(channelName + str(0)), 'recommendedBilling'] = 0.0
            channelSpotPckg.loc[(channelName + str(0)), 'rateCardBilling'] = 0.0
            channelSpotPckg.loc[(channelName + str(0)), 'finalBilling'] = 0.0
            channelSpotPckg.loc[(channelName + str(0)), 'precedenceBilling'] = 0.0
            channelSpotPckg.loc[(channelName + str(0)), 'fctBilling'] = 0.0
        for j in range(len(packageFrame)):

            channelSpotPckg.loc[(channelName + str(j)), 'channelName'] = channelName
            finalFCT = 0.0
            billingFCT = 0.0
            try:
                channelSpotPckg.loc[(channelName + str(j)), 'recommended'] = packageFrame.iloc[j]['recommendedPrice']
            except:
                channelSpotPckg.loc[(channelName + str(j)), 'recommended'] = 0.0
            try:
                channelSpotPckg.loc[(channelName + str(j)), 'rateCard'] = packageFrame.iloc[j]['rateCardPrice']
            except:
                channelSpotPckg.loc[(channelName + str(j)), 'rateCard'] = 0.0
            try:
                channelSpotPckg.loc[(channelName + str(j)), 'final'] = packageFrame.iloc[j]['finalPrice']
            except:
                channelSpotPckg.loc[(channelName + str(j)), 'final'] = 0.0
            channelSpotPckg.loc[(channelName + str(j)), 'precedence'] = 0.0
            channelSpotPckg.loc[(channelName + str(j)), 'fct'] = finalFCT

            try:
                channelSpotPckg.loc[(channelName + str(j)), 'recommendedBilling'] = packageFrame.iloc[j]['recommendedPrice']
            except:
                channelSpotPckg.loc[(channelName + str(j)), 'recommendedBilling'] = 0.0
            try:
                channelSpotPckg.loc[(channelName + str(j)), 'rateCardBilling'] = packageFrame.iloc[j]['rateCardPrice']
            except:
                channelSpotPckg.loc[(channelName + str(j)), 'rateCardBilling'] = 0.0

            try:
                channelSpotPckg.loc[(channelName + str(j)), 'finalBilling'] = packageFrame.iloc[j]['billingPrice']
            except:
                channelSpotPckg.loc[(channelName + str(j)), 'finalBilling'] = 0.0
            channelSpotPckg.loc[(channelName + str(j)), 'precedenceBilling'] = 0.0
            channelSpotPckg.loc[(channelName + str(j)), 'fctBilling'] = billingFCT

         #""" added for advt element """
        timebandFrame = pd.DataFrame(list(channelFrame.iloc[i]['advtTape']['timebands']))
        if (len(timebandFrame) == 0 or len(timebandFrame[timebandFrame["valueAdd"] == False]) == 0):
            channelAdvtTape.loc[(channelName + str(0)), 'channelName'] = channelName
            channelAdvtTape.loc[(channelName + str(0)), 'recommended'] = 0.0
            channelAdvtTape.loc[(channelName + str(0)), 'rateCard'] = 0.0
            channelAdvtTape.loc[(channelName + str(0)), 'final'] = 0.0
            channelAdvtTape.loc[(channelName + str(0)), 'precedence'] = 0.0
            channelAdvtTape.loc[(channelName + str(0)), 'fct'] = 0.0
            channelAdvtTape.loc[(channelName + str(0)), 'recommendedBilling'] = 0.0
            channelAdvtTape.loc[(channelName + str(0)), 'rateCardBilling'] = 0.0
            channelAdvtTape.loc[(channelName + str(0)), 'finalBilling'] = 0.0
            channelAdvtTape.loc[(channelName + str(0)), 'precedenceBilling'] = 0.0
            channelAdvtTape.loc[(channelName + str(0)), 'fctBilling'] = 0.0
        for j in range(len(timebandFrame)):
            channelAdvtTape.loc[(channelName + str(j)), 'channelName'] = channelName
            if timebandFrame.iloc[j]['valueAdd'] == False:

                try:
                    totalSlots = timebandFrame.iloc[j]["totalSlots"]
                except:
                    totalSlots = 0.0
                try:
                    channelAdvtTape.loc[(channelName + str(j)), 'recommended'] = timebandFrame.iloc[j]['recommendedPrice']*totalSlots
                except:
                    channelAdvtTape.loc[(channelName + str(j)), 'recommended'] = 0.0
                try:
                    channelAdvtTape.loc[(channelName + str(j)), 'rateCard'] = timebandFrame.iloc[j]['rateCardPrice']*totalSlots
                except:
                    channelAdvtTape.loc[(channelName + str(j)), 'rateCard'] = 0.0
                try:
                    channelAdvtTape.loc[(channelName + str(j)), 'final'] =timebandFrame.iloc[j]['finalPrice']*totalSlots
                except:
                    channelAdvtTape.loc[(channelName + str(j)), 'final'] = 0.0
                try:
                    channelAdvtTape.loc[(channelName + str(j)), 'precedence'] = timebandFrame.iloc[j]['precedence']*totalSlots
                except:
                    channelAdvtTape.loc[(channelName + str(j)), 'precedence'] = 0.0
                channelAdvtTape.loc[(channelName + str(j)), 'fct'] = totalSlots
            # Changed value add condition for billing - no impact if value add is true/false
            try:
                billingSlots = timebandFrame.iloc[j]["billingSlots"]
            except:
                billingSlots = 0.0
            try:
                channelAdvtTape.loc[(channelName + str(j)), 'recommendedBilling'] = timebandFrame.iloc[j][
                                                                                 'recommendedPrice'] * billingSlots
            except:
                channelAdvtTape.loc[(channelName + str(j)), 'recommendedBilling'] = 0.0
            try:
                channelAdvtTape.loc[(channelName + str(j)), 'rateCardBilling'] = timebandFrame.iloc[j][
                                                                              'rateCardPrice'] * billingSlots
            except:
                channelAdvtTape.loc[(channelName + str(j)), 'rateCardBilling'] = 0.0
            try:
                channelAdvtTape.loc[(channelName + str(j)), 'finalBilling'] = timebandFrame.iloc[j][
                                                                           'billingPrice'] * billingSlots
            except:
                channelAdvtTape.loc[(channelName + str(j)), 'finalBilling'] = 0.0
            try:
                channelAdvtTape.loc[(channelName + str(j)), 'precedenceBilling'] = timebandFrame.iloc[j][
                                                                                'precedence'] * billingSlots
            except:
                channelAdvtTape.loc[(channelName + str(j)), 'precedenceBilling'] = 0.0
            channelAdvtTape.loc[(channelName + str(j)), 'fctBilling'] = billingSlots

        #added for teleshopping
        timebandFrame = pd.DataFrame(list(channelFrame.iloc[i]['teleshopping']['timebands']))
        if (len(timebandFrame) == 0 or len(timebandFrame[timebandFrame["valueAdd"] == False]) == 0):
            channelTeleshopping.loc[(channelName + str(0)), 'channelName'] = channelName
            channelTeleshopping.loc[(channelName + str(0)), 'recommended'] = 0.0
            channelTeleshopping.loc[(channelName + str(0)), 'rateCard'] = 0.0
            channelTeleshopping.loc[(channelName + str(0)), 'final'] = 0.0
            channelTeleshopping.loc[(channelName + str(0)), 'precedence'] = 0.0
            channelTeleshopping.loc[(channelName + str(0)), 'fct'] = 0.0
            channelTeleshopping.loc[(channelName + str(0)), 'recommendedBilling'] = 0.0
            channelTeleshopping.loc[(channelName + str(0)), 'rateCardBilling'] = 0.0
            channelTeleshopping.loc[(channelName + str(0)), 'finalBilling'] = 0.0
            channelTeleshopping.loc[(channelName + str(0)), 'precedenceBilling'] = 0.0
            channelTeleshopping.loc[(channelName + str(0)), 'fctBilling'] = 0.0
        for j in range(len(timebandFrame)):
            channelTeleshopping.loc[(channelName + str(j)), 'channelName'] = channelName
            if timebandFrame.iloc[j]['valueAdd'] == False:
                try:
                    exposures = timebandFrame.iloc[j]["noOfExposures"]
                except:
                    exposures = 0.0
                try:
                    channelTeleshopping.loc[(channelName + str(j)), 'recommended'] = timebandFrame.iloc[j]['recommendedPrice'] * exposures
                except:
                    channelTeleshopping.loc[(channelName + str(j)), 'recommended'] = 0.0
                try:
                    channelTeleshopping.loc[(channelName + str(j)), 'rateCard'] = timebandFrame.iloc[j]['rateCardPrice'] * exposures
                except:
                    channelTeleshopping.loc[(channelName + str(j)), 'rateCard'] = 0.0
                try:
                    channelTeleshopping.loc[(channelName + str(j)), 'final'] = timebandFrame.iloc[j]['finalPrice'] * exposures
                except:
                    channelTeleshopping.loc[(channelName + str(j)), 'final'] = 0.0
                try:
                    channelTeleshopping.loc[(channelName + str(j)), 'precedence'] = timebandFrame.iloc[j]['precedence'] * exposures
                except:
                    channelTeleshopping.loc[(channelName + str(j)), 'precedence'] = 0.0
                channelTeleshopping.loc[(channelName + str(j)), 'fct'] = exposures
            # Changed value add condition for billing - no impact if value add is true/false
            try:
                exposuresBilling = timebandFrame.iloc[j]["billingExposures"]
            except:
                exposuresBilling = 0.0
            try:
                channelTeleshopping.loc[(channelName + str(j)), 'recommendedBilling'] = timebandFrame.iloc[j]['recommendedPrice'] * exposuresBilling
            except:
                channelTeleshopping.loc[(channelName + str(j)), 'recommendedBilling'] = 0.0
            try:
                channelTeleshopping.loc[(channelName + str(j)), 'rateCardBilling'] = timebandFrame.iloc[j]['rateCardPrice'] * exposuresBilling
            except:
                channelTeleshopping.loc[(channelName + str(j)), 'rateCardBilling'] = 0.0
            try:
                channelTeleshopping.loc[(channelName + str(j)), 'finalBilling'] = timebandFrame.iloc[j]['finalPrice'] * exposuresBilling
            except:
                channelTeleshopping.loc[(channelName + str(j)), 'finalBilling'] = 0.0
            try:
                channelTeleshopping.loc[(channelName + str(j)), 'precedenceBilling'] = timebandFrame.iloc[j]['precedence'] * exposuresBilling
            except:
                channelTeleshopping.loc[(channelName + str(j)), 'precedenceBilling'] = 0.0
            channelTeleshopping.loc[(channelName + str(j)), 'fctBilling'] = exposuresBilling

        # added for afp slots
        afpTBFrame = pd.DataFrame(list(channelFrame.iloc[i]['afpSlots']['slots']))
        if (len(afpTBFrame) == 0):
            channelAFPSlots.loc[(channelName + str(0)), 'channelName'] = channelName
            channelAFPSlots.loc[(channelName + str(0)), 'recommended'] = 0.0
            channelAFPSlots.loc[(channelName + str(0)), 'rateCard'] = 0.0
            channelAFPSlots.loc[(channelName + str(0)), 'final'] = 0.0
            channelAFPSlots.loc[(channelName + str(0)), 'precedence'] = 0.0
            channelAFPSlots.loc[(channelName + str(0)), 'fct'] = 0.0
            channelAFPSlots.loc[(channelName + str(0)), 'recommendedBilling'] = 0.0
            channelAFPSlots.loc[(channelName + str(0)), 'rateCardBilling'] = 0.0
            channelAFPSlots.loc[(channelName + str(0)), 'finalBilling'] = 0.0
            channelAFPSlots.loc[(channelName + str(0)), 'precedenceBilling'] = 0.0
            channelAFPSlots.loc[(channelName + str(0)), 'fctBilling'] = 0.0
        for j in range(len(afpTBFrame)):
            channelAFPSlots.loc[(channelName + str(j)), 'channelName'] = channelName
            try:
                slots = afpTBFrame.iloc[j]["slots"]
            except:
                slots = 0.0
            billingSlots = 0.0
            try:
                channelAFPSlots.loc[(channelName + str(j)), 'recommended'] = afpTBFrame.iloc[j]['recommendedPrice']
            except:
                channelAFPSlots.loc[(channelName + str(j)), 'recommended'] = 0.0
            try:
                channelAFPSlots.loc[(channelName + str(j)), 'rateCard'] = afpTBFrame.iloc[j]['rateCardPrice']
            except:
                channelAFPSlots.loc[(channelName + str(j)), 'rateCard'] = 0.0
            try:
                channelAFPSlots.loc[(channelName + str(j)), 'final'] = afpTBFrame.iloc[j]['finalPrice']
            except:
                channelAFPSlots.loc[(channelName + str(j)), 'final'] = 0.0
            try:
                channelAFPSlots.loc[(channelName + str(j)), 'precedence'] = afpTBFrame.iloc[j]['precedence']
            except:
                channelAFPSlots.loc[(channelName + str(j)), 'precedence'] = 0.0
            channelAFPSlots.loc[(channelName + str(j)), 'fct'] = slots

            channelAFPSlots.loc[(channelName + str(j)), 'recommendedBilling'] = 0.0

            channelAFPSlots.loc[(channelName + str(j)), 'rateCardBilling'] = 0.0
            try:
                channelAFPSlots.loc[(channelName + str(j)), 'finalBilling'] = afpTBFrame.iloc[j]['billingPrice']
            except:
                channelAFPSlots.loc[(channelName + str(j)), 'finalBilling'] = 0.0
            channelAFPSlots.loc[(channelName + str(j)), 'precedenceBilling'] = 0.0

            channelAFPSlots.loc[(channelName + str(j)), 'fctBilling'] =billingSlots

    channelSpotLightFrame = channelAdvtTape.append([channelSpotPckg,channelTeleshopping,channelAFPSlots])
    channelFrameSpotLightAgg = channelSpotLightFrame.groupby(['channelName']).sum()
    return channelFrameSpotLightAgg
def getChannelFrameSponsorship(gridRec):
    channelFrameSponsorship = pd.DataFrame()
    channelSponPckg = pd.DataFrame()
    channelFrame = pd.DataFrame(list(gridRec['channel'])).fillna(0.0)
    for i in range(len(channelFrame)):
        channelName = channelFrame.iloc[i]['name']
        #print('ChannelName', channelName)
        timebandFrame = pd.DataFrame(list(channelFrame.iloc[i]['sponsorship']['timebands']))
        timebandFrame["valueAdd"] = False if "valueAdd" not in timebandFrame.columns else timebandFrame["valueAdd"]
        if (len(timebandFrame) == 0 or len(timebandFrame[timebandFrame["valueAdd"] == False]) == 0):
            channelFrameSponsorship.loc[(channelName + str(0)), 'channelName'] = channelName
            channelFrameSponsorship.loc[(channelName + str(0)), 'recommended'] = 0.0
            channelFrameSponsorship.loc[(channelName + str(0)), 'rateCard'] = 0.0
            channelFrameSponsorship.loc[(channelName + str(0)), 'final'] = 0.0
            channelFrameSponsorship.loc[(channelName + str(0)), 'precedence'] = 0.0
            channelFrameSponsorship.loc[(channelName + str(0)), 'fct'] = 0.0
            channelFrameSponsorship.loc[(channelName + str(0)), 'recommendedBilling'] = 0.0
            channelFrameSponsorship.loc[(channelName + str(0)), 'rateCardBilling'] = 0.0
            channelFrameSponsorship.loc[(channelName + str(0)), 'finalBilling'] = 0.0
            channelFrameSponsorship.loc[(channelName + str(0)), 'precedenceBilling'] = 0.0
            channelFrameSponsorship.loc[(channelName + str(0)), 'fctBilling'] = 0.0
        for j in range(len(timebandFrame)):
            channelFrameSponsorship.loc[(channelName + str(j)), 'channelName'] = channelName
            if timebandFrame.iloc[j]['valueAdd'] == False:
                try:
                    finalFCT =timebandFrame.iloc[j]['finalFCT']
                except:
                    finalFCT = 0.0
                try:
                    channelFrameSponsorship.loc[(channelName + str(j)), 'recommended'] = timebandFrame.iloc[j]['recommendedPrice'] \
                                                                                                               * finalFCT/10
                except:
                    channelFrameSponsorship.loc[(channelName + str(j)), 'recommended'] = 0.0
                try:
                    channelFrameSponsorship.loc[(channelName + str(j)), 'rateCard'] = timebandFrame.iloc[j]['rateCardPrice'] * finalFCT/10
                except:
                    channelFrameSponsorship.loc[(channelName + str(j)), 'rateCard'] = 0.0
                try:
                    channelFrameSponsorship.loc[(channelName + str(j)), 'final'] = timebandFrame.iloc[j]['finalPrice'] * finalFCT/10
                except:
                    channelFrameSponsorship.loc[(channelName + str(j)), 'final'] = 0.0
                channelFrameSponsorship.loc[(channelName + str(j)), 'precedence'] = 0.0
                channelFrameSponsorship.loc[(channelName + str(j)), 'fct'] = finalFCT

            # Changed value add condition for billing - no impact if value add is true/false
            try:
                billingFCT = timebandFrame.iloc[j]['billingFCT']
            except:
                billingFCT = 0.0
            try:
                channelFrameSponsorship.loc[(channelName + str(j)), 'recommendedBilling'] = timebandFrame.iloc[j][
                                                                                                'recommendedPrice'] \
                                                                                            * billingFCT / 10
            except:
                channelFrameSponsorship.loc[(channelName + str(j)), 'recommendedBilling'] = 0.0
            try:
                channelFrameSponsorship.loc[(channelName + str(j)), 'rateCardBilling'] = timebandFrame.iloc[j][
                                                                                             'rateCardPrice'] * billingFCT / 10
            except:
                channelFrameSponsorship.loc[(channelName + str(j)), 'rateCardBilling'] = 0.0
            try:
                channelFrameSponsorship.loc[(channelName + str(j)), 'finalBilling'] = timebandFrame.iloc[j][
                                                                                          'billingPrice'] * billingFCT / 10
            except:
                channelFrameSponsorship.loc[(channelName + str(j)), 'finalBilling'] = 0.0
            channelFrameSponsorship.loc[(channelName + str(j)), 'precedenceBilling'] = 0.0
            channelFrameSponsorship.loc[(channelName + str(j)), 'fctBilling'] = billingFCT

        #for sponsorship packages added by ravinder (2019-02-05)
        packageFrame = pd.DataFrame(list(channelFrame.iloc[i]['sponsorshipPackage']['packages']))
        if (len(packageFrame) == 0):
            channelSponPckg.loc[(channelName + str(0)), 'channelName'] = channelName
            channelSponPckg.loc[(channelName + str(0)), 'recommended'] = 0.0
            channelSponPckg.loc[(channelName + str(0)), 'rateCard'] = 0.0
            channelSponPckg.loc[(channelName + str(0)), 'final'] = 0.0
            channelSponPckg.loc[(channelName + str(0)), 'precedence'] = 0.0
            channelSponPckg.loc[(channelName + str(0)), 'fct'] = 0.0
            channelSponPckg.loc[(channelName + str(0)), 'recommendedBilling'] = 0.0
            channelSponPckg.loc[(channelName + str(0)), 'rateCardBilling'] = 0.0
            channelSponPckg.loc[(channelName + str(0)), 'finalBilling'] = 0.0
            channelSponPckg.loc[(channelName + str(0)), 'precedenceBilling'] = 0.0
            channelSponPckg.loc[(channelName + str(0)), 'fctBilling'] = 0.0
        for j in range(len(packageFrame)):

            channelSponPckg.loc[(channelName + str(j)), 'channelName'] = channelName
            finalFCT = 0.0
            billingFCT = 0.0
            try:
                channelSponPckg.loc[(channelName + str(j)), 'recommended'] = packageFrame.iloc[j]['recommendedPrice']
            except:
                channelSponPckg.loc[(channelName + str(j)), 'recommended'] = 0.0
            try:
                channelSponPckg.loc[(channelName + str(j)), 'rateCard'] = packageFrame.iloc[j]['rateCardPrice']
            except:
                channelSponPckg.loc[(channelName + str(j)), 'rateCard'] = 0.0
            try:
                channelSponPckg.loc[(channelName + str(j)), 'final'] = packageFrame.iloc[j]['finalPrice']
            except:
                channelSponPckg.loc[(channelName + str(j)), 'final'] = 0.0
            channelSponPckg.loc[(channelName + str(j)), 'precedence'] = 0.0
            channelSponPckg.loc[(channelName + str(j)), 'fct'] = finalFCT

            try:
                channelSponPckg.loc[(channelName + str(j)), 'recommendedBilling'] = packageFrame.iloc[j]['recommendedPrice']
            except:
                channelSponPckg.loc[(channelName + str(j)), 'recommendedBilling'] = 0.0

            try:
                channelSponPckg.loc[(channelName + str(j)), 'rateCardBilling'] = packageFrame.iloc[j]['rateCardPrice']
            except:
                channelSponPckg.loc[(channelName + str(j)), 'rateCardBilling'] = 0.0

            try:
                channelSponPckg.loc[(channelName + str(j)), 'finalBilling'] = packageFrame.iloc[j]['billingPrice']
            except:
                channelSponPckg.loc[(channelName + str(j)), 'finalBilling'] = 0.0
            channelSponPckg.loc[(channelName + str(j)), 'precedenceBilling'] = 0.0
            channelSponPckg.loc[(channelName + str(j)), 'fctBilling'] = billingFCT
    channelFrameSponIncPckg = channelFrameSponsorship.append([channelSponPckg])
    channelFrameSponsorshipAgg = channelFrameSponIncPckg.groupby(['channelName']).sum()
    return channelFrameSponsorshipAgg


def getChannelFrameNonFCT(gridRec):
    channelFrameNonFCTMon = pd.DataFrame()
    channelFrameNonFCTExp = pd.DataFrame()
    channelFrameTicker = pd.DataFrame()
    channelFrameAston = pd.DataFrame()
    channelFrameLBand = pd.DataFrame()
    channelFrame = pd.DataFrame(list(gridRec['channel'])).fillna(0.0)
    for i in range(len(channelFrame)):
        channelName = channelFrame.iloc[i]['name']
        timebandFrameNonFCTMon = pd.DataFrame(list(channelFrame.iloc[i]['nonFCTMonthly']['timebands']))
        if (len(timebandFrameNonFCTMon) == 0 or len(timebandFrameNonFCTMon[timebandFrameNonFCTMon["valueAdd"] == False]) == 0):
            channelFrameNonFCTMon.loc[(channelName + str(0)), 'channelName'] = channelName
            channelFrameNonFCTMon.loc[(channelName + str(0)), 'recommended'] = 0.0
            channelFrameNonFCTMon.loc[(channelName + str(0)), 'rateCard'] = 0.0
            channelFrameNonFCTMon.loc[(channelName + str(0)), 'final'] = 0.0
            channelFrameNonFCTMon.loc[(channelName + str(0)), 'precedence'] = 0.0
            channelFrameNonFCTMon.loc[(channelName + str(0)), 'fct'] = 0.0
            channelFrameNonFCTMon.loc[(channelName + str(0)), 'recommendedBilling'] = 0.0
            channelFrameNonFCTMon.loc[(channelName + str(0)), 'rateCardBilling'] = 0.0
            channelFrameNonFCTMon.loc[(channelName + str(0)), 'finalBilling'] = 0.0
            channelFrameNonFCTMon.loc[(channelName + str(0)), 'precedenceBilling'] = 0.0
            channelFrameNonFCTMon.loc[(channelName + str(0)), 'fctBilling'] = 0.0
        for j in range(len(timebandFrameNonFCTMon)):
            channelFrameNonFCTMon.loc[(channelName + str(j)), 'channelName'] = channelName
            if timebandFrameNonFCTMon.iloc[j]['valueAdd'] == False:
                try:
                    outlay = timebandFrameNonFCTMon.iloc[j]['outlay']
                except:
                    outlay =0.0
                try:
                    finalPrice = timebandFrameNonFCTMon.iloc[j]['finalPrice']
                except:
                    finalPrice =0.0

                #numberOfMonths = safe_div(outlay, finalPrice)
                #numberOfMonthsBilling = safe_div(outlay, billingPrice)
                try:
                    channelFrameNonFCTMon.loc[(channelName + str(j)), 'recommended'] = timebandFrameNonFCTMon.iloc[j]['recommendedPrice'] \

                except:
                    channelFrameNonFCTMon.loc[(channelName + str(j)), 'recommended'] = 0.0

                try:
                    channelFrameNonFCTMon.loc[(channelName + str(j)), 'rateCard'] = timebandFrameNonFCTMon.iloc[j]['rateCardPrice']
                except:
                    channelFrameNonFCTMon.loc[(channelName + str(j)), 'rateCard'] = 0.0

                try:
                    channelFrameNonFCTMon.loc[(channelName + str(j)), 'final'] = timebandFrameNonFCTMon.iloc[j]['finalPrice']
                except:
                    channelFrameNonFCTMon.loc[(channelName + str(j)), 'final'] = 0.0
                channelFrameNonFCTMon.loc[(channelName + str(j)), 'precedence'] = 0.0
                channelFrameNonFCTMon.loc[(channelName + str(j)), 'fct'] = 0.0

            # Changed value add condition for billing - no impact if value add is true/false
            try:
                billingPrice = timebandFrameNonFCTMon.iloc[j]['billingPrice']
            except:
                billingPrice = 0.0
            numberOfMonths = timebandFrameNonFCTMon.iloc[j]['billingFCT']
            try:
                channelFrameNonFCTMon.loc[(channelName + str(j)), 'recommendedBilling'] = timebandFrameNonFCTMon.iloc[j]['recommendedPrice']
            except:
                channelFrameNonFCTMon.loc[(channelName + str(j)), 'recommendedBilling'] = 0.0

            try:
                channelFrameNonFCTMon.loc[(channelName + str(j)), 'rateCardBilling'] = timebandFrameNonFCTMon.iloc[j][
                    'rateCardPrice']
            except:
                channelFrameNonFCTMon.loc[(channelName + str(j)), 'rateCardBilling'] = 0.0

            try:
                channelFrameNonFCTMon.loc[(channelName + str(j)), 'finalBilling'] = timebandFrameNonFCTMon.iloc[j][
                                                                                        'billingPrice'] * numberOfMonths
            except:
                channelFrameNonFCTMon.loc[(channelName + str(j)), 'finalBilling'] = 0.0
            channelFrameNonFCTMon.loc[(channelName + str(j)), 'precedenceBilling'] = 0.0
            channelFrameNonFCTMon.loc[(channelName + str(j)), 'fctBilling'] = 0.0

        timebandFrameNonFCTExp = pd.DataFrame(list(channelFrame.iloc[i]['nonFCTExposure']['timebands']))
        if (len(timebandFrameNonFCTExp) == 0 or len(timebandFrameNonFCTExp[timebandFrameNonFCTExp["valueAdd"] == False]) == 0):
            channelFrameNonFCTExp.loc[(channelName + str(0)), 'channelName'] = channelName
            channelFrameNonFCTExp.loc[(channelName + str(0)), 'recommended'] = 0.0
            channelFrameNonFCTExp.loc[(channelName + str(0)), 'rateCard'] = 0.0
            channelFrameNonFCTExp.loc[(channelName + str(0)), 'final'] = 0.0
            channelFrameNonFCTExp.loc[(channelName + str(0)), 'precedence'] = 0.0
            channelFrameNonFCTExp.loc[(channelName + str(0)), 'fct'] = 0.0
            channelFrameNonFCTExp.loc[(channelName + str(0)), 'recommendedBilling'] = 0.0
            channelFrameNonFCTExp.loc[(channelName + str(0)), 'rateCardBilling'] = 0.0
            channelFrameNonFCTExp.loc[(channelName + str(0)), 'finalBilling'] = 0.0
            channelFrameNonFCTExp.loc[(channelName + str(0)), 'precedenceBilling'] = 0.0
            channelFrameNonFCTExp.loc[(channelName + str(0)), 'fctBilling'] = 0.0
        for j in range(len(timebandFrameNonFCTExp)):
            channelFrameNonFCTExp.loc[(channelName + str(j)), 'channelName'] = channelName
            if timebandFrameNonFCTExp.iloc[j]['valueAdd'] == False:
                try:
                    totalExposures = timebandFrameNonFCTExp.iloc[j]['totalExposures']
                except:
                    totalExposures = 0.0
                try:
                    channelFrameNonFCTExp.loc[(channelName + str(j)), 'recommended'] = timebandFrameNonFCTExp.iloc[j][
                                                                                     'recommendedPrice'] \
                                                                                 * totalExposures
                except:
                    channelFrameNonFCTExp.loc[(channelName + str(j)), 'recommended'] = 0.0

                try:
                    channelFrameNonFCTExp.loc[(channelName + str(j)), 'rateCard'] = timebandFrameNonFCTExp.iloc[j][
                                                                                    'rateCardPrice'] * totalExposures
                except:
                    channelFrameNonFCTExp.loc[(channelName + str(j)), 'rateCard'] = 0.0

                try:
                    channelFrameNonFCTExp.loc[(channelName + str(j)), 'final'] = timebandFrameNonFCTExp.iloc[j][
                                                                                 'finalPrice'] * totalExposures
                except:
                    channelFrameNonFCTExp.loc[(channelName + str(j)), 'final'] = 0.0

                channelFrameNonFCTExp.loc[(channelName + str(j)), 'precedence'] = 0.0
                channelFrameNonFCTExp.loc[(channelName + str(j)), 'fct'] = 0.0

            # Changed value add condition for billing - no impact if value add is true/false
            try:
                totalExposuresBilling = timebandFrameNonFCTExp.iloc[j]['billingFCT']
            except:
                totalExposuresBilling = 0.0
            try:
                channelFrameNonFCTExp.loc[(channelName + str(j)), 'recommendedBilling'] = timebandFrameNonFCTExp.iloc[j][
                                                                                 'recommendedPrice'] \
                                                                             * totalExposuresBilling
            except:
                channelFrameNonFCTExp.loc[(channelName + str(j)), 'recommendedBilling'] = 0.0

            try:
                channelFrameNonFCTExp.loc[(channelName + str(j)), 'rateCardBilling'] = timebandFrameNonFCTExp.iloc[j][
                                                                                'rateCardPrice'] * totalExposuresBilling
            except:
                channelFrameNonFCTExp.loc[(channelName + str(j)), 'rateCardBilling'] = 0.0

            try:
                channelFrameNonFCTExp.loc[(channelName + str(j)), 'finalBilling'] = timebandFrameNonFCTExp.iloc[j][
                                                                             'billingPrice'] * totalExposuresBilling
            except:
                channelFrameNonFCTExp.loc[(channelName + str(j)), 'finalBilling'] = 0.0

            channelFrameNonFCTExp.loc[(channelName + str(j)), 'precedenceBilling'] = 0.0
            channelFrameNonFCTExp.loc[(channelName + str(j)), 'fctBilling'] = 0.0

        timebandFrameTicker = pd.DataFrame(list(channelFrame.iloc[i]['ticker']['timebands']))
        if (len(timebandFrameTicker) == 0 or len(timebandFrameTicker[timebandFrameTicker["valueAdd"] == False]) == 0):
            channelFrameTicker.loc[(channelName + str(0)), 'channelName'] = channelName
            channelFrameTicker.loc[(channelName + str(0)), 'recommended'] = 0.0
            channelFrameTicker.loc[(channelName + str(0)), 'rateCard'] = 0.0
            channelFrameTicker.loc[(channelName + str(0)), 'final'] = 0.0
            channelFrameTicker.loc[(channelName + str(0)), 'precedence'] = 0.0
            channelFrameTicker.loc[(channelName + str(0)), 'fct'] = 0.0
            channelFrameTicker.loc[(channelName + str(0)), 'recommendedBilling'] = 0.0
            channelFrameTicker.loc[(channelName + str(0)), 'rateCardBilling'] = 0.0
            channelFrameTicker.loc[(channelName + str(0)), 'finalBilling'] = 0.0
            channelFrameTicker.loc[(channelName + str(0)), 'precedenceBilling'] = 0.0
            channelFrameTicker.loc[(channelName + str(0)), 'fctBilling'] = 0.0
        for j in range(len(timebandFrameTicker)):
            channelFrameTicker.loc[(channelName + str(j)), 'channelName'] = channelName
            if timebandFrameTicker.iloc[j]['valueAdd'] == False:
                try:
                    channelFrameTicker.loc[(channelName + str(j)), 'recommended'] = timebandFrameTicker.iloc[j][
                                                                                               'recommendedPrice']
                except:
                    channelFrameTicker.loc[(channelName + str(j)), 'recommended'] = 0.0

                try:
                    channelFrameTicker.loc[(channelName + str(j)), 'rateCard'] = timebandFrameTicker.iloc[j][
                                                                                            'rateCardPrice']
                except:
                    channelFrameTicker.loc[(channelName + str(j)), 'rateCard'] = 0.0

                try:
                    channelFrameTicker.loc[(channelName + str(j)), 'final'] = timebandFrameTicker.iloc[j][
                                                                                         'finalPrice']
                except:
                    channelFrameTicker.loc[(channelName + str(j)), 'final'] = 0.0

                channelFrameTicker.loc[(channelName + str(j)), 'precedence'] = 0.0
                channelFrameTicker.loc[(channelName + str(j)), 'fct'] = 0.0

            # Changed value add condition for billing - no impact if value add is true/false
            try:
                channelFrameTicker.loc[(channelName + str(j)), 'recommendedBilling'] = timebandFrameTicker.iloc[j][
                                                                                           'recommendedPrice']
            except:
                channelFrameTicker.loc[(channelName + str(j)), 'recommendedBilling'] = 0.0
            try:
                channelFrameTicker.loc[(channelName + str(j)), 'rateCardBilling'] = timebandFrameTicker.iloc[j][
                                                                                        'rateCardPrice']
            except:
                channelFrameTicker.loc[(channelName + str(j)), 'rateCardBilling'] = 0.0
            try:
                channelFrameTicker.loc[(channelName + str(j)), 'finalBilling'] = timebandFrameTicker.iloc[j]['billingPrice']
            except:
                channelFrameTicker.loc[(channelName + str(j)), 'finalBilling'] = 0.0

            channelFrameTicker.loc[(channelName + str(j)), 'precedenceBilling'] = 0.0
            channelFrameTicker.loc[(channelName + str(j)), 'fctBilling'] = 0.0


        timebandFrameAston = pd.DataFrame(list(channelFrame.iloc[i]['politicalAston']['timebands']))
        if (len(timebandFrameAston) == 0):
            channelFrameAston.loc[(channelName + str(0)), 'channelName'] = channelName
            channelFrameAston.loc[(channelName + str(0)), 'recommended'] = 0.0
            channelFrameAston.loc[(channelName + str(0)), 'rateCard'] = 0.0
            channelFrameAston.loc[(channelName + str(0)), 'final'] = 0.0
            channelFrameAston.loc[(channelName + str(0)), 'precedence'] = 0.0
            channelFrameAston.loc[(channelName + str(0)), 'fct'] = 0.0
            channelFrameAston.loc[(channelName + str(0)), 'recommendedBilling'] = 0.0
            channelFrameAston.loc[(channelName + str(0)), 'rateCardBilling'] = 0.0
            channelFrameAston.loc[(channelName + str(0)), 'finalBilling'] = 0.0
            channelFrameAston.loc[(channelName + str(0)), 'precedenceBilling'] = 0.0
            channelFrameAston.loc[(channelName + str(0)), 'fctBilling'] = 0.0
        for j in range(len(timebandFrameAston)):
            #if timebandFrameNonFCTExp.iloc[j]['valueAdd'] == False:
            channelFrameAston.loc[(channelName + str(j)), 'channelName'] = channelName
            try:
                totalExposures = timebandFrameAston.iloc[j]['totalExposures']
            except:
                totalExposures = 0.0
            try:
                totalExposuresBilling = timebandFrameAston.iloc[j]['billingFCT']
            except:
                totalExposuresBilling = 0.0

            try:
                channelFrameAston.loc[(channelName + str(j)), 'recommended'] = timebandFrameAston.iloc[j][
                                                                                       'recommendedPrice']*totalExposures
            except:
                channelFrameAston.loc[(channelName + str(j)), 'recommended'] = 0.0

            try:
                channelFrameAston.loc[(channelName + str(j)), 'rateCard'] = timebandFrameAston.iloc[j][
                                                                                    'rateCardPrice']*totalExposures
            except:
                channelFrameAston.loc[(channelName + str(j)), 'rateCard'] = 0.0

            try:
                channelFrameAston.loc[(channelName + str(j)), 'final'] = timebandFrameAston.iloc[j][
                                                                                 'finalPrice']*totalExposures
            except:
                channelFrameAston.loc[(channelName + str(j)), 'final'] = 0.0

            channelFrameAston.loc[(channelName + str(j)), 'precedence'] = 0.0
            channelFrameAston.loc[(channelName + str(j)), 'fct'] = 0.0
            try:
                channelFrameAston.loc[(channelName + str(j)), 'recommendedBilling'] = timebandFrameAston.iloc[j][
                                                                                       'recommendedPrice']*totalExposuresBilling
            except:
                channelFrameAston.loc[(channelName + str(j)), 'recommendedBilling'] = 0.0

            try:
                channelFrameAston.loc[(channelName + str(j)), 'rateCardBilling'] = timebandFrameAston.iloc[j][
                                                                                    'rateCardPrice']*totalExposuresBilling
            except:
                channelFrameAston.loc[(channelName + str(j)), 'rateCardBilling'] = 0.0

            try:
                channelFrameAston.loc[(channelName + str(j)), 'finalBilling'] = timebandFrameAston.iloc[j][
                                                                                 'billingPrice']*totalExposuresBilling
            except:
                channelFrameAston.loc[(channelName + str(j)), 'finalBilling'] = 0.0

            channelFrameAston.loc[(channelName + str(j)), 'precedenceBilling'] = 0.0
            channelFrameAston.loc[(channelName + str(j)), 'fctBilling'] = 0.0

        timebandFrameLBand = pd.DataFrame(list(channelFrame.iloc[i]['politicalLBand']['timebands']))
        if (len(timebandFrameLBand) == 0):
            channelFrameLBand.loc[(channelName + str(0)), 'channelName'] = channelName
            channelFrameLBand.loc[(channelName + str(0)), 'recommended'] = 0.0
            channelFrameLBand.loc[(channelName + str(0)), 'rateCard'] = 0.0
            channelFrameLBand.loc[(channelName + str(0)), 'final'] = 0.0
            channelFrameLBand.loc[(channelName + str(0)), 'precedence'] = 0.0
            channelFrameLBand.loc[(channelName + str(0)), 'fct'] = 0.0
            channelFrameLBand.loc[(channelName + str(0)), 'recommendedBilling'] = 0.0
            channelFrameLBand.loc[(channelName + str(0)), 'rateCardBilling'] = 0.0
            channelFrameLBand.loc[(channelName + str(0)), 'finalBilling'] = 0.0
            channelFrameLBand.loc[(channelName + str(0)), 'precedenceBilling'] = 0.0
            channelFrameLBand.loc[(channelName + str(0)), 'fctBilling'] = 0.0
        for j in range(len(timebandFrameLBand)):
            # if timebandFrameNonFCTExp.iloc[j]['valueAdd'] == False:
            channelFrameLBand.loc[(channelName + str(j)), 'channelName'] = channelName
            try:
                totalExposures = timebandFrameLBand.iloc[j]['totalExposures']
            except:
                totalExposures = 0.0
            try:
                totalExposuresBilling = timebandFrameLBand.iloc[j]['billingFCT']
            except:
                totalExposuresBilling = 0.0
            try:
                channelFrameLBand.loc[(channelName + str(j)), 'recommended'] = timebandFrameLBand.iloc[j][
                                                                                       'recommendedPrice']*totalExposures
            except:
                channelFrameLBand.loc[(channelName + str(j)), 'recommended'] = 0.0

            try:
                channelFrameLBand.loc[(channelName + str(j)), 'rateCard'] = timebandFrameLBand.iloc[j][
                                                                                    'rateCardPrice']*totalExposures
            except:
                channelFrameLBand.loc[(channelName + str(j)), 'rateCard'] = 0.0

            try:
                channelFrameLBand.loc[(channelName + str(j)), 'final'] = timebandFrameLBand.iloc[j][
                                                                                 'finalPrice']*totalExposures
            except:
                channelFrameLBand.loc[(channelName + str(j)), 'final'] = 0.0
            channelFrameLBand.loc[(channelName + str(j)), 'precedence'] = 0.0
            channelFrameLBand.loc[(channelName + str(j)), 'fct'] = 0.0
            try:
                channelFrameLBand.loc[(channelName + str(j)), 'recommendedBilling'] = timebandFrameLBand.iloc[j][
                                                                                       'recommendedPrice']*totalExposuresBilling
            except:
                channelFrameLBand.loc[(channelName + str(j)), 'recommendedBilling'] = 0.0

            try:
                channelFrameLBand.loc[(channelName + str(j)), 'rateCardBilling'] = timebandFrameLBand.iloc[j][
                                                                                    'rateCardPrice']*totalExposuresBilling
            except:
                channelFrameLBand.loc[(channelName + str(j)), 'rateCardBilling'] = 0.0

            try:
                channelFrameLBand.loc[(channelName + str(j)), 'finalBilling'] = timebandFrameLBand.iloc[j][
                                                                                 'billingPrice']*totalExposuresBilling
            except:
                channelFrameLBand.loc[(channelName + str(j)), 'finalBilling'] = 0.0
            channelFrameLBand.loc[(channelName + str(j)), 'precedenceBilling'] = 0.0
            channelFrameLBand.loc[(channelName + str(j)), 'fctBilling'] = 0.0

    channelFrameNonFCTExpAgg = channelFrameNonFCTExp.groupby(["channelName"]).sum()
    channelFrameNonFCTMonAgg = channelFrameNonFCTMon.groupby(["channelName"]).sum()
    channelFrameTickerAgg = channelFrameTicker.groupby(["channelName"]).sum()
    channelFrameAstonAgg = channelFrameAston.groupby(["channelName"]).sum()
    channelFrameLBandAgg = channelFrameLBand.groupby(["channelName"]).sum()
    channelFrameNonFCT = channelFrameNonFCTMon.append([channelFrameNonFCTExp, channelFrameLBand,channelFrameAston,channelFrameTicker])
    channelFrameNonFCTAgg = channelFrameNonFCT.groupby(['channelName']).sum()
    return channelFrameNonFCTAgg, channelFrameNonFCTMonAgg, channelFrameNonFCTExpAgg, channelFrameTickerAgg, channelFrameLBandAgg, channelFrameAstonAgg

#added for retail

def getChannelFrameRetail(gridRec):
    channelFrame = pd.DataFrame()
    channelFrameBRLBand = pd.DataFrame()
    channelFrameBRAston = pd.DataFrame()
    channelFrameBRTicker = pd.DataFrame()
    channelFrameBRSpotBuy = pd.DataFrame()
    channelFrameRetailSpotBuy = pd.DataFrame()
    channelFrame = pd.DataFrame(list(gridRec['channel'])).fillna(0.0)
    for i in range(len(channelFrame)):
        channelName = channelFrame.iloc[i]['name']
        timebandFrameBRLBand = pd.DataFrame(list(channelFrame.iloc[i]['retailBRLBand']['timebands']))
        if (len(timebandFrameBRLBand) == 0):
            channelFrameBRLBand.loc[(channelName + str(0)), 'channelName'] = channelName
            channelFrameBRLBand.loc[(channelName + str(0)), 'recommended'] = 0.0
            channelFrameBRLBand.loc[(channelName + str(0)), 'rateCard'] = 0.0
            channelFrameBRLBand.loc[(channelName + str(0)), 'final'] = 0.0
            channelFrameBRLBand.loc[(channelName + str(0)), 'precedence'] = 0.0
            channelFrameBRLBand.loc[(channelName + str(0)), 'fct'] = 0.0
            channelFrameBRLBand.loc[(channelName + str(0)), 'recommendedBilling'] = 0.0
            channelFrameBRLBand.loc[(channelName + str(0)), 'rateCardBilling'] = 0.0
            channelFrameBRLBand.loc[(channelName + str(0)), 'finalBilling'] = 0.0
            channelFrameBRLBand.loc[(channelName + str(0)), 'precedenceBilling'] = 0.0
            channelFrameBRLBand.loc[(channelName + str(0)), 'fctBilling'] = 0.0
        for j in range(len(timebandFrameBRLBand)):
            channelFrameBRLBand.loc[(channelName + str(j)), 'channelName'] = channelName
            try:
                totalExposures = timebandFrameBRLBand.iloc[j]['totalExposures']
            except:
                totalExposures = 0.0
            try:
                totalExposuresBilling = timebandFrameBRLBand.iloc[j]['billingFCT']
            except:
                totalExposuresBilling = 0.0

            try:
                channelFrameBRLBand.loc[(channelName + str(j)), 'recommended'] = timebandFrameBRLBand.iloc[j][
                                                                                     'recommendedPrice'] \
                                                                                 * totalExposures
            except:
                channelFrameBRLBand.loc[(channelName + str(j)), 'recommended'] = 0.0

            try:
                channelFrameBRLBand.loc[(channelName + str(j)), 'rateCard'] = timebandFrameBRLBand.iloc[j][
                                                                                        'rateCardPrice'] * totalExposures
            except:
                channelFrameBRLBand.loc[(channelName + str(j)), 'rateCard'] = 0.0

            try:
                channelFrameBRLBand.loc[(channelName + str(j)), 'final'] = timebandFrameBRLBand.iloc[j][
                                                                                     'finalPrice'] * totalExposures
            except:
                channelFrameBRLBand.loc[(channelName + str(j)), 'final'] = 0.0

            channelFrameBRLBand.loc[(channelName + str(j)), 'precedence'] = 0.0
            channelFrameBRLBand.loc[(channelName + str(j)), 'fct'] = 0.0
            try:
                channelFrameBRLBand.loc[(channelName + str(j)), 'recommendedBilling'] = timebandFrameBRLBand.iloc[j][
                                                                                     'recommendedPrice'] \
                                                                                 * totalExposuresBilling
            except:
                channelFrameBRLBand.loc[(channelName + str(j)), 'recommendedBilling'] = 0.0

            try:
                channelFrameBRLBand.loc[(channelName + str(j)), 'rateCardBilling'] = timebandFrameBRLBand.iloc[j][
                                                                                        'rateCardPrice'] * totalExposuresBilling
            except:
                channelFrameBRLBand.loc[(channelName + str(j)), 'rateCardBilling'] = 0.0

            try:
                channelFrameBRLBand.loc[(channelName + str(j)), 'finalBilling'] = timebandFrameBRLBand.iloc[j][
                                                                                     'billingPrice'] * totalExposuresBilling
            except:
                channelFrameBRLBand.loc[(channelName + str(j)), 'finalBilling'] = 0.0

            channelFrameBRLBand.loc[(channelName + str(j)), 'precedenceBilling'] = 0.0
            channelFrameBRLBand.loc[(channelName + str(j)), 'fctBilling'] = 0.0


        timebandFrameBRAston = pd.DataFrame(list(channelFrame.iloc[i]['retailBRAston']['timebands']))
        if (len(timebandFrameBRAston) == 0):
            channelFrameBRAston.loc[(channelName + str(0)), 'channelName'] = channelName
            channelFrameBRAston.loc[(channelName + str(0)), 'recommended'] = 0.0
            channelFrameBRAston.loc[(channelName + str(0)), 'rateCard'] = 0.0
            channelFrameBRAston.loc[(channelName + str(0)), 'final'] = 0.0
            channelFrameBRAston.loc[(channelName + str(0)), 'precedence'] = 0.0
            channelFrameBRAston.loc[(channelName + str(0)), 'fct'] = 0.0
            channelFrameBRAston.loc[(channelName + str(0)), 'recommendedBilling'] = 0.0
            channelFrameBRAston.loc[(channelName + str(0)), 'rateCardBilling'] = 0.0
            channelFrameBRAston.loc[(channelName + str(0)), 'finalBilling'] = 0.0
            channelFrameBRAston.loc[(channelName + str(0)), 'precedenceBilling'] = 0.0
            channelFrameBRAston.loc[(channelName + str(0)), 'fctBilling'] = 0.0
        for j in range(len(timebandFrameBRAston)):
            #if timebandFrameNonFCTExp.iloc[j]['valueAdd'] == False:
            channelFrameBRAston.loc[(channelName + str(j)), 'channelName'] = channelName
            try:
                totalExposures = timebandFrameBRAston.iloc[j]['totalExposures']
            except:
                totalExposures = 0.0
            try:
                totalExposuresBilling = timebandFrameBRAston.iloc[j]['billingFCT']
            except:
                totalExposuresBilling = 0.0

            try:
                channelFrameBRAston.loc[(channelName + str(j)), 'recommended'] = timebandFrameBRAston.iloc[j][
                                                                                       'recommendedPrice']*totalExposures
            except:
                channelFrameBRAston.loc[(channelName + str(j)), 'recommended'] = 0.0

            try:
                channelFrameBRAston.loc[(channelName + str(j)), 'rateCard'] = timebandFrameBRAston.iloc[j][
                                                                                    'rateCardPrice']*totalExposures
            except:
                channelFrameBRAston.loc[(channelName + str(j)), 'rateCard'] = 0.0

            try:
                channelFrameBRAston.loc[(channelName + str(j)), 'final'] = timebandFrameBRAston.iloc[j][
                                                                                 'finalPrice']*totalExposures
            except:
                channelFrameBRAston.loc[(channelName + str(j)), 'final'] = 0.0

            channelFrameBRAston.loc[(channelName + str(j)), 'precedence'] = 0.0
            channelFrameBRAston.loc[(channelName + str(j)), 'fct'] = 0.0
            try:
                channelFrameBRAston.loc[(channelName + str(j)), 'recommendedBilling'] = timebandFrameBRAston.iloc[j][
                                                                                       'recommendedPrice']*totalExposuresBilling
            except:
                channelFrameBRAston.loc[(channelName + str(j)), 'recommendedBilling'] = 0.0

            try:
                channelFrameBRAston.loc[(channelName + str(j)), 'rateCardBilling'] = timebandFrameBRAston.iloc[j][
                                                                                    'rateCardPrice']*totalExposuresBilling
            except:
                channelFrameBRAston.loc[(channelName + str(j)), 'rateCardBilling'] = 0.0

            try:
                channelFrameBRAston.loc[(channelName + str(j)), 'finalBilling'] = timebandFrameBRAston.iloc[j][
                                                                                 'billingPrice']*totalExposuresBilling
            except:
                channelFrameBRAston.loc[(channelName + str(j)), 'finalBilling'] = 0.0

            channelFrameBRAston.loc[(channelName + str(j)), 'precedenceBilling'] = 0.0
            channelFrameBRAston.loc[(channelName + str(j)), 'fctBilling'] = 0.0

        timebandFrameBRSpotBuy = pd.DataFrame(list(channelFrame.iloc[i]['retailBRSpotBuy']['timebands']))
        if (len(timebandFrameBRSpotBuy) == 0):
            channelFrameBRSpotBuy.loc[(channelName + str(0)), 'channelName'] = channelName
            channelFrameBRSpotBuy.loc[(channelName + str(0)), 'recommended'] = 0.0
            channelFrameBRSpotBuy.loc[(channelName + str(0)), 'rateCard'] = 0.0
            channelFrameBRSpotBuy.loc[(channelName + str(0)), 'final'] = 0.0
            channelFrameBRSpotBuy.loc[(channelName + str(0)), 'precedence'] = 0.0
            channelFrameBRSpotBuy.loc[(channelName + str(0)), 'fct'] = 0.0
            channelFrameBRSpotBuy.loc[(channelName + str(0)), 'recommendedBilling'] = 0.0
            channelFrameBRSpotBuy.loc[(channelName + str(0)), 'rateCardBilling'] = 0.0
            channelFrameBRSpotBuy.loc[(channelName + str(0)), 'finalBilling'] = 0.0
            channelFrameBRSpotBuy.loc[(channelName + str(0)), 'precedenceBilling'] = 0.0
            channelFrameBRSpotBuy.loc[(channelName + str(0)), 'fctBilling'] = 0.0
        for j in range(len(timebandFrameBRSpotBuy)):
            # if timebandFrameNonFCTExp.iloc[j]['valueAdd'] == False:
            channelFrameBRSpotBuy.loc[(channelName + str(j)), 'channelName'] = channelName
            try:
                finalFCT = timebandFrameBRSpotBuy.iloc[j]['finalFCT'] or 0.0
            except:
                finalFCT = 0.0
            try:
                billingFCT = timebandFrameBRSpotBuy.iloc[j]['billingFCT'] or 0.0
            except:
                billingFCT = 0.0
            try:
                channelFrameBRSpotBuy.loc[(channelName + str(j)), 'recommended'] = timebandFrameBRSpotBuy.iloc[j][
                                                                                     'recommendedPrice'] * finalFCT / 10.0
            except:
                channelFrameBRSpotBuy.loc[(channelName + str(j)), 'recommended'] = 0.0

            try:
                channelFrameBRSpotBuy.loc[(channelName + str(j)), 'rateCard'] = timebandFrameBRSpotBuy.iloc[j][
                                                                                  'rateCardPrice'] * finalFCT / 10.0
            except:
                channelFrameBRSpotBuy.loc[(channelName + str(j)), 'rateCard'] = 0.0
            try:
                channelFrameBRSpotBuy.loc[(channelName + str(j)), 'final'] = timebandFrameBRSpotBuy.iloc[j][
                                                                               'finalPrice'] * finalFCT / 10.0
            except:
                channelFrameBRSpotBuy.loc[(channelName + str(j)), 'final'] = 0.0

            try:
                channelFrameBRSpotBuy.loc[(channelName + str(j)), 'precedence'] = timebandFrameBRSpotBuy.iloc[j][
                                                                                    'previousPrice'] * finalFCT / 10.0
            except:
                channelFrameBRSpotBuy.loc[(channelName + str(j)), 'precedence'] = 0.0

            channelFrameBRSpotBuy.loc[(channelName + str(j)), 'fct'] = finalFCT
            try:
                channelFrameBRSpotBuy.loc[(channelName + str(j)), 'recommendedBilling'] = timebandFrameBRSpotBuy.iloc[j][
                                                                                     'recommendedPrice'] * billingFCT / 10.0
            except:
                channelFrameBRSpotBuy.loc[(channelName + str(j)), 'recommendedBilling'] = 0.0

            try:
                channelFrameBRSpotBuy.loc[(channelName + str(j)), 'rateCardBilling'] = timebandFrameBRSpotBuy.iloc[j][
                                                                                  'rateCardPrice'] * billingFCT / 10.0
            except:
                channelFrameBRSpotBuy.loc[(channelName + str(j)), 'rateCardBilling'] = 0.0
            try:
                channelFrameBRSpotBuy.loc[(channelName + str(j)), 'finalBilling'] = timebandFrameBRSpotBuy.iloc[j][
                                                                               'billingPrice'] * billingFCT / 10.0
            except:
                channelFrameBRSpotBuy.loc[(channelName + str(j)), 'finalBilling'] = 0.0

            try:
                channelFrameBRSpotBuy.loc[(channelName + str(j)), 'precedenceBilling'] = timebandFrameBRSpotBuy.iloc[j][
                                                                                    'previousPrice'] * billingFCT / 10.0
            except:
                channelFrameBRSpotBuy.loc[(channelName + str(j)), 'precedenceBilling'] = 0.0

            channelFrameBRSpotBuy.loc[(channelName + str(j)), 'fct'] = finalFCT

        timebandFrameRetailSpotBuy = pd.DataFrame(list(channelFrame.iloc[i]['retailSpotBuy']['timebands']))
        timebandFrameRetailSpotBuy["valueAdd"] = False if "valueAdd" not in timebandFrameRetailSpotBuy.columns else timebandFrameRetailSpotBuy["valueAdd"]
        if (len(timebandFrameRetailSpotBuy) == 0 or len(timebandFrameRetailSpotBuy[timebandFrameRetailSpotBuy["valueAdd"] != True]) == 0):
            channelFrameRetailSpotBuy.loc[(channelName + str(0)), 'channelName'] = channelName
            channelFrameRetailSpotBuy.loc[(channelName + str(0)), 'recommended'] = 0.0
            channelFrameRetailSpotBuy.loc[(channelName + str(0)), 'rateCard'] = 0.0
            channelFrameRetailSpotBuy.loc[(channelName + str(0)), 'final'] = 0.0
            channelFrameRetailSpotBuy.loc[(channelName + str(0)), 'precedence'] = 0.0
            channelFrameRetailSpotBuy.loc[(channelName + str(0)), 'fct'] = 0.0
            channelFrameRetailSpotBuy.loc[(channelName + str(0)), 'recommendedBilling'] = 0.0
            channelFrameRetailSpotBuy.loc[(channelName + str(0)), 'rateCardBilling'] = 0.0
            channelFrameRetailSpotBuy.loc[(channelName + str(0)), 'finalBilling'] = 0.0
            channelFrameRetailSpotBuy.loc[(channelName + str(0)), 'precedenceBilling'] = 0.0
            channelFrameRetailSpotBuy.loc[(channelName + str(0)), 'fctBilling'] = 0.0
        for j in range(len(timebandFrameRetailSpotBuy)):
            channelFrameRetailSpotBuy.loc[(channelName + str(j)), 'channelName'] = channelName
            if timebandFrameRetailSpotBuy.iloc[j]['valueAdd'] != True:
                try:
                    finalFCT = timebandFrameRetailSpotBuy.iloc[j]['finalFCT'] or 0.0
                except:
                    finalFCT = 0.0

                try:
                    channelFrameRetailSpotBuy.loc[(channelName + str(j)), 'recommended'] = timebandFrameRetailSpotBuy.iloc[j][
                                                                                           'recommendedPrice'] * finalFCT / 10.0
                except:
                    channelFrameRetailSpotBuy.loc[(channelName + str(j)), 'recommended'] = 0.0

                try:
                    channelFrameRetailSpotBuy.loc[(channelName + str(j)), 'rateCard'] = timebandFrameRetailSpotBuy.iloc[j][
                                                                                        'rateCardPrice'] * finalFCT / 10.0
                except:
                    channelFrameRetailSpotBuy.loc[(channelName + str(j)), 'rateCard'] = 0.0
                try:
                    channelFrameRetailSpotBuy.loc[(channelName + str(j)), 'final'] = timebandFrameRetailSpotBuy.iloc[j][
                                                                                     'finalPrice'] * finalFCT / 10.0
                except:
                    channelFrameRetailSpotBuy.loc[(channelName + str(j)), 'final'] = 0.0

                try:
                    channelFrameRetailSpotBuy.loc[(channelName + str(j)), 'precedence'] = timebandFrameRetailSpotBuy.iloc[j][
                                                                                          'previousPrice'] * finalFCT / 10.0
                except:
                    channelFrameRetailSpotBuy.loc[(channelName + str(j)), 'precedence'] = 0.0

                channelFrameRetailSpotBuy.loc[(channelName + str(j)), 'fct'] = finalFCT

            try:
                billingFCT = timebandFrameRetailSpotBuy.iloc[j]['billingFCT'] or 0.0
            except:
                billingFCT = 0.0
            try:
                channelFrameRetailSpotBuy.loc[(channelName + str(j)), 'recommendedBilling'] = timebandFrameRetailSpotBuy.iloc[j][
                                                                                       'recommendedPrice'] * billingFCT / 10.0
            except:
                channelFrameRetailSpotBuy.loc[(channelName + str(j)), 'recommendedBilling'] = 0.0

            try:
                channelFrameRetailSpotBuy.loc[(channelName + str(j)), 'rateCardBilling'] = timebandFrameRetailSpotBuy.iloc[j][
                                                                                    'rateCardPrice'] * billingFCT / 10.0
            except:
                channelFrameRetailSpotBuy.loc[(channelName + str(j)), 'rateCardBilling'] = 0.0
            try:
                channelFrameRetailSpotBuy.loc[(channelName + str(j)), 'finalBilling'] = timebandFrameRetailSpotBuy.iloc[j][
                                                                                 'billingPrice'] * billingFCT / 10.0
            except:
                channelFrameRetailSpotBuy.loc[(channelName + str(j)), 'finalBilling'] = 0.0

            try:
                channelFrameRetailSpotBuy.loc[(channelName + str(j)), 'precedenceBilling'] = timebandFrameRetailSpotBuy.iloc[j][
                                                                                      'previousPrice'] * billingFCT / 10.0
            except:
                channelFrameRetailSpotBuy.loc[(channelName + str(j)), 'precedenceBilling'] = 0.0

            channelFrameRetailSpotBuy.loc[(channelName + str(j)), 'fct'] = billingFCT

    channelFrameBRAstonAgg = channelFrameBRAston.groupby(["channelName"]).sum()
    channelFrameBRLBandAgg = channelFrameBRLBand.groupby(["channelName"]).sum()
    channelFrameBRSpotBuyAgg = channelFrameBRSpotBuy.groupby(["channelName"]).sum()
    channelFrameRetailSpotBuyAgg = channelFrameRetailSpotBuy.groupby(["channelName"]).sum()
    channelFrameBR = channelFrameBRSpotBuy.append([channelFrameBRAston, channelFrameBRLBand])
    channelFrameBRAgg = channelFrameBR.groupby(['channelName']).sum()
    channelFrameRetail = channelFrameBRSpotBuy.append([channelFrameBRAston, channelFrameBRLBand, channelFrameRetailSpotBuy])
    channelFrameRetailAgg = channelFrameRetail.groupby(['channelName']).sum()
    return channelFrameRetailAgg, channelFrameBRAgg, channelFrameRetailSpotBuyAgg, channelFrameBRAstonAgg, channelFrameBRLBandAgg, channelFrameBRSpotBuyAgg


def createBudgetFrame(brief):
    budgetFrame = pd.DataFrame()
    channelFrame = pd.DataFrame(list(brief['channel'])).fillna(0.0)

    for i in range(len(channelFrame)):
        channelName = channelFrame.iloc[i]['name']
        budgetFrame.loc[channelName, 'channelName'] = channelFrame.iloc[i]['name']

        # budgetFrame.loc[channelName, 'spotBuy'] = channelFrame.iloc[i]['budget']['spotBuy']
        # budgetFrame.loc[channelName, 'sponsorship'] = channelFrame.iloc[i]['budget']['sponsorship']
        try:
            budgetFrame.loc[channelName, 'spotLight'] = channelFrame.iloc[i]['budget']['spotLight']
        except:
            budgetFrame.loc[channelName, 'spotLight'] = 0.0
        # budgetFrame.loc[channelName, 'nonFCTMonthly'] = channelFrame.iloc[i]['budget']['nonFCTMonthly']
        # budgetFrame.loc[channelName, 'nonFCTExposure'] = channelFrame.iloc[i]['budget']['nonFCTExposure']
        # budgetFrame.loc[channelName, 'minFCT'] = channelFrame.iloc[i]['budget']['minFCT']
        # budgetFrame.loc[channelName, 'totalOutlay'] = channelFrame.iloc[i]['budget']['totalOutlay']

    return budgetFrame

#dictionary for operators
ops = { "BELOW": operator.lt, "<=": operator.le,">=":operator.ge, "ABOVE":operator.gt }

def getAlertRules(dbAlertsRuleMstr):
    dfAlertRules = pd.DataFrame(list(dbAlertsRuleMstr.find({"isActive":True},{"createdAt":0, "updatedAt" : 0,
                                                                                 "createdBy" : 0, "updatedBy": 0})))
    listAlertObject = []
    for i in range(len(dfAlertRules)):
        channel = dfAlertRules["channel"][i]["name"]
        metric = dfAlertRules["metric"][i]
        type = dfAlertRules["type"][i]
        compareValue = dfAlertRules["compareValue"][i]
        comparisonOperator = dfAlertRules["operator"][i]
        compareTo = dfAlertRules["compareTo"][i]
        threshold = dfAlertRules["threshold"][i]
        isActive = dfAlertRules["isActive"][i]
        standard = dfAlertRules["standard"][i]

        alertObject = cAlertRule(channel, metric, type, compareValue,comparisonOperator, compareTo, threshold, isActive, standard)
        listAlertObject.append(alertObject)

    return listAlertObject

def getOutlayBuckets(dbRateRules,channelName):
    bucketDef = dbRateRules.find_one({"channel":channelName})["bucketDef"]
    outlayBuckets=[]
    for i in bucketDef:
        outlayBuckets.append(OutlayRange(i["minOutlay"],i["maxOutlay"],i["bucket"]))
    return outlayBuckets

class OutlayRange:
    def __init__(self,minOutlay,maxOutlay,bucket):
        self.minOutlay = minOutlay
        self.maxOutlay = maxOutlay
        self.bucket = bucket


def getDealOutlayBucket(totalBudget, primaryChannel,category,dbRateRules):
    outlayBuckets = getOutlayBuckets(dbRateRules,primaryChannel)
    precedenceChannelList = ["ABP NEWS"]
    regionalChannelList = ["ABP MAJHA", "ABP ANANDA", "ABP ASMITA","ABP GANGA"]

    bucket = ""

    for range in outlayBuckets:
        if range.minOutlay <= totalBudget <= range.maxOutlay:
            bucket = range.bucket
            print('***Outlaybucket', bucket)

    if primaryChannel.upper() in regionalChannelList and (bucket == '200+' or bucket == '150+') and category != "COMBO":
        bucket = '100+'

    if primaryChannel.upper() in precedenceChannelList and bucket == '10+' and category != "COMBO":
        bucket = '0+'

    return bucket

class cAlertRule:
    def __init__(self,channel,metric,type,compareValue,comparisonOperator,compareTo,threshold,isActive, standard):
        self.channel = channel
        self.metric = metric
        self.type = type
        self.compareValue = compareValue
        self.comparisonOperator = comparisonOperator
        self.compareTo = compareTo
        self.threshold = threshold
        self.isActive = isActive
        self.standard = standard

class cAlerts:
    def __init__(self,channel, dealId, metric, comparisonOperator, compareValue, compareTo, threshold, differencePercent, \
                 finalAmount, compareToAmount):
        self.channel = channel.upper()
        self.dealId = dealId
        self.metric = metric.upper()
        self.comparisonOperator = comparisonOperator
        self.compareValue = compareValue.upper()
        self.compareTo = compareTo.upper()
        self.threshold = threshold
        self.differencePercent = differencePercent
        self.finalAmount = finalAmount
        self.compareToAmount = compareToAmount

    def returnContent(self):
        content = self.compareValue + " "  + self.metric + " : " + str(round(self.finalAmount)) + " IS " +self.comparisonOperator +\
                  " " + self.compareTo + " : " + str(round(self.compareToAmount)) + " BY " + str(round(self.differencePercent)) + "%"
        return content

class cTimeBandAlerts(cAlerts):
    def __init__(self,channel, metric, comparisonOperator, compareValue, compareTo, threshold, differencePercent, \
                 finalAmount, compareToAmount, dow, rateType, spotType, timeband):
        cAlerts.__init__(self, channel, "NONE", metric, comparisonOperator, compareValue, compareTo, threshold, differencePercent, \
                 finalAmount, compareToAmount)
        self.dow = dow.upper()
        self.rateType = rateType.upper()
        self.spotType = spotType
        self.timeband = timeband.upper()

    def returnTimeBandContent(self):
        timeBandContent = self.compareValue + " "  + self.metric + " : " + str(round(self.finalAmount)) + \
                          "FOR " + self.dow + ", " + self.timeband + ", " + self.rateType +\
                          " IS " +self.comparisonOperator + " " + self.compareTo + " : " \
                          + str(round(self.compareToAmount)) + " BY " + str(round(self.differencePercent)) + "%"

        return timeBandContent

if __name__ == '__main__':
    app.run(debug=True)
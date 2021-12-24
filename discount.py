# Developer - Poorva
# Computes all the discounts for a deal at channel/combo level and are updated in the database

from __future__ import division
from datetime import *
import pandas as pd
import pytz
from flask import Flask, jsonify
from flask_pymongo import PyMongo
from flask import request
from dateutil import relativedelta
utc = pytz.UTC
import datetime

app = Flask(__name__)
app.config['MONGO_DBNAME'] = 'ABPDev'
app.config['MONGO_URI'] = "mongodb://abpnonproduser:Hell0Greta@abpnonprod-shard-00-00.fm59u.mongodb.net:27017, \
                           abpnonprod-shard-00-01.fm59u.mongodb.net:27017, \
                           abpnonprod-shard-00-02.fm59u.mongodb.net:27017/ABPDev?replicaSet=atlas-a2pokt-shard-0&ssl=true&authSource=admin"
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

mongo = PyMongo(app, config_prefix='MONGO')
mongo1 = PyMongo(app, config_prefix='MONGO1')
mongo2 = PyMongo(app, config_prefix='MONGO2')

@app.route('/abpDiscount/<dealId>', methods=['GET'])
def discountCalc(dealId):
    print('*** DealId ***',dealId)
    urlString = request.url
    print (urlString)
    if urlString.find('uat') > -1:
        MODE = 'UAT'
        print ('*** Running UAT mode ***')
    elif urlString.find('prod') > -1:
        MODE ='PROD'
        print('*** Running PROD mode ***')
    else:
        MODE = 'DEV'
        print('*** Running DEV mode ***')

    updateFlag = 'Y'
    MODE = 'UAT'
    # Database collections
    if MODE == 'UAT':
        dbBrief = mongo1.db.dealBriefTxn
        dbGrid = mongo1.db.dealGridTxn
        dbRateRules = mongo1.db.dsRateCardRuleMstr
        dbCombo = mongo1.db.comboMstr
        dbRetail = mongo1.db.dsRetailPackageIncentive
        dbNetworkRules = mongo1.db.networkDealRules
        dbParameters = mongo1.db.dsParameters
        dbChannelList = mongo1.db.dsChannalListDiscMstr
    elif MODE == 'PROD':
        dbBrief = mongo2.db.dealBriefTxn
        dbGrid = mongo2.db.dealGridTxn
        dbRateRules = mongo2.db.dsRateCardRuleMstr
        dbCombo = mongo2.db.comboMstr
        dbRetail = mongo2.db.dsRetailPackageIncentive
        dbNetworkRules = mongo2.db.networkDealRules
        dbParameters = mongo2.db.dsParameters
        dbChannelList = mongo2.db.dsChannalListDiscMstr
    else:
        dbBrief = mongo.db.dealBriefTxn
        dbGrid = mongo.db.dealGridTxn
        dbRateRules = mongo.db.dsRateCardRuleMstr
        dbCombo = mongo.db.comboMstr
        dbRetail = mongo.db.dsRetailPackageIncentive
        dbNetworkRules = mongo.db.networkDealRules
        dbParameters = mongo.db.dsParameters
        dbChannelList = mongo.db.dsChannalListDiscMstr

    # Read deal from dealBriefTxn
    print('*** Brief Inputs ***')
    brief = dbBrief.find_one({'dealId': dealId})
    dealStartDate = brief['fromDate']
    dealEndDate = brief['toDate']
    briefCategoryName = brief['category']['name']
    fy, year = getFY(dealStartDate)
    newCombo = brief["newCombo"]
    prevDealCombo = pd.DataFrame(brief['budgetPrevDeal']['newCombo'])
    isNewCombo = False if len(newCombo) == 0 else True
    isPrevDealNewCombo = False if len(prevDealCombo) == 0 else True
    newCombo = pd.DataFrame(newCombo)

    # Read static variables from dsChannalListDiscMstr
    chnlList = dbChannelList.find_one({})
    cntRegionalChannelforDiscount = chnlList['cntRegionalChannelforDiscount']
    digitalPctThreshold = chnlList['digitalPctThreshold']
    digitalIncentConst = chnlList['digitalIncentConst']
    newAdvertiserIncentConst = chnlList['newAdvertiserIncentConst']

    # new combo logic
    print('*** Combo Inputs ***')
    if len(newCombo) == 0:
        comboDF = pd.DataFrame(columns=['name', 'level_1', 'channels', 'comboTotalOutlay', 'comboSpotBuy', 'comboMinFCT', 'comboHighPT', 'isNewAdvertiserIncentive','comboNewAdvInc'])
    else:
        comboDF = newCombo.set_index(['name']).channels.apply(pd.Series).stack().reset_index(name='channels')
        budgetCombo = newCombo.set_index(['name']).budget.apply(pd.Series).reset_index()
        comboDF = comboDF.merge(budgetCombo, how='left', on=['name'])
    comboDF.rename(columns={"channels": "channelName", "name": "comboName", "totalOutlay": "comboTotalOutlay",
                                "spotBuy": "comboSpotBuy", "minFCT": "comboMinFCT", "highPT": "comboHighPT", "isNewAdvertiserIncentive": "comboNewAdvIncFlag"}, inplace=True)

    # Added newAdvertiserIncentive logic at combo level - Poorva - 30-04-2020
    for cmb in range(0,len(comboDF)):
        if comboDF.loc[cmb,'comboNewAdvIncFlag'] == True:
            comboDF.loc[cmb, 'comboNewAdvInc'] = newAdvertiserIncentConst
        else:
            comboDF.loc[cmb, 'comboNewAdvInc'] = 0.0

    # getting combo channels with their proportion values
    if len(newCombo) == 0:
        comboDF['proportionValue'] = 0.0
        comboDF['proportionTotalOutlay'] = 0.0
        comboDF['proportionSpotBuy'] = 0.0
    else:
        propDf = pd.DataFrame(list(dbCombo.find({'name':{'$in':newCombo['name'].tolist()}},{'name':1,'proportion':1})))
        if len(propDf) == 0:
            propDf = pd.DataFrame(list(dbCombo.find({'comboMapping': {'$in': newCombo['name'].tolist()}}, {'name': 1, 'comboMapping': 1, 'proportion': 1})))
        # getting channel wise proportion value and then calculating total outlay and spotbuy for each channel in that combo
        for combo in range(0, len(comboDF)):
            df_combo = propDf[propDf['name'] == comboDF['comboName'][combo]].reset_index()
            if len(df_combo)== 0:
                df_combo = propDf[propDf['comboMapping'] == comboDF['comboName'][combo]].reset_index()
            for n in range(0, len(df_combo['proportion'][0])):
                if comboDF['channelName'][combo] == df_combo['proportion'][0][n]['channel']:
                    comboDF.loc[combo, 'proportionValue'] = df_combo['proportion'][0][n]['value']
        comboDF['proportionTotalOutlay'] = comboDF['proportionValue'] * comboDF['comboTotalOutlay']
        comboDF['proportionSpotBuy'] = comboDF['proportionValue'] * comboDF['comboSpotBuy']

    print('comboDFF')
    print(comboDF[['comboName','channelName','comboTotalOutlay','comboSpotBuy','proportionValue','proportionTotalOutlay','proportionSpotBuy']])

    # new combo logic for prev deal
    if len(prevDealCombo) == 0:
        prevComboDF = pd.DataFrame(columns=['name', 'level_1', 'channels', 'comboTotalOutlay', 'comboSpotBuy', 'comboMinFCT', 'comboHighPT'])
    else:
        prevComboDF = prevDealCombo.set_index(['name']).channels.apply(pd.Series).stack().reset_index(name='channels')
        budgetPrevCombo = prevDealCombo.set_index(['name']).budget.apply(pd.Series).reset_index()
        prevComboDF = prevComboDF.merge(budgetPrevCombo, how='left', on=['name'])
    prevComboDF.rename(columns={"channels": "channelName", "name": "comboName", "totalOutlay": "comboTotalOutlay",
                            "spotBuy": "comboSpotBuy", "minFCT": "comboMinFCT", "highPT": "comboHighPT"}, inplace=True)

    # getting combo channles with their proportion values
    if len(prevDealCombo) == 0:
        prevComboDF['proportionValue'] = 0.0
        prevComboDF['proportionTotalOutlay'] = 0.0
        prevComboDF['proportionSpotBuy'] = 0.0
    else:
        propPrevDf = pd.DataFrame(list(dbCombo.find({'name':{'$in':prevDealCombo['name'].tolist()}},{'name':1,'proportion':1})))
        # getting channel wise proportion value and then calculating total outlay and spotbuy for each channel in that combo
        for combo in range(0, len(prevComboDF)):
            df_prev_combo = propPrevDf[propPrevDf['name'] == prevComboDF['comboName'][combo]].reset_index()
            for n in range(0, len(df_prev_combo['proportion'][0])):
                if prevComboDF['channelName'][combo] == df_prev_combo['proportion'][0][n]['channel']:
                    prevComboDF.loc[combo, 'proportionValue'] = df_prev_combo['proportion'][0][n]['value']
        prevComboDF['proportionTotalOutlay'] = prevComboDF['proportionValue'] * prevComboDF['comboTotalOutlay']
        prevComboDF['proportionSpotBuy'] = prevComboDF['proportionValue'] * prevComboDF['comboSpotBuy']

    print('prevComboDFFFF')
    print(prevComboDF)
    # old advertiser Incentive logic - on entire deal - stored in db for old deals
    try:
        newAdvertiserFlag = brief['addNewAdvertiserIncentive']
    except:
        newAdvertiserFlag = False
    if newAdvertiserFlag == True:
        newAdvertiserIncent = newAdvertiserIncentConst
    else:
        newAdvertiserIncent = 0.0

    # Get Digital budget
    try:
        budgetDigital = brief['budgetDigital']
        if budgetDigital is None:
            budgetDigital = 0.0
    except:
        budgetDigital = 0.0

    # flag for prev budget, by default - true - include always
    try:
        includePrevDeal = brief['includePrevDeal']
    except:
        includePrevDeal = True

    print('*** Budget Frame ***')
    budgetFrame = createBudgetFrame(brief,isNewCombo,comboDF,newAdvertiserIncentConst)
    prevBudgetFrame = createPrevBudgetFrame(brief, includePrevDeal,isPrevDealNewCombo,prevComboDF)
    print('prevBudgetFrame')
    print(prevBudgetFrame[['totalOutlay']])
    prevBudgetFrame = prevBudgetFrame.fillna(0)

    try:
        prevBudgetDigital = brief['prevBudgetDigital']

    except:
        prevBudgetDigital = 0.0

    allChannels, allNetworkChannels,lenNetworkChannels, primaryChannel, totalChannelCount,regionalChannelCount,totalBudget, combinedBudgetFrame, \
    spotLightPct, discountPct = getKeyParameters(prevBudgetFrame,budgetFrame,budgetDigital,prevBudgetDigital,dbNetworkRules,comboDF,prevComboDF,chnlList)

    print('*** Incentives - Channel level ***')
    multiChannelFlag = True if discountPct!=0 else False
    dealPeriodBucket = getDealPeriodBucket(dealStartDate, dealEndDate)
    dealOutlayBucket = getDealOutlayBucket(dbRateRules, totalBudget, primaryChannel, chnlList, year)
    # get outlay incentive - for channel
    combinedBudgetFrame["channelTotalBudget"] = combinedBudgetFrame.groupby("channelName")["totalBudget"].transform("sum")
    combinedBudgetFrame["channelOutlayBucket"] = combinedBudgetFrame.apply(lambda x: getDealOutlayBucket(dbRateRules, x["channelTotalBudget"], x["channelName"],chnlList, year), axis=1)
    combinedBudgetFrame["channelOutlayIncentive"] = combinedBudgetFrame.apply(lambda x: getPrimaryDiscPct(dbRateRules, x["channelName"], x["channelOutlayBucket"], year), axis=1)
    # Change - apply network deal incentive when deal has channels <=4 - Poorva - 18-04-2020
    # morethanTwo key to be set as 0.0 and create new key networkIncentive when deal has channels <=4 - Poorva - 22-04-2020
    combinedBudgetFrame["moreThanTwo"] = 0.0
    # get network incentive
    #if lenNetworkChannels >= 4:
    # Changed network incentive from 4 to 3 channels - Poorva - 07-06-2021
    if lenNetworkChannels >= 3:
        print('network channels are 3 or more')
        ## Changed - picking all channels totalBudget instead of only network channels budget - Poorva - 30-07-2021
        #multiChOutlay = combinedBudgetFrame.loc[(combinedBudgetFrame['isNetwork'] == True), 'channelTotalBudget'].sum()
        multiChOutlay = combinedBudgetFrame['channelTotalBudget'].sum()
        print('multiChOutlay-->', multiChOutlay)
        combinedBudgetFrame["networkIncentive"] = combinedBudgetFrame.apply(
            lambda x: getMultiChIncentiveNewRule(lenNetworkChannels, dbParameters, multiChOutlay, x["isNetwork"]), axis=1)
    else:
        combinedBudgetFrame["networkIncentive"] = 0.0
    combinedBudgetFrame["multiChIncentive"] = discountPct
    combinedBudgetFrame["multiChFlag"] = True if discountPct!=0 else False
    combinedBudgetFrame['westIncentivePct'] = 0.0
    if totalBudget * digitalPctThreshold <= 200000:
        digitalCompareValueCust = totalBudget * digitalPctThreshold
    else:
        digitalCompareValueCust = 200000
    if budgetDigital >= digitalCompareValueCust and budgetDigital!=0:
        digitalIncentPct = digitalIncentConst
        digitalIncentInternalFlag = True
    else:
        digitalIncentPct = 0
        digitalIncentInternalFlag = False
    combinedBudgetFrame["digitalIncentPct"] = digitalIncentPct
    combinedBudgetFrame["digitalIncentInternalFlag"] = digitalIncentInternalFlag

    # Brand activation discount will no longer be applicable - Poorva - 18-04-2020
    combinedBudgetFrame['channelSpecificIncentive'] = 0.0
    activityBasedIncentiveChannels = []
    for i in range(len(combinedBudgetFrame)):
        if combinedBudgetFrame.iloc[i]['channelSpecificIncentive'] > 0.0:
            channelIncentiveObject = ChannelIncentive(combinedBudgetFrame.iloc[i]['channelName'],
                                                      combinedBudgetFrame.iloc[i]['channelSpecificIncentive'])
            activityBasedIncentiveChannels.append(channelIncentiveObject.__dict__)

    # ValueAdd condition changed - applied for single channel also basis its totalBudget - Poorva - 18-06-2020
    try:
        valueAddEntitlement = getValueAddEntitlement(totalBudget)[0]
    except:
        valueAddEntitlement = 0.0
    print('valueADD value -->', valueAddEntitlement)

    # EarlyBirdEntitlement - Poorva - 17-07-2021
    earlyBirdEntitlement = 0.0
    try:
        date_aug = datetime.date(2021, 8, 30)
        strDate = dealStartDate.date()
        print('type -', type(date_aug), type(strDate))
        print('EARLY BIRD ENTITLEMENT CHECK')
        print('date_aug, dealStartDate -->', date_aug, strDate)
        if strDate < date_aug :
            print('in if')
            for chn in range(0,len(combinedBudgetFrame)):
                print('loop num, channel and minFCT-->', chn, combinedBudgetFrame.loc[chn,'channelName'], combinedBudgetFrame.loc[chn,'minFCT'])
                if combinedBudgetFrame.loc[chn,'channelName'] == 'ABP NEWS' and combinedBudgetFrame.loc[chn,'minFCT'] >= 10000:
                    print('channel is ABP NEWS and minFCT is more than 10000')
                    earlyBirdEntitlement = getEarlyBirdAddEntitlement(totalBudget)[0]
    except:
        earlyBirdEntitlement = 0.0
    print('earlyBirdEntitlement value -->', earlyBirdEntitlement)

    # Added advertiserIncentive at channel level as per the new logic, keeping old one too - Poorva - 30-04-2020
    combinedBudgetFrame["overallIncentive"] = 1 - (
                (1 - combinedBudgetFrame["channelOutlayIncentive"]) * (1 - combinedBudgetFrame["multiChIncentive"]) *
                (1 - combinedBudgetFrame["digitalIncentPct"]) * (1 - combinedBudgetFrame["networkIncentive"]) * (1 - newAdvertiserIncent) *
                (1 - combinedBudgetFrame['channelSpecificIncentive']) * (1 - combinedBudgetFrame['advertiserIncentive']))

    print('*** Channel - Incentives ***')
    print(combinedBudgetFrame[['channelName', 'channelOutlayIncentive', 'networkIncentive', 'overallIncentive']])

    # get combo incentives
    print('*** Incentives - Combo level ***')
    comboIncFrame = combinedBudgetFrame[['comboName', 'networkIncentive','multiChIncentive']]
    comboIncFrame = comboIncFrame.drop_duplicates(inplace=False)
    prevComboDF['comboNewAdvInc'] = 0.0
    if len(comboDF)!= 0:
        comboDF_cropped = comboDF[['comboName', 'channelName', 'proportionTotalOutlay', 'comboMinFCT', 'comboNewAdvInc']]
    else:
        comboDF_cropped = pd.DataFrame(columns=['comboName', 'channelName', 'proportionTotalOutlay', 'comboMinFCT', 'comboNewAdvInc'])
    if len(prevComboDF) != 0 and includePrevDeal == True:
        prevComboDF_cropped = prevComboDF[['comboName', 'channelName', 'proportionTotalOutlay', 'comboMinFCT','comboNewAdvInc']]
    else:
        prevComboDF_cropped = pd.DataFrame(columns=['comboName', 'channelName', 'proportionTotalOutlay', 'comboMinFCT', 'comboNewAdvInc'])
    comboCombined = comboDF_cropped.append(prevComboDF_cropped)
    print('combo combinedd')
    print(comboCombined[['comboName', 'channelName', 'proportionTotalOutlay']])

    if len(comboCombined) != 0:
        if len(prevComboDF_cropped) == 0:
            comboCombined = comboCombined.groupby(['comboName', 'channelName']).agg({'proportionTotalOutlay': 'sum', 'comboMinFCT': 'sum', 'comboNewAdvInc': 'mean'}).reset_index()
        else:
            comboCombined = comboCombined.groupby(['comboName']).agg({'proportionTotalOutlay': 'sum', 'comboMinFCT': 'sum', 'comboNewAdvInc': 'mean'}).reset_index()
    combinedComboDF = pd.DataFrame()
    if len(prevComboDF_cropped) == 0:
        print('No prev combo')
        comboChannels = list(set(comboCombined.channelName.tolist()))
        comboCombined = comboCombined.set_index(['channelName'])
        if len(comboCombined) != 0:
            for channel in comboChannels:
                if comboCombined.proportionTotalOutlay.get(channel, 0) != 0:
                    combinedComboDF.loc[channel, 'channelName'] = channel
                    combinedComboDF.loc[channel, 'comboName'] = comboCombined.comboName.get(channel, 0)
                    combinedComboDF.loc[channel, 'comboTotalOutlay'] = comboCombined.proportionTotalOutlay.get(channel, 0)
                    combinedComboDF.loc[channel, 'comboMinFCT'] = comboCombined.comboMinFCT.get(channel, 0)
                    combinedComboDF.loc[channel, 'comboAdvertiserIncentive'] = comboCombined.comboNewAdvInc.get(channel,0)
    else:
        print('prev combo exists')
        comboChannels = list(set(comboCombined.comboName.tolist()))
        comboCombined = comboCombined.set_index(['comboName'])
        if len(comboCombined) != 0:
            for channel in comboChannels:
                if comboCombined.proportionTotalOutlay.get(channel, 0) != 0:
                    combinedComboDF.loc[channel, 'comboName'] = channel
                    combinedComboDF.loc[channel, 'comboTotalOutlay'] = comboCombined.proportionTotalOutlay.get(channel, 0)
                    combinedComboDF.loc[channel, 'comboMinFCT'] = comboCombined.comboMinFCT.get(channel, 0)
                    combinedComboDF.loc[channel, 'comboAdvertiserIncentive'] = comboCombined.comboNewAdvInc.get(channel,0)


    print('combinedComboDFFFF')
    print(combinedComboDF)
    # print(combinedComboDF[['comboName', 'comboTotalOutlay']])
    if len(combinedComboDF)!=0:
        comboTotalBudget = combinedComboDF.groupby(['comboName']).agg({'comboTotalOutlay': 'sum', 'comboMinFCT': 'sum', 'comboAdvertiserIncentive': 'mean'}).reset_index()
        comboTotalBudget["comboOutlayBucket"] = comboTotalBudget.apply(
            lambda x: getDealOutlayBucket(dbRateRules, x["comboTotalOutlay"], x["comboName"],chnlList, year), axis=1)
        comboTotalBudget["comboOutlayIncentive"] = comboTotalBudget.apply(lambda x: getPrimaryDiscPct(dbRateRules, x["comboName"], x["comboOutlayBucket"], year), axis=1)
        for i in range(0, len(comboTotalBudget)):
            try:
                comboTotalBudget.loc[i, 'networkIncentive'] = comboIncFrame[(comboIncFrame['comboName'] == comboTotalBudget['comboName'][i])].iloc[0]['networkIncentive']
            except:
                comboTotalBudget.loc[i, 'networkIncentive'] = 0.0
            try:
                comboTotalBudget.loc[i, 'multiChIncentive'] = comboIncFrame[(comboIncFrame['comboName'] == comboTotalBudget['comboName'][i])].iloc[0]['multiChIncentive']
            except:
                comboTotalBudget.loc[i, 'multiChIncentive'] = 0.0
        # except:
        #     comboTotalBudget = pd.DataFrame()
    else:
        comboTotalBudget = pd.DataFrame()

    combo_activityIncentive = combinedBudgetFrame[['comboName', 'channelSpecificIncentive','digitalIncentPct']]
    combo_activityIncentive = combo_activityIncentive.groupby(['comboName']).agg({'channelSpecificIncentive': 'mean', 'digitalIncentPct': 'mean'}).reset_index()
    for i in range(0, len(comboTotalBudget)):
        try:
            comboTotalBudget.loc[i, 'activityBasedIncentive'] = combo_activityIncentive[(combo_activityIncentive['comboName'] == comboTotalBudget['comboName'][i])].iloc[0]['channelSpecificIncentive']
            comboTotalBudget.loc[i, 'digitalIncentive'] = combo_activityIncentive[(combo_activityIncentive['comboName'] == comboTotalBudget['comboName'][i])].iloc[0]['digitalIncentPct']
        except:
            comboTotalBudget.loc[i, 'activityBasedIncentive'] = 0.0
            comboTotalBudget.loc[i, 'digitalIncentive'] = 0.0

    # Added advertiserIncentive at combo level as per the new logic, keeping old one too - Poorva - 30-04-2020
    if len(comboTotalBudget) != 0:
        comboTotalBudget['overallIncentive'] = 1 - ((1 - comboTotalBudget['comboOutlayIncentive']) * (1 - comboTotalBudget["multiChIncentive"]) *
                                                    (1 - comboTotalBudget['networkIncentive']) * (1 - comboTotalBudget['activityBasedIncentive']) *
                                                    (1 - newAdvertiserIncent) * (1 - comboTotalBudget['digitalIncentive']) * (1 - comboTotalBudget['comboAdvertiserIncentive']))

        print('*** Combo - Incentives ***')
        print(comboTotalBudget[['comboName', 'comboOutlayIncentive', 'comboAdvertiserIncentive']])

    # Other category deals
    if briefCategoryName in ["EDUCATION","ALLIANCE","PRACHAR","AD WISER","IPO"]:
        print('*** Deal of category --> ', briefCategoryName)
        for n in range(0,len(allChannels)):
            updateSuccessflag = dbBrief.update({'dealId': dealId}, {'$set': {
                                                   'budgetIncentive.channel.' + str(n) + '.name': allChannels[n],
                                                   'budgetIncentive.channel.' + str(n) + '.outlayIncentive': 0.0,
                                                   'budgetIncentive.primaryChannel': primaryChannel,
                                                   'budgetIncentive.totalChannelCount': totalChannelCount,
                                                   'budgetIncentive.regionalChannelCount': regionalChannelCount,
                                                   'budgetIncentive.dealPeriodBucket': dealPeriodBucket,
                                                   'budgetIncentive.dealOutlayBucket': dealOutlayBucket,
                                                   'budgetIncentive.multiChIncentive': 0.0,
                                                   'budgetIncentive.multiChannelIncentiveFlag': False,
                                                   'budgetIncentive.digitalIncentive': 0.0,
                                                   'budgetIncentive.newAdvertiserIncentFlag': False,
                                                   'budgetIncentive.newAdvertiserIncentive': 0.0,
                                                   'budgetIncentive.digitalIncentInternalFlag': False,
                                                   'budgetIncentive.valueAddEntitlement': 0.0,
                                                   'budgetIncentive.earlyBirdEntitlement': 0.0,
                                                   'budgetIncentive.totalBudget': totalBudget}})
        return jsonify({"status":"success"})

    # Retail Category deal
    retailPackageDf = pd.DataFrame()
    if briefCategoryName == 'RETAIL':
        print('*** Deal of category --> ', briefCategoryName)
        for ch in range(0,len(brief['channel'])):
            channel_name = brief['channel'][ch]['name']
            dealStartDateYear = dealStartDate.year
            dealStartDateMonth = dealStartDate.month
            briefRetailPackage = brief['channel'][ch]['budget']['retail']
            if briefRetailPackage != 0:
                retail = dbRetail.find_one({'channelName': channel_name})
                for package in range(0,len(retail['packages'])):
                    if (briefRetailPackage >= retail['packages'][package]['startPackageValue']) and (briefRetailPackage <= retail['packages'][package]['endPackageValue']):
                        for inc in range(0,len(retail['packages'][package]['incentive'])):
                            if (dealStartDateYear == retail['packages'][package]['incentive'][inc]['year']) and \
                                    (dealStartDateMonth >= retail['packages'][package]['incentive'][inc]['startMonth']) and \
                                    (dealStartDateMonth <= retail['packages'][package]['incentive'][inc]['endMonth']):
                                retailIncentive = retail['packages'][package]['incentive'][inc]['pctIncentive']
                                ticker = retail['packages'][package]['incentive'][inc]['ticker']
            else:
                retailIncentive = 0
                ticker = 0
            retailPackageDf.loc[ch,'channelName'] = channel_name
            retailPackageDf.loc[ch,'retailIncentive'] = retailIncentive
            retailPackageDf.loc[ch,'ticker'] = ticker
    channelRetailIncentive = []
    for i in range(0,len(retailPackageDf)):
        retailIncentiveObject = RetailIncentive(retailPackageDf.loc[i,'channelName'], float(retailPackageDf.loc[i,'retailIncentive']), retailPackageDf.loc[i,'ticker'])
        channelRetailIncentive.append(retailIncentiveObject.__dict__)

    print('*** Update incentives in brief - budgetIncentive ***')
    updateSuccessflag = dbBrief.update({'dealId': dealId},{'$set': {'budgetIncentive.primaryChannel':primaryChannel,
                                                                    'budgetIncentive.totalChannelCount':totalChannelCount,
                                                                    'budgetIncentive.regionalChannelCount':regionalChannelCount,
                                                                    'budgetIncentive.dealPeriodBucket':dealPeriodBucket,
                                                                    'budgetIncentive.dealOutlayBucket':dealOutlayBucket,
                                                                    'budgetIncentive.multiChIncentive':discountPct,
                                                                    'budgetIncentive.multiChannelIncentiveFlag':multiChannelFlag,
                                                                    'budgetIncentive.digitalIncentive':digitalIncentPct,
                                                                    'budgetIncentive.newAdvertiserIncentFlag': newAdvertiserFlag,
                                                                    'budgetIncentive.newAdvertiserIncentive':newAdvertiserIncent,
                                                                    'budgetIncentive.digitalIncentInternalFlag': digitalIncentInternalFlag,
                                                                    'budgetIncentive.regionalChannelForIncentiveCnt': cntRegionalChannelforDiscount,
                                                                    'budgetIncentive.valueAddEntitlement': valueAddEntitlement,
                                                                    'budgetIncentive.earlyBirdEntitlement': earlyBirdEntitlement,
                                                                    'budgetIncentive.activityBasedIncentiveChannels': activityBasedIncentiveChannels,
                                                                    'budgetIncentive.retailIncentive': channelRetailIncentive,
                                                                    'budgetIncentive.totalBudget':totalBudget,
                                                                    'budgetIncentive.channel':[]}})

    if len(comboTotalBudget) != 0:
        print('*** Update combo incentives in brief - budgetIncentive - combo ***')
        comboTotalBudget.rename(columns={"comboName": "name", "comboTotalOutlay":"totalBudget","comboMinFCT":"minFCT",
                                     "comboOutlayBucket":"dealOutlayBucket","comboOutlayIncentive":"outlayIncentive",
                                         "comboAdvertiserIncentive":"advertiserIncentive"}, inplace=True)
        dbBrief.update({"dealId": dealId}, {'$set': {"budgetIncentive.combo": list(comboTotalBudget.T.to_dict().values())}})

    print('*** Update incentives at channel level in brief - budgetIncetive - channel ***')
    channelList = combinedBudgetFrame["channelName"].tolist()
    combinedBudgetFrame = combinedBudgetFrame.set_index('channelName')
    if updateFlag == 'Y':
        for i in range(len(channelList)):
             dbBrief.update({"dealId": dealId},
                               {'$set': { "budgetIncentive.channel."+str(i)+".name":channelList[i],
                                          "budgetIncentive.channel."+str(i)+".dealOutlayBucket" : str(combinedBudgetFrame.channelOutlayBucket.get(channelList[i],0.0)),
                                          "budgetIncentive.channel."+str(i) + ".dealPeriodBucket" : dealPeriodBucket,
                                          "budgetIncentive.channel." + str(i) + ".outlayIncentive": float(combinedBudgetFrame.channelOutlayIncentive.get(channelList[i], 0.0)),
                                          "budgetIncentive.channel." + str(i) + ".moreThanTwo": float(combinedBudgetFrame.moreThanTwo.get(channelList[i], 0.0)),
                                          "budgetIncentive.channel." + str(i) + ".networkIncentive": float(combinedBudgetFrame.networkIncentive.get(channelList[i], 0.0)),
                                          "budgetIncentive.channel." + str(i) + ".overallIncentive": float(combinedBudgetFrame.overallIncentive.get(channelList[i], 0.0)),
                                          "budgetIncentive.channel." + str(i) + ".advertiserIncentive": float(combinedBudgetFrame.advertiserIncentive.get(channelList[i], 0.0)),
                                          "budgetIncentive.channel." + str(i) + ".totalBudget": float(combinedBudgetFrame.totalBudget.get(channelList[i], 0.0)),
                                          "budgetIncentive.channel." + str(i) + ".isHighPT": bool(combinedBudgetFrame.highPT.get(channelList[i], False))
                                        }})

             updateGridSuccessflag = dbGrid.update({'dealId': brief['dealId'], 'channel.name': channelList[i]},
                                                   {'$set': {'channel.$.channelIncentive': float(combinedBudgetFrame.loc[channelList[i], 'channelSpecificIncentive']),
                                                             'channel.$.outlayIncentive': float(combinedBudgetFrame.channelOutlayIncentive.get(channelList[i], 0.0)),
                                                             'channel.$.overallIncentive': float(combinedBudgetFrame.overallIncentive.get(channelList[i], 0.0)),
                                                             'channel.$.isHighPT': bool(combinedBudgetFrame.highPT.get(channelList[i], False)),
                                                             'channel.$.moreThanTwo': float(combinedBudgetFrame.moreThanTwo.get(channelList[i],0.0)),
                                                             'channel.$.networkIncentive': float(combinedBudgetFrame.networkIncentive.get(channelList[i], 0.0)),
                                                             'channel.$.advertiserIncentive': float(combinedBudgetFrame.advertiserIncentive.get(channelList[i], 0.0))
                                                            }})
    print('*** Success - Discounts updated in database ***')
    return jsonify({"status":"success"})

def getValueAddEntitlement(totalBudget):
    print('in valueAdd ---- total budget --', totalBudget)
    # Change in valueAdd logic - Poorva - 16-06-2021
    if 5000000 <= totalBudget < 10000000:
        valueAddEntitlement = totalBudget * 0.01
    elif 10000000 <= totalBudget < 15000000:
        valueAddEntitlement = totalBudget * 0.02
    elif 15000000 <= totalBudget < 20000000:
        valueAddEntitlement = totalBudget * 0.025
    elif totalBudget >= 20000000:
        valueAddEntitlement = totalBudget * 0.03
    else:
        valueAddEntitlement = 0.0
    print('valueAddEntitlement-->', valueAddEntitlement)
    return [valueAddEntitlement]

def getEarlyBirdAddEntitlement(totalBudget):
    print('in earlyBird logic ---- total budget --', totalBudget)
    if 10000000 <= totalBudget < 20000000:
        earlyBirdEntitlement = totalBudget * 0.02
    elif 20000000 <= totalBudget < 30000000:
        earlyBirdEntitlement = totalBudget * 0.025
    elif totalBudget >= 30000000:
        earlyBirdEntitlement = totalBudget * 0.03
    else:
        earlyBirdEntitlement = 0.0
    print('earlyBirdEntitlement-->', earlyBirdEntitlement)
    return [earlyBirdEntitlement]

# def getValueAddEntitlement(totalBudget):
#     # Change in valueAdd logic - Poorva - 18-04-2020
#     if 10000000 <= totalBudget < 20000000:
#         valueAddEntitlement = totalBudget * 0.02
#     elif 20000000 <= totalBudget < 30000000:
#         valueAddEntitlement = totalBudget * 0.03
#     elif totalBudget >= 30000000:
#         valueAddEntitlement = totalBudget * 0.04
#     else:
#         valueAddEntitlement = 0.0
#     return [valueAddEntitlement]

# changes made for implemeting new combo logic
def createBudgetFrame(brief,isNewCombo,comboDF,newAdvertiserIncentConst):
    print('in create budget frame')
    budgetFrame = pd.DataFrame()
    channelFrame = pd.DataFrame(list(brief['channel']))
    for i in range(len(channelFrame)):
        channelName = channelFrame.iloc[i]['name']
        budgetFrame.loc[channelName,'channelName'] = channelFrame.iloc[i]['name']
        budgetFrame.loc[channelName,'channelSpotBuy'] =  channelFrame.iloc[i]['budget']['spotBuy']
        budgetFrame.loc[channelName,'sponsorship'] = channelFrame.iloc[i]['budget']['sponsorship']
        try:
            budgetFrame.loc[channelName,'spotLight'] = channelFrame.iloc[i]['budget']['spotLight']
            budgetFrame.loc[channelName,'nonFCTMonthly'] = channelFrame.iloc[i]['budget']['nonFCTMonthly']
            budgetFrame.loc[channelName,'nonFCTExposure'] = channelFrame.iloc[i]['budget']['nonFCTExposure']
            budgetFrame.loc[channelName, 'channelMinFCT'] = channelFrame.iloc[i]['budget']['minFCT']
        except:
            budgetFrame.loc[channelName, 'spotLight'] = 0
            budgetFrame.loc[channelName, 'nonFCTMonthly'] = 0
            budgetFrame.loc[channelName, 'nonFCTExposure'] = 0
            budgetFrame.loc[channelName, 'channelMinFCT'] = 0
        try:
            budgetFrame.loc[channelName, 'highPT'] = channelFrame.iloc[i]['budget']['highPT']
        except:
            budgetFrame.loc[channelName, 'highPT'] = False
        # Added newAdvertiserIncentive logic at channel level - Poorva - 30-04-2020
        try:
            budgetFrame.loc[channelName, 'channelNewAdvIncFlag'] = channelFrame.iloc[i]['budget']['isNewAdvertiserIncentive']
        except:
            budgetFrame.loc[channelName, 'channelNewAdvIncFlag'] = False
        if budgetFrame.loc[channelName, 'channelNewAdvIncFlag'] == True:
            budgetFrame.loc[channelName, 'channelNewAdvInc'] = newAdvertiserIncentConst
        else:
            budgetFrame.loc[channelName, 'channelNewAdvInc'] = 0.0
        try:
            budgetFrame.loc[channelName, 'retail'] = channelFrame.iloc[i]['budget']['retail']
        except:
            budgetFrame.loc[channelName, 'retail']=0

        budgetFrame.loc[channelName,'channelTotalOutlay'] = channelFrame.iloc[i]['budget']['totalOutlay']
    print(budgetFrame[['channelName','channelSpotBuy','channelTotalOutlay']])
    if isNewCombo:
        comboPropDF = comboDF.groupby(['channelName']).agg({'proportionTotalOutlay':'sum','proportionSpotBuy':'sum','comboMinFCT':'sum','comboNewAdvInc':'sum'}).reset_index()#.set_index(['channelName'])
        print('000000000')
        print(comboPropDF)
        budgetFrame = budgetFrame.reset_index()
        budgetFrame = budgetFrame.merge(comboPropDF, how='left', on=['channelName']).set_index(['channelName']).fillna(0)
        print('0000011111111111')
        print(budgetFrame)
        budgetFrame['spotBuy'] = budgetFrame['channelSpotBuy'] + budgetFrame['proportionSpotBuy']
        budgetFrame['totalOutlay'] = budgetFrame['channelTotalOutlay'] + budgetFrame['proportionTotalOutlay']
        budgetFrame['minFCT'] = budgetFrame['channelMinFCT'] + budgetFrame['comboMinFCT']
    else:
        budgetFrame['spotBuy'] = budgetFrame['channelSpotBuy']
        budgetFrame['totalOutlay'] = budgetFrame['channelTotalOutlay']
        budgetFrame['minFCT'] = budgetFrame['channelMinFCT']
        budgetFrame['comboNewAdvInc'] = 0.0
    print('000000022222222')
    print(budgetFrame)
    # Changes for adding advertiserIncentive at channel level - Poorva - 30-04-2020
    for i in range(len(budgetFrame)):
        channelName = channelFrame.iloc[i]['name']
        if budgetFrame.loc[channelName,'channelNewAdvInc'] != 0 or budgetFrame.loc[channelName,'comboNewAdvInc'] != 0:
            budgetFrame.loc[channelName,'advertiserIncentive'] = newAdvertiserIncentConst
        else:
            budgetFrame.loc[channelName,'advertiserIncentive'] = 0.0
    return budgetFrame

def createPrevBudgetFrame(brief,includePrevDeal,isPrevDealNewCombo,prevComboDF):
    print('in prev Budget, prev flag-->', includePrevDeal)
    budgetFrame = pd.DataFrame(columns=['channelName','spotBuy','sponsorship','spotLight','nonFCTMonthly','nonFCTExposure','minFCT','totalOutlay'])
    if includePrevDeal == True:
        channelFrame = pd.DataFrame(list(brief['budgetPrevDeal']['channel']))
        for i in range(len(channelFrame)):
            try:
                channelName = channelFrame.iloc[i]['name']
            except:
                break
            budgetFrame.loc[channelName,'channelName'] = channelFrame.iloc[i]['name']
            budgetFrame.loc[channelName,'spotBuy'] =  channelFrame.iloc[i]['budgetEdited']['spotBuy']
            budgetFrame.loc[channelName,'sponsorship'] = channelFrame.iloc[i]['budgetEdited']['sponsorship']
            try:
                budgetFrame.loc[channelName,'spotLight'] = channelFrame.iloc[i]['budgetEdited']['spotLight']
                budgetFrame.loc[channelName,'nonFCTMonthly'] = channelFrame.iloc[i]['budgetEdited']['nonFCTMonthly']
                budgetFrame.loc[channelName,'nonFCTExposure'] = channelFrame.iloc[i]['budgetEdited']['nonFCTExposure']
                budgetFrame.loc[channelName, 'minFCT'] = channelFrame.iloc[i]['budgetEdited']['minFCT']
            except:
                budgetFrame.loc[channelName, 'spotLight'] = 0
                budgetFrame.loc[channelName, 'nonFCTMonthly'] = 0
                budgetFrame.loc[channelName, 'nonFCTExposure'] = 0
                budgetFrame.loc[channelName, 'minFCT'] = 0
            try:
                budgetFrame.loc[channelName, 'retail'] = channelFrame.iloc["budget"]["retail"]
            except:
                budgetFrame.loc[channelName, 'retail'] = 0
            budgetFrame.loc[channelName,'totalOutlay'] = channelFrame.iloc[i]['budgetEdited']['totalOutlay']
        print('111111')
        print(budgetFrame[['channelName', 'spotBuy', 'totalOutlay']])
        if isPrevDealNewCombo:
            comboPropDF = prevComboDF.groupby(['channelName']).agg({'proportionTotalOutlay': 'sum', 'proportionSpotBuy': 'sum',
                                            'comboMinFCT': 'sum'}).reset_index()  # .set_index(['channelName'])
            print('000000000')
            print(comboPropDF)
            budgetFrame = budgetFrame.reset_index()
            budgetFrame = budgetFrame.merge(comboPropDF, how='left', on=['channelName']).set_index(
                ['channelName']).fillna(0)
            print('0000011111111111')
            print(budgetFrame)
            budgetFrame['spotBuy'] = budgetFrame['spotBuy'] + budgetFrame['proportionSpotBuy']
            budgetFrame['totalOutlay'] = budgetFrame['totalOutlay'] + budgetFrame['proportionTotalOutlay']
            budgetFrame['minFCT'] = budgetFrame['minFCT'] + budgetFrame['comboMinFCT']
        else:
            budgetFrame['spotBuy'] = budgetFrame['spotBuy']
            budgetFrame['totalOutlay'] = budgetFrame['totalOutlay']
            budgetFrame['minFCT'] = budgetFrame['minFCT']
    else:
        pass
    print('000000022222222')
    print(budgetFrame)
    return budgetFrame

def getPrimaryDiscPct(dbRateRules,primaryChannel,dealOutlayBucket,year):
    try:
        rateRec = dbRateRules.find_one({'channel':primaryChannel,'year':int(year)})
        rateIncentiveFrame = pd.DataFrame(list(rateRec['outlayIncentive']))
        rateIncentiveFrame = rateIncentiveFrame.set_index(['outlay', 'month'])
        # Changed logic, dealPeriodBucket not required - Poorva 19-04-2020
        # Hard coding period bucket for older deals, picking discount rate for periodBucket = 12 - Poorva - 24-07-2020
        discRate = rateIncentiveFrame.loc[dealOutlayBucket, 12]['discount']
    except:
        discRate = 0.0
    return discRate

def getDealOutlayBucket(dbRateRules,totalBudget, primaryChannel,chnlList,year):
    print('in outlay bucket - main')
    regionalChannelList=chnlList['regionalChannelList']
    precedenceChannelList=chnlList['precedenceChannelList']
    print('primaryChannel,year-->', primaryChannel,year)
    outlayBuckets = getOutlayBuckets(dbRateRules,primaryChannel,year)
    #print('outlayBuckets --', outlayBuckets)
    bucket = ''
    for range in outlayBuckets:
            if range.minOutlay <= totalBudget <range.maxOutlay:
                bucket = range.bucket
    # removed - Poorva - 16-07-2021
    # if primaryChannel in regionalChannelList and (bucket=='200+' or bucket=='150+') :
    #     bucket = '100+'
    # if primaryChannel in precedenceChannelList and bucket=='10+':
    #     bucket = '0+'
    print('bucket-->', bucket)
    return bucket

def getDealPeriodBucket(dealStartDate,dealEndDate):
    monthcounter = 0
    iterdate = dealStartDate
    while iterdate < dealEndDate:
        monthcounter = monthcounter + 1
        iterdate = dealStartDate + relativedelta.relativedelta(months=(1*monthcounter)) - relativedelta.relativedelta(days=1)
    diffmonths = monthcounter
    bucket = getBucketMonths(diffmonths)
    return bucket

def getBucketMonths(diffmonths):
    if diffmonths <=3:
        bucket = 3
    else:
        bucket = 12
    return bucket

def getOutlayBuckets(dbRateRules,channelName,year):
    print('in get outlay buckets --channel--', channelName,year)
    bucketDef = dbRateRules.find_one({"channel":channelName, "year":int(year)})["bucketDef"]
    outlayBuckets=[]
    for i in bucketDef:
        outlayBuckets.append(OutlayRange(i["minOutlay"],i["maxOutlay"],i["bucket"]))
    return outlayBuckets

class RetailIncentive:
    def __init__(self,name,pctIncentive,ticker):
        self.name = name
        self.pctIncentive = pctIncentive
        self.ticker = ticker

class OutlayRange:
    def __init__(self,minOutlay,maxOutlay,bucket):
        self.minOutlay = minOutlay
        self.maxOutlay = maxOutlay
        self.bucket = bucket

class ChannelIncentive:
    def __init__(self,name,incentivePct):
        self.name = name
        self.incentivePct = incentivePct

def getKeyParameters(prevBudgetFrame,budgetFrame,budgetDigital,prevBudgetDigital, dbNetworkRules,comboDF,prevComboDF,chnlList):
    print('in key parameters function')
    precedenceChannelList = chnlList['precedenceChannelList']
    regionalChannelList = chnlList['regionalChannelList']
    priorityListChannels= chnlList ['priorityListChannels']
    prevBudgetFrame = prevBudgetFrame.loc[prevBudgetFrame['totalOutlay']>=  0]
    allChannels = list(set(prevBudgetFrame.index.tolist() + budgetFrame.index.tolist()))
    print('all channels-->', allChannels)
    spotLightThresholdPct = chnlList['spotLightThresholdPct']
    spotLightIncentiveConst = chnlList['spotLightThresholdPct']
    budgetFrame = budgetFrame.fillna(0)
    combinedBudgetFrame = pd.DataFrame(columns=['channelName', 'spotBuy', 'sponsorship', 'spotLight', 'nonFCTMonthly', 'nonFCTExposure', 'minFCT',
                 'totalOutlay','fctPCT', 'spotLightDeal', 'spotLightPct', 'nonFCTExposureDealPct', 'spotLightIncentive',
                 'nonFCTIncentivePct', 'spotLightIncentivePct', 'digitalIncentivePct','highPT','advertiserIncentive'])
    # Initial Addition
    for channel in allChannels:
        combinedBudgetFrame.loc[channel, 'channelName'] = channel
        combinedBudgetFrame.loc[channel, 'spotBuy'] = budgetFrame.spotBuy.get(channel, 0) + prevBudgetFrame.spotBuy.get(channel, 0)
        combinedBudgetFrame.loc[channel, 'sponsorship'] = budgetFrame.sponsorship.get(channel,0) + prevBudgetFrame.sponsorship.get(channel, 0)
        combinedBudgetFrame.loc[channel, 'spotLight'] = budgetFrame.spotLight.get(channel,0) + prevBudgetFrame.spotLight.get(channel, 0)
        combinedBudgetFrame.loc[channel, 'nonFCTMonthly'] = budgetFrame.nonFCTMonthly.get(channel,0) + prevBudgetFrame.nonFCTMonthly.get(channel, 0)
        combinedBudgetFrame.loc[channel, 'nonFCTExposure'] = budgetFrame.nonFCTExposure.get(channel,0) + prevBudgetFrame.nonFCTExposure.get(channel, 0)
        combinedBudgetFrame.loc[channel, 'minFCT'] = budgetFrame.minFCT.get(channel, 0) + prevBudgetFrame.minFCT.get(channel, 0)
        combinedBudgetFrame.loc[channel, 'totalOutlay'] = budgetFrame.totalOutlay.get(channel,0) + prevBudgetFrame.totalOutlay.get(channel, 0)
        combinedBudgetFrame.loc[channel, 'spotLightDeal'] = budgetFrame.spotLight.get(channel, 0)
        combinedBudgetFrame.loc[channel, "isNetwork"] = True if combinedBudgetFrame.loc[channel,"minFCT"] >= 5000 else False
        try:
            combinedBudgetFrame.loc[channel, 'spotLightDealPct'] = budgetFrame.spotLight.get(channel, 0) / budgetFrame.totalOutlay.get(channel, 0)
            if combinedBudgetFrame.loc[channel, 'spotLightDealPct'] >= spotLightThresholdPct:
                combinedBudgetFrame.loc[channel, 'spotLightIncentivePctspotLightIncentivePct'] = spotLightIncentiveConst
            combinedBudgetFrame.loc[channel, 'nonFCTExposureDealPct'] = budgetFrame.nonFCTExposure.get(channel,0) / budgetFrame.totalOutlay.get(channel, 0)
        except:
            combinedBudgetFrame.loc[channel, 'spotLightDealPct'] = 0.0
            combinedBudgetFrame.loc[channel, 'spotLightIncentivePct'] = 0.0
            combinedBudgetFrame.loc[channel, 'nonFCTExposureDealPct'] = 0.0
        combinedBudgetFrame.loc[channel,"highPT"] = budgetFrame.highPT.get(channel,False)
        # Added advertiserIncentive logic at chanel level - Poorva - 30-04-2020
        combinedBudgetFrame.loc[channel,"advertiserIncentive"] = budgetFrame.advertiserIncentive.get(channel,0.0)
    print('44444444444444444444')
    print(combinedBudgetFrame[['channelName','spotBuy','totalOutlay']])

    allNetworkChannels = combinedBudgetFrame[combinedBudgetFrame["isNetwork"]==True].index.tolist()
    lenNetworkChannels = len(allNetworkChannels)
    for channel in priorityListChannels:
        if channel in allChannels:
            primaryChannelOld = channel
            break

    """add new logic for primary channel from database"""
    networkRuleDF = pd.DataFrame(list(dbNetworkRules.find({"channel": {"$all": allNetworkChannels, "$size": lenNetworkChannels}})))
    if len(networkRuleDF) == 0:
        primaryChannel = primaryChannelOld
        allNetworkChannels = []
    else:
        allNetworkChannels = networkRuleDF.iloc[0]["channel"]
        primaryChannel = networkRuleDF.iloc[0]["channel"][0]

    discountPct = 0.0
    regionalChannelCount = len(list(set(regionalChannelList) & set(allNetworkChannels)))
    combinedBudgetFrame.loc[channel, 'nonFCTIncentivePct'] = 0.0
    if primaryChannel in precedenceChannelList:
        primaryChannelFCT = combinedBudgetFrame.loc[primaryChannel, 'minFCT']
        combinedBudgetFrame['fctPCT'] = combinedBudgetFrame['minFCT'].apply(lambda x: x / primaryChannelFCT)
    else:
        combinedBudgetFrame['fctPCT'] = 0.0

    combinedBudgetFrame = combinedBudgetFrame.fillna(0)
    totalChannelCount = len(allChannels)
    budgetFrame = budgetFrame.fillna(0)
    totalDealBudgetSpotlight = budgetFrame["spotLight"].sum()
    totalBudget = combinedBudgetFrame["totalOutlay"].sum()+budgetDigital+prevBudgetDigital
    combinedBudgetFrame["groupCol"] = combinedBudgetFrame.apply(lambda x: "network" if x["isNetwork"] == True else x["channelName"],axis=1)
    combinedBudgetFrame["totalBudget"] = combinedBudgetFrame["totalOutlay"]
    combinedBudgetFrame = combinedBudgetFrame.reset_index()
    combinedBudgetFrame = pd.merge(combinedBudgetFrame,comboDF,on="channelName",how="left")
    print('?????????????????????')
    print(combinedBudgetFrame)
    combinedBudgetFrame.fillna(0,inplace=True)
    spotLightPct = totalDealBudgetSpotlight / (budgetFrame['totalOutlay'].sum() + budgetDigital)
    budgetFrame['channelPct'] = 0.0

    if len(combinedBudgetFrame)== 0:
        primaryChannel = 'ABP NEWS'
        totalChannelCount = 1
        regionalChannelCount = 0
        totalBudget = 1
        channel = 'ABP NEWS'
        combinedBudgetFrame.loc[channel, 'channelName'] = channel
        combinedBudgetFrame.loc[channel, 'spotBuy'] = 0.0
        combinedBudgetFrame.loc[channel, 'sponsorship'] = 0.0
        combinedBudgetFrame.loc[channel, 'spotLight'] = 0.0
        combinedBudgetFrame.loc[channel, 'nonFCTMonthly'] = 0.0
        combinedBudgetFrame.loc[channel, 'nonFCTExposure'] = 0.0
        combinedBudgetFrame.loc[channel, 'minFCT'] = 0.0
        combinedBudgetFrame.loc[channel, 'totalOutlay'] = 0.0
        combinedBudgetFrame.loc[channel, 'spotLightDeal'] = 0.0
        combinedBudgetFrame.loc[channel, 'spotLightDealPct'] = 0.0
        combinedBudgetFrame.loc[channel, 'nonFCTExposureDealPct'] = 0.0
        combinedBudgetFrame.loc[channel, 'nonFCTExposureDealPct'] = 0.0
        combinedBudgetFrame.loc[channel, 'spotLightIncentivePct'] = 0.0
        combinedBudgetFrame.loc[channel, 'nonFCTIncentivePct'] = 0.0
        combinedBudgetFrame.loc[channel, 'advertiserIncentive'] = 0.0
        spotLightPct =0.0

    return allChannels,allNetworkChannels,lenNetworkChannels, primaryChannel, totalChannelCount,regionalChannelCount,\
            totalBudget, combinedBudgetFrame, spotLightPct, discountPct

def getMultiChIncentiveNewRule(lenNetworkChannels,dbParameters,multiChOutlay,isNetwork):
    # filtering on num of channels - Poorva - 07-06-2021
    print('in multi channel incentive --> channels --', lenNetworkChannels)
    print('multiChOutlay-->', multiChOutlay)
    rateCardRuleRec = dbParameters.find_one({'channelList':lenNetworkChannels})
    multiChIncentiveRec = pd.DataFrame(rateCardRuleRec["multiChannel"])
    print('multiChIncentiveRec')
    print(multiChIncentiveRec)
    if isNetwork:
        try:
            cond0 = multiChIncentiveRec.outlayFrom<=multiChOutlay
            cond1 = multiChIncentiveRec.outlayTo > multiChOutlay
            multiChIncentiveDF = multiChIncentiveRec[cond0 & cond1]
            print('multiChIncentiveDF')
            print(multiChIncentiveDF)
            # Removed dealPeriod condition - Poorva - 25-04-2020
            #cond2 = multiChIncentveRec.month==dealPeriod
            #multiChIncentiveDF = multiChIncentveRec[cond0 & cond1 & cond2]
            outlayMultiChIncent = multiChIncentiveDF.iloc[0]["discount"]
            print('outlayMultiChIncent-->', outlayMultiChIncent)
        except:
            outlayMultiChIncent=0.0
    else:
        outlayMultiChIncent=0.0
    print('multi ch inventive -->', outlayMultiChIncent)
    return outlayMultiChIncent

def getFY(startDate):
    if startDate.month >= 3:
        fy = str(startDate.year)+"-"+str(startDate.year+1)[-2:]
    else:
        fy = str(startDate.year-1)+"-"+str(startDate.year)[-2:]
    year = fy.split("-")[0]
    return fy, year

if __name__ == '__main__':
    app.run(debug=True)
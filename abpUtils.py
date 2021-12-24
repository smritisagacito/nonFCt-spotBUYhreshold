import numpy as np
from datetime import *
import pytz
import pandas as pd
import re
import math
from dateutil import relativedelta

# import pymongo
utc = pytz.UTC


def timeslottedarray(starttime, endtime):
    weekhrarray = np.zeros(52)
    startimeindex = timeslotindex(starttime)
    endtimeindex = timeslotindex(endtime)
    for j_time in range(0, 52):
        if (j_time >= startimeindex and j_time < endtimeindex):
            weekhrarray[j_time] = 1
        else:
            weekhrarray[j_time] = 0
    return weekhrarray


def timeslotindex(timesent):
    timehr = int(timesent.split(':')[0])
    timeminute = int(timesent.split(':')[1])
    # if (timeminute == 30):
    """big change  for rounding off timebands like 07:45, 07:15 etc"""
    if (timeminute > 0 and timeminute <= 30):
        halfhr = 1
        timehr = timehr
    elif (timeminute > 30 and timeminute <= 59):
        halfhr = 0
        timehr = timehr + 1
    else:
        halfhr = 0
        timehr = timehr

    return (timehr * 2 + halfhr)


def getspotPremiumFrame(premiumRec, channelName):
    premiumFrame = pd.DataFrame(list(premiumRec['premium']))
    premiumFrame = premiumFrame.set_index('spot')
    return premiumFrame


def getRecoBasedOnThresholds(db,recoNoGR, previousPrice, rateCardPrice, virtualPreviousPrice, prevDealIndex, channel, region):
    apiMstrVar=db.dsBasePriceAllApiMstr.find_one()
    print('####recoNoGR**', recoNoGR, 'previousPrice:', previousPrice, 'rateCardPrice', rateCardPrice)
    recoGuardrail = db.dsRecoGuardrail.find_one({'region': region, 'channel': channel})
    print(recoGuardrail)
    rateCardLowerThresholdPctConst = 1 - recoGuardrail['index']
    previousPriceThresholdPctConst = apiMstrVar['previousPriceThresholdPctConst']

    # Old constants
    #rateCardLowerThresholdPctConst = apiMstrVar['rateCardLowerThresholdPctConst']
    #previousIndexThresholdPctConst = apiMstrVar['previousIndexThresholdPctConst']
    # previousPriceThresholdPctConst = 1.20
    # rateCardLowerThresholdPctConst = (1 - 0.16)
    # previousIndexThresholdPctConst = (1 - 0.16)

    if previousPrice is None:
        if virtualPreviousPrice is not None:
            previousPrice = virtualPreviousPrice
    if prevDealIndex == 10000:
        if previousPrice is None or previousPrice == 0:
            upperBound = math.floor(rateCardPrice * 0.95)
            lowerBound = max(recoNoGR, rateCardPrice * rateCardLowerThresholdPctConst)
        else:
            upperBound = min(math.floor(rateCardPrice * 0.95), previousPrice * previousPriceThresholdPctConst)
            lowerBound = max(recoNoGR, previousPrice, rateCardPrice * rateCardLowerThresholdPctConst)
    else:
        if previousPrice is None or previousPrice == 0:
            upperBound = math.floor(rateCardPrice * 0.95)
            lowerBound = max(recoNoGR, rateCardPrice * rateCardLowerThresholdPctConst)
        else:
            upperBound = min(math.floor(rateCardPrice * 0.95), previousPrice * previousPriceThresholdPctConst)
            lowerBound = max(recoNoGR, previousPrice, rateCardPrice * rateCardLowerThresholdPctConst)

    print('****upper bound**', upperBound)
    if recoNoGR > upperBound:
        recoNoGR = upperBound
    else:
        pass

    if recoNoGR < lowerBound:
        recoNoGR = lowerBound

    if upperBound < lowerBound:
        recoNoGR = upperBound

    if recoNoGR <= rateCardPrice * rateCardLowerThresholdPctConst:
        recoNoGR = rateCardPrice * rateCardLowerThresholdPctConst

    print('****ulower bound**', lowerBound)
    print('***Reco Price after guardrail**', recoNoGR)

    return recoNoGR


def getRegionChannelPremium(dbRegChannelPremium, region, channel):
    try:
        regionChannelRec = dbRegChannelPremium.find_one({'region': region, 'channel': channel})
        print('here in regionChannel')
        print(regionChannelRec)
        index = regionChannelRec['index']

        print('regional channel premium ', region, channel, index)
    except:
        index = 1.0
    return index


def getPoliticalRates(dbPoliticalRates, channelName, timeband, dow, startDate, endDate, politicalType):
    monthList = getMonthlist(startDate, endDate)
    rateType = 'SPOTBUY'
    polRateCardFrame = pd.DataFrame(list(dbPoliticalRates.find({'channelName': channelName},
                                                               {'_id': 0, 'channelName': 1, 'monthYear': 1,
                                                                'rate': 1, 'rateType': 1, 'timeBand': 1,
                                                                'type': politicalType})))
    polRateCardFrame = polRateCardFrame[polRateCardFrame['monthYear'].isin(monthList)]
    print('@@Political Rate Card', polRateCardFrame)
    polRateCardFrameAgg = polRateCardFrame.groupby(['channelName', 'rateType', 'timeBand', 'type']).mean()

    rate = polRateCardFrameAgg.loc[(channelName, rateType, timeband, politicalType), 'rate']

    return rate


def getWeekListRatings(startWeek, rollingNo):
    #    print('***rollingNo**',rollingNo)
    #    startWeek='2018-06'
    week = int((startWeek).split('-')[1])
    year = int((startWeek).split('-')[0])
    weekList = []
    j = 0
    for i in range(0, rollingNo):

        if week == 0:
            week = 52
            year = year - 1

        weekList.append(str(year) + '-' + str(week).zfill(2))
        week = week - 1
    #    print ('***weekList**',weekList)
    return weekList


def getRateScenario(startDate, endDate):
    print('in getRateScenario function')
    startQtr = getQuarter(startDate)
    print('startQtr->', startQtr)
    endQtr = getQuarter(endDate)

    if startQtr == 'Q1':
        if endQtr == 'Q1':
            rateScenario = 1
        elif endQtr == 'Q2':
            rateScenario = 2
        elif endQtr == 'Q3':
            rateScenario = 3
        elif endQtr == 'Q4':
            rateScenario = 4
        elif endQtr == 'Q5':
            rateScenario = 4
        else:
            rateScenario = 5
    elif startQtr == 'Q2':
        if endQtr == 'Q2':
            rateScenario = 6
        elif endQtr == 'Q3':
            rateScenario = 7
        elif endQtr == 'Q4':
            rateScenario = 7
        elif endQtr == 'Q5':
            rateScenario = 8
        else:
            rateScenario = 7
    elif startQtr == 'Q3':
        if endQtr == 'Q3':
            rateScenario = 9
        elif endQtr == 'Q4':
            rateScenario = 10
        elif endQtr == 'Q5':
            rateScenario = 10
        else:
            rateScenario = 10
    elif startQtr == 'Q4':
        if endQtr == 'Q4':
            rateScenario = 11
        elif endQtr == 'Q5':
            rateScenario = 11
        else:
            rateScenario = 11
    else:
        rateScenario = 12

    print('**Rate Scenario', rateScenario, startDate, endDate, startQtr, endQtr)

    return rateScenario


def getQuarter(paramDate):
    # yy = paramDate.year
    print('in getQuarter function')

    now = datetime.now()
    print('now->', now)
    # now = datetime_format('2019-04-01')
    yy_fin = now.year
    print('yy_fin->', yy_fin)

    if now.month <= 3:
        yy_fin = yy_fin - 1
    else:
        yy_fin
    finYear = yy_fin
    print('***Financial Year***', yy_fin)

    # if datetime.datetime.now() <=

    #     if paramDate.month <= 6:
    #         yy = yy-1
    #     else:
    #         yy
    # print ('***Paramdate Month',yy,paramDate.month)
    q1Date = datetime_format(str(yy_fin) + '-06-30') + timedelta(hours=6, minutes=30)
    q2Date = datetime_format(str(yy_fin) + '-09-30') + timedelta(hours=6, minutes=30)
    q3Date = datetime_format(str(yy_fin) + '-12-31') + timedelta(hours=6, minutes=30)
    q4Date = datetime_format(str(yy_fin + 1) + '-03-31') + timedelta(hours=6, minutes=30)
    q5Date = datetime_format(str(yy_fin + 1) + '-06-30') + timedelta(hours=6, minutes=30)
    print('q1Date->', q1Date)
    # print ('***Compare', q4Date,paramDate)
    print('paramDate->', paramDate)
    if paramDate <= q1Date:
        qtr = 'Q1'
    elif paramDate <= q2Date:
        qtr = 'Q2'
    elif paramDate <= q3Date:
        qtr = 'Q3'
    elif paramDate <= q4Date:
        qtr = 'Q4'
    elif paramDate <= q5Date:
        qtr = 'Q5'
    else:
        qtr = 'NotValid'

    return qtr


'''
def getScenarioMonthsGhz(startDate):
    #print('GHZ month->', startDate.month, startDate.year)
    monthList = str(startDate.month, startDate.year)
    print(monthList)
    return startDate.month
'''


def getScenarioMonths(rateScenario):
    monthList = []
    if rateScenario == 1:
        monthList = ['04-2018']
    elif rateScenario == 2:
        monthList = ['04-2018', '07-2018']
    elif rateScenario == 3:
        monthList = ['04-2018', '07-2018']
    elif rateScenario == 4:
        monthList = ['04-2018', '07-2018']
    elif rateScenario == 5:
        monthList = ['04-2018', '07-2018']
    elif rateScenario == 6:
        monthList = ['07-2018']
    elif rateScenario == 7:
        monthList = ['07-2018']
    elif rateScenario == 8:
        monthList = ['07-2018', '01-2019']
    elif rateScenario == 9:
        monthList = ['10-2018']
    elif rateScenario == 10:
        monthList = ['10-2018', '01-2019']
    elif rateScenario == 11:
        monthList = ['01-2019']
    else:
        monthList = ['04-2018', '07-2018']
    return monthList


def getRateCard(rateCardFrame, channelName, scenario, startDate, endDate):
    monthList = getScenarioMonths(scenario)
    print('monthList->', monthList)
    rateCardFrame = rateCardFrame[rateCardFrame['monthYear'].isin(monthList)]
    rateCardFrameAgg = rateCardFrame.groupby(['channelName', 'rateType', 'timeBand', 'dayOfWeek']).mean()
    return rateCardFrameAgg, monthList


def getRateCardSpotBuy(rateType, rateCardFrame, channelName, scenario, startDate, endDate, ghzDate):
    if rateType != 'GHZ':
        monthList = getScenarioMonths(scenario)
        print('monthList->', monthList)
    else:
        monthList = ghzDate
        print('ghzDate->', ghzDate)
    monthList = getScenarioMonths(scenario)
    print('monthList->', monthList)
    rateCardFrame = rateCardFrame[rateCardFrame['monthYear'].isin(monthList)]
    rateCardFrameAgg = rateCardFrame.groupby(['channelName', 'rateType', 'timeBand', 'dayOfWeek']).mean()
    return rateCardFrameAgg, monthList


def getComboRateCard(comboRateCardFrame, comboName, scenario, startDate, endDate):
    print('***comboName, scenario,startDate,endDate***->', comboName, scenario, startDate, endDate)
    # monthList = getScenarioMonths(scenario)
    monthList = getMonthlist(startDate, endDate)
    print('***Rate Card MOnth List ', monthList)

    comboRateCardFrame = comboRateCardFrame[comboRateCardFrame['monthYear'].isin(monthList)]
    print('***comboRateCardFrame**->', comboRateCardFrame)
    comboRateCardFrameAgg = comboRateCardFrame.groupby(['comboName', 'rateType', 'timeBand', 'dayOfWeek']).mean()
    print('***comboRateCardFrameAgg**->', comboRateCardFrameAgg)

    return comboRateCardFrameAgg, monthList


def getInflation(dbInflation, channelName):
    try:
        inflationRec = dbInflation.find_one({'channelName': channelName, 'year': '2019'})
        inflationPct = float(inflationRec['inflation'])
    except:
        inflationPct = 1.0
    return inflationPct


def getFinancialYear(paramDate):
    if datetime_format('2016-04-01') <= paramDate <= datetime_format('2016-03-31'):
        finYear = '2016-17'
    else:
        if datetime_format('2017-04-01') <= paramDate <= datetime_format('2017-03-31'):
            finYear = '2017-18'
        else:
            if datetime_format('2018-04-01') <= paramDate <= datetime_format('2019-03-31'):
                finYear = '2018-19'
            else:
                if datetime_format('2019-04-01') <= paramDate <= datetime_format('2020-03-31'):
                    finYear = '2019-20'
    return finYear


def datetime_format(date):
    # return datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=utc) ----commented by ravinder
    return datetime.strptime(date, "%Y-%m-%d")


def getRecoConstants(db, channel):
    
    try:
        recoRec = db.dsRecoFactors.find_one({'channelName': channel})
        multichannelIncentConst = recoRec['multiChannelIncentConst']
        digitalIncentConst = recoRec['digitalIncentConst']
        premiumForHighPTConst = recoRec['premiumForHighPTConst']
        skewPTPercentThreshold = recoRec['skewPTPercentThreshold']
    except:
        apiMstrVar=db.dsBasePriceAllApiMstr.find_one()
 
        # multichannelIncentConst = 0.94
        # digitalIncentConst = 0.96
        # premiumForHighPTConst = 1.05
        # skewPTPercentThreshold = 0.4
        multichannelIncentConst = apiMstrVar['multichannelIncentConst']
        digitalIncentConst = apiMstrVar['digitalIncentConst']
        premiumForHighPTConst = apiMstrVar['premiumForHighPTConst']
        skewPTPercentThreshold = apiMstrVar['skewPTPercentThreshold']

    return multichannelIncentConst, digitalIncentConst, premiumForHighPTConst, skewPTPercentThreshold

def rating_factor_calc(rate_frame, rolling_no, timeband):
    timeBand_rate = re.sub(" ", "", timeband)
    rate_frame.loc[:, 'week_day'].replace(
        ['Weekday', 'Weekend'],
        ['MON-FRI', 'SAT-SUN'], inplace=True)
    print('***rate_frame**', timeband)
    print(rate_frame)
    rate_frame = rate_frame.loc[rate_frame['timeband'] == timeband, :]
    rate_frame.reset_index(drop=True)
    rate_frame.sort_values(['year', 'week'], ascending=[True, True], inplace=True)
    print('***rate_frame** 2')
    print(rate_frame)
    if rolling_no > len(rate_frame):
        rolling_no = len(rate_frame)
    avg_rate = sum(rate_frame.iloc[list(
        range(len(rate_frame) - rolling_no, len(rate_frame)))]['averageRting'].tolist()) / rolling_no
    return (avg_rate)


def getPrevDealIndex(dealId, dbBrief, dbClusterPrevIndex, advertiserClusterId, channelName, financialYear):
    print("************index parameters", channelName, financialYear, advertiserClusterId)
    # changed fy from 2018-19 to 2019-20 - Poorva 21-04-2020
    prevYearDate = datetime(2019, 4, 1)
    fyEnd = datetime(2020, 3, 31)
    prevTwoYearDate = prevYearDate - timedelta(days=365)

    # prevIndexRec = dbClusterPrevIndex.find_one({'advertiserClusterId': advertiserClusterId, 'channel': channelName,
    #                                            'financialYear': '2018-19'})
    # for abp simulation
    prevIndexRec = pd.DataFrame(
        list(dbClusterPrevIndex.find({'advertiserClusterId': advertiserClusterId, 'channel': channelName,
                                      'financialYear': financialYear})))
    print('Prev Index Rec->', prevIndexRec)
    # added for passing for simulation(as cluster prev index not updated)
    try:
        prevIndexRec = prevIndexRec.sort_values('totalOutlay', ascending=False)
    except:
        pass
    # prevIndexRec.to_csv('C:/Users/Lenovo/Desktop/prevIndexRec.csv')
    try:
        prevDealIndex = prevIndexRec.iloc[0]['index']
    except:
        prevDealIndex = 10000

    if prevDealIndex != 10000:
        # BY RAVINDER FOR SIMULATION
        # if prevDealIndex < 0.75 or prevDealIndex > 1.25 :
        if prevDealIndex < 0.5 or prevDealIndex > 2:
            prevDealIndex = 10000
    # new logic for finding prev Deal
    print('prevDealINdex--->>>', prevDealIndex)

    queryOneYear = [{"$match": {"advertiser.clusterId": advertiserClusterId, "channel.name": channelName,
                                "status": "BOOKED", "fromDate": {"$gte": prevYearDate, "$lte": fyEnd}}},
                    {"$lookup": {"from": "dealSummaryTxn", "localField": "dealId", "foreignField": "dealId",
                                 "as": "dealSummary"}}
        , {"$unwind": "$dealSummary"},
                    {"$unwind": "$dealSummary.channels"},
                    {"$match": {"dealSummary.channels.name": channelName}},
                    {"$project": {"_id": 0, "outlay": "$dealSummary.channels.spotBuy.total.final",
                                  "er": "$dealSummary.channels.spotBuy.er.final",
                                  "skewPT": "$dealSummary.channels.spotBuy.ptSkew.final",
                                  "skewWE": "$dealSummary.channels.spotBuy.weSkew.final",
                                  "skewMT": "$dealSummary.channels.spotBuy.mtSkew.final"}}
        , {"$sort": {"outlay": -1}}
        , {"$limit": 1}]

    queryTwoYear = [{"$match": {"advertiser.clusterId": advertiserClusterId, "channel.name": channelName,
                                "status": "BOOKED", "fromDate": {"$gte": prevTwoYearDate, "$lte": fyEnd}}},
                    {"$lookup": {"from": "dealSummaryTxn", "localField": "dealId", "foreignField": "dealId",
                                 "as": "dealSummary"}}
        , {"$unwind": "$dealSummary"},
                    {"$unwind": "$dealSummary.channels"},
                    {"$match": {"dealSummary.channels.name": channelName}},
                    {"$project": {"_id": 0, "outlay": "$dealSummary.channels.spotBuy.total.final",
                                  "er": "$dealSummary.channels.spotBuy.er.final",
                                  "skewPT": "$dealSummary.channels.spotBuy.ptSkew.final",
                                  "skewWE": "$dealSummary.channels.spotBuy.weSkew.final",
                                  "skewMT": "$dealSummary.channels.spotBuy.mtSkew.final"}}
        , {"$sort": {"outlay": -1}}
        , {"$limit": 1}]

    if advertiserClusterId == "":
        prevObject = {'outlay': 0.0, 'er': 0.0, 'skewPT': 0.0, 'skewWE': 0.0, 'skewMT': 0.0}
    else:
        prevList = list(dbBrief.aggregate(queryOneYear))
        print('prevList->', prevList)
        if len(prevList) != 0:
            print('in iff')
            prevObject = prevList[0]
        else:
            print('in elseee')
            prevList = list(dbBrief.aggregate(queryTwoYear))
            print('prevList->', prevList)
            if len(prevList) == 0:
                prevObject = {'outlay': 0.0, 'er': 0.0, 'skewPT': 0.0, 'skewWE': 0.0, 'skewMT': 0.0}
            else:
                prevObject = prevList[0]
    return prevDealIndex, prevObject


def getBasePrice(basePriceFrame, channelName, startDate, endDate):
    monthList = getMonthlist(startDate, endDate)
    # print ('****month List for',monthList)

    basePriceFrame = basePriceFrame[basePriceFrame['monthYear'].isin(monthList)]
    basePriceFrameAgg = basePriceFrame.groupby(['channelName', 'rateType', 'timeBand', 'dayOfWeek']).mean()
    # print(basePriceFrameAgg)
    return basePriceFrameAgg, monthList


def getComboBasePrice(comboBasePriceFrame, comboName, startDate, endDate):
    monthList = getMonthlist(startDate, endDate)
    comboBasePriceFrame = comboBasePriceFrame[comboBasePriceFrame['monthYear'].isin(monthList)]
    comboBasePriceFrameAgg = comboBasePriceFrame.groupby(['comboName', 'rateType', 'timeBand', 'dayOfWeek']).mean()
    return comboBasePriceFrameAgg, monthList


def getLastNWeeks(dbRating, n):
    weekList = dbRating.distinct("yearWeek")
    requiredWeekList = sorted(weekList, reverse=True)[:n]
    # print('***requiredWeekList')
    # print(requiredWeekList)

    return requiredWeekList


def getMonthlist(startDate, endDate):
    start, end = startDate, endDate
    total_months = lambda dt: dt.month + 12 * dt.year
    mlist = []
    for tot_m in range(total_months(start) - 1, total_months(end)):
        y, m = divmod(tot_m, 12)
        mlist.append(datetime(y, m + 1, 1).strftime("%m-%Y"))
    return mlist


def createBudgetFrame(brief, channelNameParam):
    rolling_period = 0
    tgMarket = ""
    budgetFrame = pd.DataFrame()
    channelFrame = pd.DataFrame(list(brief['channel']))

    for i in range(len(channelFrame)):
        channelName = channelFrame.iloc[i]['name']
        budgetFrame.loc[channelName, 'channelName'] = channelFrame.iloc[i]['name']

        budgetFrame.loc[channelName, 'spotBuy'] = channelFrame.iloc[i]['budget']['spotBuy']
        budgetFrame.loc[channelName, 'sponsorship'] = channelFrame.iloc[i]['budget']['sponsorship']
        budgetFrame.loc[channelName, 'spotLight'] = channelFrame.iloc[i]['budget']['spotLight']
        budgetFrame.loc[channelName, 'nonFCTMonthly'] = channelFrame.iloc[i]['budget']['nonFCTMonthly']
        budgetFrame.loc[channelName, 'nonFCTExposure'] = channelFrame.iloc[i]['budget']['nonFCTExposure']
        budgetFrame.loc[channelName, 'minFCT'] = channelFrame.iloc[i]['budget']['minFCT']
        budgetFrame.loc[channelName, 'totalOutlay'] = channelFrame.iloc[i]['budget']['totalOutlay']
        if channelName == channelNameParam:
            rolling_period = channelFrame.iloc[i]['period']['name']
            tgMarket = channelFrame.iloc[i]['targetMarket']['name']

    return budgetFrame, rolling_period, tgMarket


def createtgMktFrame(brief, channelNameParam):
    print('HERE-->>', channelNameParam)
    channelFrame = pd.DataFrame(list(brief['channel']))
    print(channelNameParam)
    for i in range(len(channelFrame)):
        channelName = channelFrame.iloc[i]['name']
        print('channelName->', channelName)
        if channelName == channelNameParam:
            rolling_period = channelFrame.iloc[i]['period']['name']
            tgMarket = channelFrame.iloc[i]['targetMarket']['name']

    return rolling_period, tgMarket


def getGridParameters(dbGrid, brief, dealId, channelName, multichannelIncentConst):
    print('Find Grid', dealId, channelName)
    gridRec = dbGrid.find_one({'dealId': dealId})

    # print (gridRec)
    # print(idframe)
    for i in range(0, len(gridRec['channel'])):

        if gridRec['channel'][i]['name'] == channelName:
            # added by ravinder for simulation handling nulls timebands
            gridTB = [x for x in gridRec['channel'][i]['spotBuy']['timebands'] if x is not None]
            if 'primarySpotBuy' in gridRec['channel'][i]:
                primarySpotBuy = [x for x in gridRec['channel'][i]['primarySpotBuy']['timebands'] if x is not None]
            else:
                primarySpotBuy = []
            nonFCTMonthly = [x for x in gridRec['channel'][i]['nonFCTMonthly']['timebands'] if x is not None]
            nonFCTExposure = [x for x in gridRec['channel'][i]['nonFCTExposure']['timebands'] if x is not None]
            # print("gridddd",gridTB)
            # dfSpotBuyTimeband = pd.DataFrame(list(gridRec['channel'][i]['spotBuy']['timebands']))
            dfSpotBuyTimeband = pd.DataFrame(list(gridTB))
            dfPrimarySpotBuys = pd.DataFrame(list(primarySpotBuy))
            dfNonFCTMonthly = pd.DataFrame(list(nonFCTMonthly))
            dfNonFCTExposure = pd.DataFrame(list(nonFCTExposure))
            try:
                premiumHighPT = 1.05 if gridRec['channel'][i]['isHighPT'] else 1.0
                isHighPT = gridRec['channel'][i]['isHighPT']
            except:
                premiumHighPT = 1.0
                isHighPT = False

                # if not newComboName:
            #     budgetDict = next((item for item in brief['channel']['channel'] if item["name"] == channelName), None)
            # else:
            #     budgetDict = next((item for item in brief['channel']['channel'] if item["name"] == channelName), None)
            # if channelBudgetIncentive and 'isHighPT' in channelBudgetIncentive:
            #     isHighPT = channelBudgetIncentive['isHighPT']
            #     premiumHighPT = 1.05 if channelBudgetIncentive['isHighPT'] else 1.0
            # else:
            #     isHighPT = 1.0
            #     premiumHighPT = 1.0

            channelBudgetIncentive = next(
                (item for item in brief['budgetIncentive']['channel'] if item["name"] == channelName), None)
            if channelBudgetIncentive and 'networkIncentive' in channelBudgetIncentive:
                # Changed morethantwoincentive to networkIncentive - Poorva - 22-04-2020
                networkIncentive = 1 - channelBudgetIncentive['networkIncentive']
            else:
                networkIncentive = 1.0

            try:

                westRegIncentive = gridRec['channel'][i]['westRegIncentive']
            except:
                westRegIncentive = 0.0
            try:
                channelSpecificIncentive = gridRec['channel'][i]['channelIncentive']
            except:
                channelSpecificIncentive = 0.0
            try:

                outlayIncentive = 1 - gridRec['channel'][i]['outlayIncentive']
            except:
                outlayIncentive = 1.0
            try:

                overAllIncentive = 1 - gridRec['channel'][i]['overallIncentive']
            except:
                overAllIncentive = 1.0

            # Channel level newAdvertiserIncentive added - Poorva - 30-04-2020
            try:
                advertiserIncentive = gridRec['channel'][i]['advertiserIncentive']
            except:
                advertiserIncentive = 0.0

            break

    try:
        ChannelAskCurrent = ChannelAsk(channelName, gridRec['outlay'],
                                       gridRec['er'],
                                       gridRec['erRateCard'],
                                       gridRec['erRecommended'],
                                       gridRec['skewPTPercent'],
                                       gridRec['skewWEPercent'],
                                       gridRec['skewMTPercent'],
                                       gridRec['minFCT'])
    except:
        ChannelAskCurrent = ChannelAsk(channelName, 10000,
                                       10000,
                                       10000,
                                       10000,
                                       .25,
                                       .25,
                                       .25
                                       )
    # print('***ChannelAskCurrent***',ChannelAskCurrent)
    # print("bassssssse",multiChannelIncentive,outlayIncentive,overAllIncentive)
    return dfSpotBuyTimeband, dfPrimarySpotBuys, dfNonFCTMonthly, dfNonFCTExposure, ChannelAskCurrent, westRegIncentive, channelSpecificIncentive, outlayIncentive, overAllIncentive, premiumHighPT, isHighPT, networkIncentive, advertiserIncentive


class ChannelAsk:
    def __init__(self, channelName, outlay, er, erRateCard, erRecommended, skewPTPercent, skewWEPercent, skewMTPercent):
        self.channelName = channelName
        self.outlay = outlay
        self.er = er
        self.erRateCard = erRateCard
        self.erRecommended = erRecommended
        self.skewPTPercent = skewPTPercent
        self.skewWEPercent = skewWEPercent
        self.skewMTPercent = skewMTPercent


def rank_factor_calc(rank_frame, Dow, timeband, channel_name, dbRankRules):
    if channel_name != "ABP GANGA":

        rank_frame = rank_frame.loc[(rank_frame['channelName'] == channel_name) & (rank_frame['weekDay'] == Dow) & (
                    rank_frame['timeBand'] == timeband)]
        print('RANK FRAME')
        print(rank_frame)
        try:
            rank = np.mean(rank_frame['rank'])
        except:
            rank = 3
        if math.isnan(rank):
            rank = 3

        print('***Rank_frame**', Dow, timeband, channel_name, rank)
        # if rank== 1:
        #     index =  (1.03)
        # elif rank == 2:
        #     index =  (1.02)
        # elif (rank > 2) & (rank <= 4):
        #     index =  (1.0)
        # else:
        #     #index = (1.0)
        #     #change for lower rank
        #     index =  (0.98)
        # new logic
        rankRulesDF = pd.DataFrame(list(dbRankRules.find({"channel": channel_name})))
        cond0 = rankRulesDF.lower <= rank
        cond1 = rankRulesDF.upper >= rank
        index = 1 + round(float(rankRulesDF[cond0 & cond1].iloc[0]["factor"]), 2)
    else:
        index = 0.98

    return index


def timeSkewPremium(db,timeband, restrictFromTimeband, restrictToTimeband):
    # Change in logic - 5% squeeze premium in case of  25 to 24 hrs squeeze, else 10% - Poorva - 18-04-2020
    print('in time skew premium function')
    # timeBandSqueezePremiumRecoConstant = 1.10
    # timeSkewPremiumPctRateCardConstant = 1.10
    apiMstrVar=db.dsBasePriceAllApiMstr.find_one()
    timeBandSqueezePremiumRecoConstant = apiMstrVar['timeBandSqueezePremiumRecoConstant']
    timeSkewPremiumPctRateCardConstant = apiMstrVar['timeSkewPremiumPctRateCardConstant']
    timeBandSqueezePremiumRecoConstant24Hr = apiMstrVar['timeBandSqueezePremiumRecoConstant24Hr']
    timeSkewPremiumPctRateCardConstant24Hr = apiMstrVar['timeSkewPremiumPctRateCardConstant24Hr']
    timeband1 = timeband.split("-")[0]
    timeband2 = timeband.split("-")[1]
    fromTimeband = int(timeband1.split(':')[0])
    toTimeband = int(timeband2.split(':')[0])
    print('from and to timeband->', fromTimeband, toTimeband)
    restrictFrom = int(restrictFromTimeband.split(':')[0])
    restrictTo = int(restrictToTimeband.split(':')[0])
    print('restrict timeband to and from->', restrictFrom,restrictTo)
    startRestrictHrs = restrictFrom - fromTimeband
    endRestrictHrs = toTimeband - restrictTo
    restrictHrs = startRestrictHrs + endRestrictHrs
    print('start & end RestrictHrs->', startRestrictHrs, endRestrictHrs)
    if timeband == "17:00-25:00":
        if restrictTo <= 24:
            strictHr = 1
            totalRestrictHrs = startRestrictHrs + endRestrictHrs - strictHr
            print('totalRestrictHrs->', totalRestrictHrs)
            timeBandSqueezePremiumReco = (timeBandSqueezePremiumRecoConstant ** (math.ceil(totalRestrictHrs))) * (timeBandSqueezePremiumRecoConstant24Hr ** (math.ceil(strictHr)))
            timeBandSqueezePremiumRateCard = (timeSkewPremiumPctRateCardConstant ** (math.ceil(totalRestrictHrs))) * (timeSkewPremiumPctRateCardConstant24Hr ** (math.ceil(strictHr)))
        else:
            totalRestrictHrs = startRestrictHrs + endRestrictHrs
            print('totalRestrictHrs->', totalRestrictHrs)
            # if totalRestrictHrs == 0:
            #     totalRestrictHrs = 1
            timeBandSqueezePremiumReco = timeBandSqueezePremiumRecoConstant ** (math.ceil(totalRestrictHrs))
            timeBandSqueezePremiumRateCard = timeSkewPremiumPctRateCardConstant ** (math.ceil(totalRestrictHrs))
    else:
        totalRestrictHrs = startRestrictHrs + endRestrictHrs
        print('totalRestrictHrs->', totalRestrictHrs)
        timeBandSqueezePremiumReco = timeBandSqueezePremiumRecoConstant ** (math.ceil(totalRestrictHrs))
        timeBandSqueezePremiumRateCard = timeSkewPremiumPctRateCardConstant ** (math.ceil(totalRestrictHrs))

    print('***timesquezepreimum**', timeBandSqueezePremiumReco, timeBandSqueezePremiumRateCard)

    # Old logic - commented on 18-04-2020
    '''
    baseArray = timeslottedarray(timeband1, timeband2)
    print('baseArray->', baseArray)
    if restrictFromTimeband is None or restrictToTimeband is None:
        restrictFromTimeband = timeband.split("-")[0]
        restrictToTimeband = timeband.split("-")[1]

    timeband_array = timeslottedarray(restrictFromTimeband, restrictToTimeband)
    print(timeband_array)
    overlap = (baseArray * timeband_array).sum()
    total_band = (baseArray).sum()
    print('overlap,total_band->', overlap, total_band)
    non_overlap = math.ceil((total_band - overlap) / 2)
    print('non_overlap->', non_overlap)
    timeBandSqueezePremiumReco = timeBandSqueezePremiumRecoConstant ** (math.ceil(non_overlap))
    timeBandSqueezePremiumRateCard = timeSkewPremiumPctRateCardConstant ** (math.ceil(non_overlap))
    '''
    return timeBandSqueezePremiumReco, timeBandSqueezePremiumRateCard, float(restrictHrs)


def comboTimeSkewPremium(db,timeband, restrictFromTimeband, restrictToTimeband):
    timeBandSqueezePremiumRecoConstant = 1.10
    timeSkewPremiumPctRateCardConstant1 = 1.20
    timeSkewPremiumPctRateCardConstant2 = 1.10
    # timeBandSqueezePremiumRecoConstant = apiMstrVar['timeBandSqueezePremiumRecoConstant']
    # timeSkewPremiumPctRateCardConstant1 = apiMstrVar['timeSkewPremiumPctRateCardConstant1']
    # timeSkewPremiumPctRateCardConstant1 = apiMstrVar['timeSkewPremiumPctRateCardConstant1']
    timeband1 = timeband.split("-")[0]
    timeband2 = timeband.split("-")[1]
    baseArray = timeslottedarray(timeband1, timeband2)
    if restrictFromTimeband is None or restrictToTimeband is None:
        restrictFromTimeband = timeband.split("-")[0]
        restrictToTimeband = timeband.split("-")[1]
    timeband_array = timeslottedarray(restrictFromTimeband, restrictToTimeband)
    overlap = (baseArray * timeband_array).sum()
    total_band = (baseArray).sum()
    non_overlap = math.floor((total_band - overlap) / 2)
    print('combo Time Skew Function-->', overlap, total_band, non_overlap)
    timeBandSqueezePremiumReco = timeBandSqueezePremiumRecoConstant ** (math.ceil(non_overlap))
    if timeband1 == '17:00':
        timeBandSqueezePremiumRateCard = timeSkewPremiumPctRateCardConstant1 ** (math.ceil(non_overlap))
    else:
        timeBandSqueezePremiumRateCard = timeSkewPremiumPctRateCardConstant2 ** (math.ceil(non_overlap))
    print('***timesquezepreimum**', timeBandSqueezePremiumReco, timeBandSqueezePremiumRateCard)

    return timeBandSqueezePremiumReco, timeBandSqueezePremiumRateCard, float(non_overlap)


def timebandTrx(timeband):
    t1 = timeband.split("-")[0]
    t2 = timeband.split("-")[1]

    nightBand_start = "01:00"
    nightBand_end = "06:00"
    morningBand_start = "06:00"
    morningBand_end = "12:00"
    afternoonBand_start = "12:00"
    afternoonBand_end = "18:00"
    eveningBand_start = "18:00"
    eveningBand_end = "24:30"

    nightband_array = timeslottedarray(nightBand_start, nightBand_end)
    timeband_array = timeslottedarray(t1, t2)

    overlayarray = pd.DataFrame(columns=['timeband', 'overlap'])

    nb_overlap = (nightband_array * timeband_array).sum() / timeband_array.sum()

    overlayarray.loc[0, 'timeband'] = "01:00-06:00"
    overlayarray.iloc[0, 1] = nb_overlap

    monrningband_array = timeslottedarray(morningBand_start, morningBand_end)
    mb_overlap = (monrningband_array * timeband_array).sum() / timeband_array.sum()

    overlayarray.loc[1, 'timeband'] = "06:00-12:00"
    overlayarray.iloc[1, 1] = mb_overlap

    afternoonband_array = timeslottedarray(afternoonBand_start, afternoonBand_end)
    ab_overlap = (afternoonband_array * timeband_array).sum() / timeband_array.sum()

    overlayarray.loc[2, 'timeband'] = "12:00-18:00"
    overlayarray.iloc[2, 1] = ab_overlap

    eveningband_array = timeslottedarray(eveningBand_start, eveningBand_end)
    eb_overlap = (eveningband_array * timeband_array).sum() / timeband_array.sum()

    overlayarray.loc[3, 'timeband'] = "18:00-24:30"
    overlayarray.iloc[3, 1] = eb_overlap
    # print(overlayarray)
    band = (
        overlayarray.loc[overlayarray.loc[:, 'overlap'] == overlayarray.loc[:, 'overlap'].max(), 'timeband']).tolist()
    band = band[0]
    return (band)

# Renamed moreThanTwoIncentive to NetworkIncentive - Poorva - 22-04-2020
# Adding priceType premium for nonFCT exposure in waterfall - Poorva - 09-05-2020
class RateCardPrice:
    def __init__(self, ratePrice, rateFactor, monthsConsidered, priceTypePremium, outLayIncentivePct, digitalIncentivePct,
                 multiChannelIncentivePct,
                 activityIncentivePct, westRegionIncentivePct, squeezePremiumPct, spotPositionPremiumPct,
                 newAdvertiserIncentive, premiumForHighPT, networkIncentive):
        self.ratePrice = ratePrice
        self.rateFactor = rateFactor - 1
        self.priceTypePremium = priceTypePremium
        self.outLayIncentivePct = 1 - outLayIncentivePct
        self.digitalIncentivePct = 1 - digitalIncentivePct
        self.multiChannelIncentivePct = 1 - multiChannelIncentivePct
        self.rateCardMonths = monthsConsidered
        self.westRegionIncentivePct = westRegionIncentivePct
        self.activityIncentive = activityIncentivePct
        self.squeezePremiumPct = squeezePremiumPct
        self.spotPositionPremiumPct = spotPositionPremiumPct
        self.newAdvertiserIncentive = 1 - newAdvertiserIncentive
        self.premiumForHighPT = premiumForHighPT - 1
        self.networkIncentive = 1 - networkIncentive


    #        self.computeWaterFall()

    def computeWaterfall(self):
        start = Step(self.ratePrice, 0, 'ADD', 'START')
        step0 = Step(start.end, self.rateFactor, 'ADD', 'Rate Factor')
        step1 = Step(step0.end, self.priceTypePremium, 'ADD', 'L Band Premium')
        step2 = Step(step1.end, self.outLayIncentivePct, 'SUB', 'Outlay Based Incentive')
        step3 = Step(step2.end, self.networkIncentive, 'SUB', 'Network Incentive')
        step4 = Step(step3.end, self.digitalIncentivePct, 'SUB', 'Digital Based Incentive')
        step5 = Step(step4.end, self.multiChannelIncentivePct, 'SUB', 'Multi Channel Incentive')
        step6 = Step(step5.end, self.westRegionIncentivePct, 'SUB', 'West Region Incentive')
        step7 = Step(step6.end, self.activityIncentive, 'SUB', 'Activity Based Incentive')
        step8 = Step(step7.end, self.newAdvertiserIncentive, 'SUB', 'New Advertiser Incentive')
        step9 = Step(step8.end, self.squeezePremiumPct, 'ADD', 'Squeeze Premium')
        step10 = Step(step9.end, self.spotPositionPremiumPct, 'ADD', 'Spot Position Premium')
        step11 = Step(step10.end, self.premiumForHighPT, 'ADD', 'Premium for High PT')
        final = Step(step11.end, 0, 'ADD', 'Final')
        return [start, step0, step1, step2, step3, step4, step5, step6, step7, step8, step9, step10, step11, final]


class RecoParameters:
    def __init__(self, recoPrice, recoIndex, prevDealIndex, indexFromOutlayAndCategory, premiumforHighPt,
                 monthsConsidered, rankingFactor, inflationPct, digitalIncentivePct, multiChannelIncentivePct,
                 squeezePremiumPct, spotPositionPremiumPct, recoOverallFactor, regionChannelPremium,
                 newAdvertiserIncentive, inventoryFill, locPremium, networkIncentive,recoMktSharePct,recoMktShareFactor):
        self.recoPrice = recoPrice
        self.prevDealIndex = prevDealIndex
        self.indexFromOutlayAndCategory = indexFromOutlayAndCategory
        self.regionChannelPremium = regionChannelPremium
        self.locPremium = locPremium
        self.multiChannelIncentivePct = multiChannelIncentivePct
        self.digitalIncentivePct = digitalIncentivePct
        self.newAdvertiserIncentive = newAdvertiserIncentive
        self.recoIndex = recoIndex
        self.rankingFactor = rankingFactor
        self.inflationPct = inflationPct
        self.premiumforHighPT = premiumforHighPt
        self.squeezePremiumPct = squeezePremiumPct
        self.spotPositionPremiumPct = spotPositionPremiumPct
        self.inventoryFill = inventoryFill
        self.recoOverallFactor = recoOverallFactor
        self.monthsConsidered = monthsConsidered
        self.networkIncentive = networkIncentive
        self.recoMktSharePct = recoMktSharePct
        self.recoMktShareFactor = recoMktShareFactor


class RecoPrice:
    def __init__(self, recoPrice, monthsConsidered, rankingFactor, inflationPct, digitalIncentivePct,
                 multiChannelIncentivePct,
                 squeezePremiumPct, spotPositionPremiumPct):
        self.recoPrice = recoPrice
        self.monthsConsidered = monthsConsidered
        self.rankingFactor = rankingFactor
        self.inflationPct = inflationPct
        self.digitalIncentivePct = digitalIncentivePct
        self.multiChannelIncentivePct = multiChannelIncentivePct
        self.squeezePremiumPct = squeezePremiumPct
        self.spotPositionPremiumPct = spotPositionPremiumPct

    def computeWaterfall(self):
        start = Step(self.recoPrice, 0, 'ADD', 'START')

    def computePct(self, pct):
        if pct > 1:
            pct = pct - 1
            action = 'ADD'
        else:
            pct = 1 - pct
            action = 'SUB'

        return pct, action


class Step:
    def __init__(self, startValue, pct, action, attribute):
        self.startValue = startValue
        if action == 'SUB':
            self.value = startValue * pct * -1
        else:
            self.value = startValue * pct

        self.end = startValue + self.value

        self.attribute = attribute

        self.pct = pct


# ADDED extra argument spotTypeDF for simulation
def getPreviousPrice(dbTimeBandPrecedence, channelName, advertiserClusterId, timeband, rateType, dow, restrictHrs,
                     spotPosition):
    print('***In previous Price', channelName, advertiserClusterId, timeband, rateType, dow, restrictHrs, spotPosition)
    previousPrice = None
    virtualPreviousPrice = None
    deEscalationConstant = 0.1
    try:
        if rateType in ["FPR", "PREMIUM RODP"]:
            previousPriceRec = dbTimeBandPrecedence.find_one(
                {'advertiserClusterId': advertiserClusterId, 'timeBand': timeband, \
                 'channel': channelName, 'rateType': rateType, 'dow': dow,
                 'spotType': spotPosition})
        else:
            previousPriceRec = dbTimeBandPrecedence.find_one(
                {'advertiserClusterId': advertiserClusterId, 'timeBand': timeband, \
                 'channel': channelName, 'rateType': rateType, 'dow': dow, 'restrictHours': restrictHrs,
                 'spotType': spotPosition})

        print("*********entered try********************", previousPriceRec)
        previousPrice = float(previousPriceRec['maxPrice'])
        print("previousPirce", previousPrice)

    except:
        previousPrice = None
        if timeband == "19:00-23:00":
            previousPriceFrame = pd.DataFrame(list(dbTimeBandPrecedence.find(
                {'advertiserClusterId': advertiserClusterId, "$or": [{'timeBand': "18:00-24:00",
                                                                      'restrictHours': 2},
                                                                     {'timeBand': '17:00-25:00', 'restrictHours': 3}],
                 'channel': channelName, 'dow': dow, \
                 'spotType': spotPosition})))
        elif rateType in ["FPR","PREMIUM RODP"]:
            # rateType = "FPR" if rateType == "PREMIUM RODP" else rateType
            previousPriceFrame = pd.DataFrame(
                list(dbTimeBandPrecedence.find({'advertiserClusterId': advertiserClusterId, 'timeBand': timeband, \
                                                'channel': channelName, 'rateType': "FPR", 'dow': dow, \
                                                'spotType': spotPosition})))


        else:
            previousPriceFrame = pd.DataFrame(
                list(dbTimeBandPrecedence.find({'advertiserClusterId': advertiserClusterId, 'timeBand': timeband, \
                                                'channel': channelName, 'rateType': rateType, 'dow': dow, \
                                                'spotType': spotPosition})))
            print('***Previous Price Frame')
            # print(previousPriceFrame)
        try:
            minRestrictHours = min(previousPriceFrame['restrictHours'])
            print('minRestrictHours ', minRestrictHours)
            previousPriceRec = previousPriceFrame[previousPriceFrame['restrictHours'] == minRestrictHours]
            print('previouPriceRec', previousPriceRec)
            diffRestrictHours = restrictHrs - minRestrictHours

            if rateType in ['RODP', 'ASR']:
                deEscalationFactor = (1 + deEscalationConstant) ** diffRestrictHours
            else:
                deEscalationFactor = 1

            print('****DeEscalation Factor**', deEscalationFactor)
            virtualPreviousPrice = float(min(previousPriceRec['maxPrice']) * deEscalationFactor)
            print('****VirtualPrevios Price', virtualPreviousPrice, 'minRestrictHours', minRestrictHours,
                  'diffRestrictHours', diffRestrictHours)
        except:
            virtualPreviousPrice = None

    return previousPrice, virtualPreviousPrice


def getIndexFromOutlayAndCategory(dbShdBindex, dbAdvrtsrMstr, dbAdvrtsrClstr, channelName, advertiser, totalOutlay,
                                  brief_advertiserClusterId):
    shouldBeIndexRec = dbShdBindex.find_one({'channel': channelName})
    shouldBeIndexFrame = pd.DataFrame(list(shouldBeIndexRec['outlayIndex']))
    # Code changed - use advertiser cluster id from brief and fetch category from advertiserCluster collection - Poorva - 23-04-2020
    try:
        advertiserClusterRec = dbAdvrtsrClstr.find_one({'clusterID': brief_advertiserClusterId})
        advertiserCategoryList = advertiserClusterRec['advertiserClusterCategory']
    except:
        advertiserCategoryList = ["MISCELLANEOUS"]
    print('***Cluster Id', brief_advertiserClusterId, advertiserCategoryList)

    # Old logic
    '''
    try:
        print('brief_advertiserClusterId->>', brief_advertiserClusterId)
        try:
            print('***Advertiser**', advertiser)
            ##by ravinder added for simulation abp
            try:
                print('in try1')
                advertiserClusterMstr = dbAdvrtsrClstr.find_one(
                    {'advertisers': {'$regex': '^' + advertiser + '$', '$options': 'i'}})
                advertiserClusterId = advertiserClusterMstr["clusterID"]

            except:
                print('in except')
                advertiserMstr = dbAdvrtsrMstr.find_one({'name': {'$regex': advertiser, '$options': 'i'}})
                # print(advertiserClusterMstr)
                advertiserClusterId = advertiserMstr['clusterId']
                # print('adv clusterid->>',advertiserClusterId )
        except:
            print('in this except')
            # advertiserMstr = dbAdvrtsrMstr.find_one({'clusterId': {'$regex': brief_advertiserClusterId, '$options': 'i'}})
            advertiserMstr = dbAdvrtsrMstr.find_one({'clusterId': brief_advertiserClusterId})
            advertiserClusterId = advertiserMstr['clusterId']
    except:
        print('in last except')
        advertiserCategoryList = ["MISCELLANEOUS"]
        advertiserClusterId = ''
    try:
        # print('in try')
        advertiserClusterRec = dbAdvrtsrClstr.find_one({'clusterID': advertiserClusterId})
        advertiserCategoryList = advertiserClusterRec['advertiserClusterCategory']
        print('***Cluster Id', advertiserClusterId, advertiserCategoryList)
    except:
        # print('in exce 1')
        advertiserCategoryList = ["MISCELLANEOUS"]
        advertiserClusterId = ''
    '''
    shouldBeIndexFrame = shouldBeIndexFrame[shouldBeIndexFrame['superCategory'].isin(advertiserCategoryList)]
    shouldBeIndexFrame = shouldBeIndexFrame.reset_index()

    print('***totalOutlay', totalOutlay)
    totalOutlay = int(totalOutlay / 100000)
    print("***totalOutlay", totalOutlay)
    if totalOutlay > 3000:
        totalOutlay = 3000
        print("***Corrected totalOutlay", totalOutlay)
    shouldBeIndexFrame = shouldBeIndexFrame[
        (shouldBeIndexFrame['outlayMax'] >= totalOutlay) & (shouldBeIndexFrame['outlay'] <= totalOutlay)]

    # print('***shouldBeIndexFrame', shouldBeIndexFrame)

    # print(indexfromOutlayANDcategory)
    if shouldBeIndexFrame.empty:
        indexfromOutlayAndCategory = 1.0
    else:
        indexfromOutlayAndCategory = max(shouldBeIndexFrame['index'])
    return indexfromOutlayAndCategory, brief_advertiserClusterId


def sponTitleRateFactor(channel_name, showTitle, sponTtlInv_db):
    sponTitlInvFrame = pd.DataFrame(list(sponTtlInv_db.find({'channel.name': channel_name, 'name': showTitle})))
    rate = sponTitlInvFrame['rate'].tolist()[0]
    print("********** rate ", rate)
    rate = rate.split("+ ")[1]
    rate = rate.split(" ")[0]
    rate = float(re.sub("%", "", rate))
    rate_factor = 1 + (rate / 100)
    return rate_factor


def incomingDateFormat(date):
    if isinstance(date, datetime) == False:
        if 'T' in date:
            date = date.split("T")[0]
        date = datetime_format(date)
    return date


'''
def weekSharePriceSpilt(price_weekday,price_weekend,startdate,enddate,dow):
    weekday = len(getWeekArray("MON-FRI"))
    weekend = len(getWeekArray("SAT-SUN"))
    #daysofweek = len(getWeekArray(dow))
    daysofweek = getWeekArray(dow)


    x = create_day_of_week_array(startdate,enddate,daysofweek)
    y = create_day_of_week_array(startdate,enddate,daysofweek)
    z = create_day_of_week_array(startdate,enddate,daysofweek)
    weekday = sum([a*b for a,b in zip(x,z)])
    weekend = sum([a*b for a,b in zip(y,z)])
    print("price weekend:",price_weekend,"  price weekday:  ",price_weekday,"  share weekend: ",weekend,"share weekday: ",weekday,"  dow: ",dow )
    price = ((price_weekday*weekday) + (price_weekend*weekend))/(weekend + weekday)
    print("price:  ",price)
    return price
'''


def weekSharePriceSpilt(price_weekday, price_weekend, dow):
    daysofweek = getWeekArray(dow)
    weekdayList = ['M', 'T', 'W', 't', 'F']
    weekendList = ['S', 's']
    weekday = len(set(daysofweek) & set(weekdayList))
    weekend = len(set(daysofweek) & set(weekendList))
    print("price weekend:", price_weekend, "  price weekday:  ", price_weekday, "  weekend: ", weekend, "weekday: ",
          weekday, "  dow: ", dow)
    price = ((price_weekday * weekday) + (price_weekend * weekend)) / (weekend + weekday)
    print("price:  ", price)
    return price


def create_day_of_week_array(startdate, enddate, daysofweek):
    date_array = np.zeros(1000, dtype=float)
    daysListInt = create_int_list(daysofweek)
    startRefDate = datetime_format('2017-06-01')
    ### 2017-06-01 is a Thursday
    startRefWeekday = startRefDate.weekday()
    endRefDate = datetime_format('2019-12-31')
    try:
        startpoint = (startdate.replace(tzinfo=utc) - startRefDate.replace(tzinfo=utc)).days
        endpoint = (enddate.replace(tzinfo=utc) - startRefDate.replace(tzinfo=utc)).days
    except Exception as e:
        print(e)
    if startpoint < 0:
        startpoint = 1
    if endpoint > 943:
        endpoint = 943
        if endpoint < 0:
            endpoint = 1
    for i in range(startpoint, endpoint + 1):
        dayofweek = (i - startRefWeekday) % 7
        # print (dayofweek)
        if dayofweek in daysListInt:
            date_array[i] = 1
        else:
            date_array[i] = 0
    return date_array


def create_int_list(daysofweek):
    dayListInt = []
    for i in daysofweek:
        dayListInt.append(create_dayint(i))
    return dayListInt


def create_dayint(daychar):
    dic = {'M': 0, 'T': 1, 'W': 2, 't': 3, 'F': 4, 'S': 5, 's': 6}.get(daychar, 'None')
    print(dic)
    return dic


def getWeekArray(weekBand):
    startDay = weekBand.split('-')[0]
    try:
        endDay = weekBand.split('-')[1]
    except:
        endDay = startDay
    print('***StartDay', startDay, '***EdnDay', endDay)

    weekList = []
    for i in range(getWeekDay(startDay), getWeekDay(endDay) + 1):
        weekList.append(weekDayFromInt(i))

    print(weekList)
    return weekList


def weekDayFromInt(dayInt):
    return {
        0: 'M',
        1: 'T',
        2: 'W',
        3: 't',
        4: 'F',
        5: 'S',
        6: 's',
    }.get(dayInt, 'None')


def getWeekDay(dayText):
    return {'MON': 0
        , 'TUE': 1
        , 'WED': 2
        , 'THU': 3
        , 'FRI': 4
        , 'SAT': 5
        , 'SUN': 6}.get(dayText, 'None')


def lowerBoundTolerance(reco_price, rc_price):
    if reco_price < (.75 * rc_price):
        reco_price = .75 * rc_price
    return reco_price


# added by ravinder for new scenario
# function returns the type of deal (long term or short term)
def isDealLongTerm(startDate, endDate):
    diff = endDate - startDate
    return True if diff.days > 183 else False


# function which return yearMonth from date
def getMonthYear(inputDate):
    monthYear = (str(inputDate.month) if len(str(inputDate.month)) == 2 else "0" + str(inputDate.month)) + "-" + str(
        inputDate.year)
    # monthYear = (str(inputDate.month) if len(str(inputDate.month)) == 2 else "0" + str(inputDate.month)) + "-" + "2019"
    return monthYear


# function which returns the dictionary [{month:days}]
def getDealMonthDays(startDate, endDate):
    dateList = pd.date_range(startDate, endDate, freq='D').tolist()
    monthYearList = [getMonthYear(i) for i in dateList]
    distinctMonthYear = list(set(monthYearList))
    monthDaysDF = pd.DataFrame()
    for i in range(len(distinctMonthYear)):
        monthDaysDF.loc[i, "monthYear"] = distinctMonthYear[i]
        monthDaysDF.loc[i, "noOfDays"] = monthYearList.count(distinctMonthYear[i])
    return monthDaysDF

# Added new logic for getting rate card - Poorva - 21-04-2020
def getRateCardLogic(rateCardRecFrame, startDate, endDate):
    print('in rate card logic function')
    monthDaysDF = getDealMonthDays(startDate, endDate)
    print('monthDays->', monthDaysDF)
    monthList = list(monthDaysDF['monthYear'])
    print('monthList-->>', monthList)
    rateCardRecFrame = rateCardRecFrame[rateCardRecFrame['monthYear'].isin(monthList)]
    print(rateCardRecFrame[['timeBand','rate']])
    uniqueRatesTimeband = pd.DataFrame(rateCardRecFrame[['channelName', 'rateType', 'timeBand', 'dayOfWeek','rate']]).drop_duplicates()
    rateCardRecFrameAgg = uniqueRatesTimeband.groupby(['channelName', 'rateType', 'timeBand', 'dayOfWeek']).mean()
    print('rate card after agg')
    print(rateCardRecFrameAgg)
    print('monthList->>', monthList)
    return rateCardRecFrameAgg, monthList

# Added new logic for getting reco - Poorva - 21-04-2020
def getRecoLogic(basePriceFrameRec, startDate, endDate):
    print('in reco logic function')
    monthDaysDF = getDealMonthDays(startDate, endDate)
    print('monthDays->', monthDaysDF)
    monthList = list(monthDaysDF['monthYear'])
    print('monthList-->>', monthList)
    basePriceFrameRec = basePriceFrameRec[basePriceFrameRec['monthYear'].isin(monthList)]
    basePriceFrameAgg = basePriceFrameRec.groupby(['channelName', 'rateType', 'timeBand', 'dayOfWeek']).mean()
    print('base price frame after agg')
    print(basePriceFrameAgg)
    print('monthList->>', monthList)
    return basePriceFrameAgg, monthList

# Implement reco market share incentive logic - Poorva - 07-05-2020
def getRecoMarketShare(channel, advertiserClusterId, dbRecoMarketShare, dbRecoMktGuardrail):
    try:
        recoMktShare = dbRecoMarketShare.find_one({'advertiser.clusterId': advertiserClusterId})
        recoMktGuardrail = dbRecoMktGuardrail.find_one({'channel': channel})
        #recoMktShare = pd.DataFrame(list(recoMktShare['marketShare']))
        recoMktShare = pd.DataFrame(recoMktShare['marketShare']).T
        recoMktShare = recoMktShare[recoMktShare['channelName'] == channel].reset_index()
        recoMktSharePct = recoMktShare['sharePct'][0]
        totalFct = recoMktShare['totalFCT'][0]
        if recoMktSharePct == 0:
            recoDiscountPct = recoMktGuardrail['discountNoShare']
        elif recoMktGuardrail['shareLower'] < recoMktSharePct <= recoMktGuardrail['shareUpper'] and totalFct >= 5000:
            recoDiscountPct = recoMktGuardrail['discountPct']
        else:
            recoDiscountPct = 0.0
    except:
        recoMktSharePct = 100
        totalFct = 0
        recoDiscountPct = 0.0
    return recoMktSharePct, recoDiscountPct

# Old funtion - not used now
##added new logic for getting rate cards by ravinder
def getRateCardNewLogic(rateCardRecFrame, startDate, endDate, year):
    print('in rate card logic function')
    monthDaysDF = getDealMonthDays(startDate, endDate)
    print('monthDays->', monthDaysDF)
    # print(list(monthDaysDF['monthYear']))
    # monthList = np.zeros(12)
    # festiveList = np.zeros(12)
    # festiveActualList = np.zeros(12)
    # nonFestiveList = np.zeros(12)
    # actualMonths = monthDaysDF["monthYear"].apply(lambda x: int(x.split("-")[0]) - 1).tolist()
    # print("actualMonths", actualMonths)
    # requiredMonthList = monthList.copy()
    # requiredMonthList[actualMonths] = 1.0
    # # festive = [3, 4]
    # # festiveActual = [8, 9, 10, 11, 0, 1, 2]
    # # nonFestive = [3, 4, 5, 6, 7]
    # festive = [9, 10, 11, 0, 1, 2, 3, 4, 5, 6]
    # festiveActual = [8, 9, 10, 11, 0, 1, 2]
    # nonFestive = [7, 8]
    # nonFestiveList[nonFestive] = 1.0
    # festiveList[festive] = 1.0
    # festiveActualList[festiveActual] = 1.0
    # nonFestiveOverlap = sum(nonFestiveList * requiredMonthList)
    # print('nonFestive overlap->>', nonFestiveOverlap)
    # festiveOverlap = sum(festiveList * requiredMonthList)
    # festiveActualOverlap = sum(festiveActualList * requiredMonthList)
    # print('festiveactualoverlap-->', festiveActualOverlap)
    # dictOverlaps = {"festive": festiveOverlap, "nonFestive": nonFestiveOverlap, "festiveActual": festiveActualOverlap}
    # # RATEcard month list
    # festive = ["09", "10", "11", "12", "01", "02", "03", "04", "05", "06"]
    # # nonFestive = ["04", "05", "06", "07", "08"]
    # nonFestive = ["07", "08"]
    # festiveMonthList = list(map(lambda x: x + "-" + year if int(x) > 6 else x + "-" + str(int(year) + 1), festive))
    # nonFestiveMonthList = list(
    #     map(lambda x: x + "-" + year if int(x) > 3 else x + "-" + str(int(year) + 1), nonFestive))
    # print('festiveMonthList')
    # print(festiveMonthList)
    # print('nonfestiveMonthList')
    # print(nonFestiveMonthList)
    # # changed for current condition as next year rates wont be there
    # # festiveMonthList = list(map(lambda x: x + "-" + year if int(x) > 3 else x + "-" + str(int(year) + 1), festive))
    # # nonFestiveMonthList = list(map(lambda x: x + "-" + year if int(x) > 3 else x + "-" + str(int(year) + 1), nonFestive))
    # dictMonthList = {"festive": festiveMonthList, "nonFestive": nonFestiveMonthList, "festiveActual": festiveMonthList}
    # print('dictMonthList')
    # print(dictMonthList)
    # if festiveActualOverlap != 0 and nonFestiveOverlap != 0:
    #     print('ratecardlogic--if---')
    #     festiveMonthList = dictMonthList["festive"]
    #     nonFestiveMonthList = dictMonthList["nonFestive"]
    #     rateCardRecFrameFestive = rateCardRecFrame[rateCardRecFrame['monthYear'].isin(festiveMonthList)]
    #     rateCardFrameAggFestive = rateCardRecFrameFestive.groupby(
    #         ['channelName', 'rateType', 'timeBand', 'dayOfWeek']).mean()
    #     rateCardRecFrameNonFestive = rateCardRecFrame[rateCardRecFrame['monthYear'].isin(nonFestiveMonthList)]
    #     rateCardFrameAggNonFestive = rateCardRecFrameNonFestive.groupby(
    #         ['channelName', 'rateType', 'timeBand', 'dayOfWeek']).mean()
    #     rateCardRecFrame = rateCardFrameAggFestive.append(rateCardFrameAggNonFestive)
    #     rateCardFrameAgg = rateCardRecFrame.groupby(['channelName', 'rateType', 'timeBand', 'dayOfWeek']).mean()
    #     monthList = festiveMonthList.copy()
    #     monthList.extend(nonFestiveMonthList)
    #     print("monthList", monthList)
    #
    # elif startDate.replace(tzinfo=pytz.UTC) > datetime(int(year), 9, 1).replace(tzinfo=pytz.UTC):
    #     print('in else if -- ratecardlogic')
    #     monthList = dictMonthList["festive"]
    #     print('monthList-->>', monthList)
    #     rateCardRecFrame = rateCardRecFrame[rateCardRecFrame['monthYear'].isin(monthList)]
    #     rateCardFrameAgg = rateCardRecFrame.groupby(['channelName', 'rateType', 'timeBand', 'dayOfWeek']).mean()
    #     print('rateCardframe after agg')
    #     print(rateCardFrameAgg)
    #
    # else:
    monthList = list(monthDaysDF['monthYear'])
    #monthList = dictMonthList[max(dictOverlaps, key=dictOverlaps.get)]
    print('monthList-->>', monthList)
    rateCardRecFrame = rateCardRecFrame[rateCardRecFrame['monthYear'].isin(monthList)]
    rateCardFrameAgg = rateCardRecFrame.groupby(['channelName', 'rateType', 'timeBand', 'dayOfWeek']).mean()
    print('rate card after agg')
    print(rateCardFrameAgg)
    print('monthList->>', monthList)
    return rateCardFrameAgg, monthList

# rate card for one line item

def getNonFCTRateCardNewLogic(rateCardRecFrame, startDate, endDate, year):
    monthDaysDF = getDealMonthDays(startDate, endDate)
    print('monthdays DF->', monthDaysDF)
    monthList = list(monthDaysDF['monthYear'])
    print("monthList",monthList)
    rateCardRecFrame = rateCardRecFrame[rateCardRecFrame['monthYear'].isin(monthList)]
    # Changed nonFCT exposure logic - avg of two time periods - Poorva - 24-04-2020
    print(rateCardRecFrame[['name', 'timeBand', 'dow', 'price']])
    uniqueRatesTimeband = pd.DataFrame(rateCardRecFrame[['name', 'timeBand', 'dow', 'price']]).drop_duplicates()
    rateCardFrameAgg = uniqueRatesTimeband.groupby(['name', 'timeBand', 'dow']).mean().reset_index()
    # old logic - getting mean
    #rateCardFrameAgg = rateCardRecFrame.mean()
    rate = rateCardFrameAgg["price"][0]
    return rate, monthList


def getHourValue(time):
    return int(time.split(":")[0])


# find overlap hour
def getOverlapNonOverlapHrs(baseTB, refTB):
    timeband1 = baseTB.split("-")[0]
    timeband2 = baseTB.split("-")[1]

    reftb1 = refTB.split("-")[0]
    reftb2 = refTB.split("-")[1]
    # checks which one should be base timeband
    refStartHr = getHourValue(reftb1)
    refEndHr = getHourValue(reftb2)
    baseStartHr = getHourValue(timeband1)
    baseEndHr = getHourValue(timeband2)
    if baseStartHr <= refStartHr and baseEndHr >= refEndHr:
        baseArray = timeslottedarray(timeband1, timeband2)
        timeband_array = timeslottedarray(reftb1, reftb2)
    else:
        baseArray = timeslottedarray(reftb1, reftb2)
        timeband_array = timeslottedarray(timeband1, timeband2)

    overlap = (baseArray * timeband_array).sum()
    total_band = (baseArray).sum()

    # print('overlap,total_band->', overlap,total_band)
    non_overlap = math.ceil((total_band - overlap) / 2)
    # print("nonnnn",non_overlap)
    return float(overlap / 2), float(non_overlap)


# added for new rate card where restrict hours >3
def getRCClosestTBFPR(rateCardFrame, timeband, channelName, rateType, dow, squeezConst, newComboName):
    timebandErrorValues = ['0', '0:00', '', '0:0', '00:00', np.nan]
    if newComboName is None:
        channelName = channelName
    else:
        channelName = newComboName
    try:
        rateCardPrice = float(rateCardFrame.loc[(channelName, rateType, timeband, dow), "rate"])
    except:
        if timeband not in timebandErrorValues:
            startHr = getHourValue(timeband.split("-")[0])
            endHr = getHourValue(timeband.split("-")[1])
            rateCardFrame = rateCardFrame.reset_index()

            rateCardFrame["startHr"] = rateCardFrame["timeBand"].apply(lambda x: getHourValue(x.split("-")[0]))
            rateCardFrame["endHr"] = rateCardFrame["timeBand"].apply(lambda x: getHourValue(x.split("-")[1]))
            cond0 = rateCardFrame.rateType == rateType
            rateCardFrame = rateCardFrame[cond0]

            cond1 = rateCardFrame.startHr <= startHr
            cond2 = rateCardFrame.endHr > startHr

            cond4 = rateCardFrame.startHr >= startHr
            cond3 = rateCardFrame.endHr <= endHr
            cond5 = rateCardFrame.endHr >= endHr
            cond6 = rateCardFrame.startHr < endHr

            rateCardFrame["baseTB"] = timeband
            # tempDF = rateCardFrame[cond1&cond2]
            # tempdf2 = rateCardFrame[cond3&cond4]
            # print("tttt",tempDF)

            rateCardFrame = rateCardFrame[((cond1 & cond2) | (cond3 & cond4) | (cond4 & cond5 & cond6))]
            print("columns", rateCardFrame)

            rateCardFrame["overlapHrs"] = rateCardFrame.apply(
                lambda x: getOverlapNonOverlapHrs(x["baseTB"], x["timeBand"])[0], axis=1)
            rateCardFrame["nonOverlapHrs"] = rateCardFrame.apply(
                lambda x: getOverlapNonOverlapHrs(x["baseTB"], x["timeBand"])[1], axis=1)
            rateCardFrame["product"] = rateCardFrame.apply(lambda x: x.overlapHrs * x.rate, axis=1)

            rateCardFrame.drop(columns=["startHr", "endHr", "rate"], axis=1, inplace=True)
            rateCardFrameAgg = rateCardFrame.groupby(["channelName", "rateType", "dayOfWeek"]).sum().eval(
                'rate = product / overlapHrs')

            # rateCardFrameAgg = rateCardFrame.groupby(["channelName", "rateType", "dayOfWeek"]).sum()
            non_overlap = float(rateCardFrameAgg.loc[(channelName, rateType, dow), "nonOverlapHrs"])
            # print("raaaaa",rateCardFrameAgg)

            rateCardPrice = float(rateCardFrameAgg.loc[(channelName, rateType, dow), "rate"])

            # rateCardPrice = (squeezConst ** (math.ceil(non_overlap))) * rateCardPrice

        else:
            rateCardPrice = 1000

    return rateCardPrice


def getRCClosestTBNonFPR(rateCardFrame, timeband, channelName, rateType, dow, squeezConst, newComboName):
    timebandErrorValues = ['0', '0:00', '', '0:0', '00:00', np.nan]
    if newComboName is None:
        channelName = channelName
    else:
        channelName = newComboName
    try:
        rateCardPrice = float(rateCardFrame.loc[(channelName, rateType, timeband, dow), "rate"])
    except:
        if timeband not in timebandErrorValues:
            startHr = getHourValue(timeband.split("-")[0])
            endHr = getHourValue(timeband.split("-")[1])
            rateCardFrame = rateCardFrame.reset_index()
            rateCardFrame["startHr"] = rateCardFrame["timeBand"].apply(lambda x: getHourValue(x.split("-")[0]))
            rateCardFrame["endHr"] = rateCardFrame["timeBand"].apply(lambda x: getHourValue(x.split("-")[1]))
            cond0 = rateCardFrame.rateType == rateType
            rateCardFrame = rateCardFrame[cond0]

            cond1 = rateCardFrame.startHr <= startHr
            cond2 = rateCardFrame.endHr > startHr

            cond4 = rateCardFrame.startHr >= startHr
            cond3 = rateCardFrame.endHr <= endHr
            cond5 = rateCardFrame.endHr >= endHr
            cond6 = rateCardFrame.startHr < endHr

            rateCardFrame["baseTB"] = timeband
            # tempDF = rateCardFrame[cond1&cond2]
            # tempdf2 = rateCardFrame[cond3&cond4]
            # print("tttt",tempDF)

            rateCardFrame = rateCardFrame[((cond1 & cond2) | (cond3 & cond4) | (cond4 & cond5 & cond6))]
            print("columns", rateCardFrame)

            rateCardFrame["overlapHrs"] = rateCardFrame.apply(
                lambda x: getOverlapNonOverlapHrs(x["baseTB"], x["timeBand"])[0], axis=1)
            rateCardFrame["nonOverlapHrs"] = rateCardFrame.apply(
                lambda x: getOverlapNonOverlapHrs(x["baseTB"], x["timeBand"])[1], axis=1)
            # rateCardFrame["product"] = rateCardFrame.apply(lambda x: x.overlapHrs * x.rate, axis=1)
            rateCardFrame = rateCardFrame[rateCardFrame["overlapHrs"] > 3]
            # print("rrrrrrrrrrrrrrr",rateCardFrame["timeBand"].tolist())

            # rateCardFrame.drop(columns=["startHr", "endHr", "rate"], axis=1, inplace=True)
            # rateCardFrameAgg = rateCardFrame.groupby(["channelName", "rateType", "dayOfWeek"]).sum()

            rateCardFrameAgg = rateCardFrame.groupby(["channelName", "rateType", "dayOfWeek"]).mean()
            non_overlap = float(rateCardFrameAgg.loc[(channelName, rateType, dow), "nonOverlapHrs"])

            rateCardPrice = float(rateCardFrameAgg.loc[(channelName, rateType, dow), "rate"])
            if non_overlap == 1 or rateType == "ROS":
                rateCardPrice = rateCardPrice
            else:
                rateCardPrice = (squeezConst ** (math.ceil(non_overlap))) * rateCardPrice

        else:
            rateCardPrice = 1000

    return rateCardPrice


# new previous price logic
#
# def getNewPreviousPrice(dbTimeBandPrecedence,channelName, advertiserClusterId,timeband,rateType,dow,restrictHrs, spotPosition, spotTypeDF):
#     print ('***In previous Price', channelName, advertiserClusterId,timeband,rateType,dow,restrictHrs, spotPosition)
#     previousPrice = None
#     virtualPreviousPrice = None
#     deEscalationConstant = 0.1
#     try:
#         previousPriceRec = pd.DataFrame(list(dbTimeBandPrecedence.find({'advertiserClusterId':advertiserClusterId, \
#                                                           'channel':channelName,'rateType':rateType,'dow':dow,'restrictHours':restrictHrs,
#                                                               'spotType':spotPosition})))
#         print("*********entered try********************")
#         #changing name so that same function can be called
#         previousPriceRec.rename(columns={"maxPrice":"rate"},inplace=True)
#         previousPrice = getRCClosestTB(previousPriceRec,timeband,channelName,rateType,dow)
#         previousPrice = previousPriceRec['maxPrice']
#
#     except:
#         previousPrice = None
#         try:
# #                print('***In previous Price Try ', channelName, advertiserClusterId, timeband, rateType, dow, restrictHrs,
# #                      spotPosition)
#             previousPriceFrame = pd.DataFrame(list(dbTimeBandPrecedence.find({'advertiserClusterId': advertiserClusterId, \
#                  'channel': channelName, 'rateType': rateType, 'dow': dow,  \
#                  'spotType': spotPosition})))
#             print('***Previous Price Frame')
#             #print(previousPriceFrame)
#             minRestrictHours = min(previousPriceFrame['restrictHours'])
#             print('minRestrictHours ',minRestrictHours)
#             previousPriceRec = previousPriceFrame[previousPriceFrame['restrictHours']==minRestrictHours]
#             print('previouPriceRec',previousPriceRec)
#             diffRestrictHours =  restrictHrs - minRestrictHours
#
#             if rateType == 'RODP' or rateType == 'ASR':
#                 deEscalationFactor = (1+deEscalationConstant)**diffRestrictHours
#             else:
#                 deEscalationFactor = 1
#
#             print ('****DeEscalation Factor**',deEscalationFactor)
#             virtualPreviousPrice = min(previousPriceRec['maxPrice']) * deEscalationFactor
#             print('****VirtualPrevios Price', virtualPreviousPrice,'minRestrictHours',minRestrictHours,'diffRestrictHours',diffRestrictHours)
#         except:
#             virtualPreviousPrice=None
#             # print("HEEEEEEEEY")
#             # #added for simulation(removed spot type for virtual price)
#             # previousPriceFrame = pd.DataFrame(list(dbTimeBandPrecedence.find({'advertiserClusterId': advertiserClusterId, 'timeBand': timeband, \
#             #                                     'channel': channelName, 'rateType': rateType, 'dow': dow})))
#             # previousPriceFrame.rename(columns={"spotType":"name"},inplace=True)
#             # print(previousPriceFrame["name"])
#             # print(spotTypeDF)
#             # mergedPremSpotDF = pd.merge(previousPriceFrame,spotTypeDF, left_on="name")
#             # mergedPremSpotDF = mergedPremSpotDF.fillna(0)
#             #
#             # mergedPremSpotDF["maxPrice"] = mergedPremSpotDF["maxPrice"]*(1-mergedPremSpotDF["premium"])
#             # mergedPremSpotDF["factor"] = mergedPremSpotDF["premium"] +1
#             #
#             # #if spotPosition =="" or spotPosition =="0":
#             #     #virtualPreviousPrice = mergedPremSpotDF.loc[0,"premium"]*mergedPremSpotDF.loc[0,"maxPrice"]
#             # #else:
#             #     #ind = spotTypeDF.index[spotTypeDF["name"]==spotPosition].tolist()[0]
#             # virtualPreviousPrice = mergedPremSpotDF.loc[0,"factor"]*mergedPremSpotDF.loc[0,"maxPrice"]
#             #
#             # print('****VirtualPrevios Price except',  virtualPreviousPrice)
#
#
#
#     return previousPrice, virtualPreviousPrice
#
#
#
# function gets the number of months of deal
def getNoOfMonthsDeal(dealStartDate, dealEndDate):
    diffDays = (dealEndDate - dealStartDate).days
    return diffDays / 30


# added for standalone channels
def getStandalonePrice(rateCardRecFrame, channelName, startDate, endDate, minFCT):
    months = getNoOfMonthsDeal(startDate, endDate)
    cond0 = months > rateCardRecFrame.fromMonth
    cond1 = months <= rateCardRecFrame.toMonth
    cond2 = minFCT >= rateCardRecFrame.minFCTFrom
    cond3 = minFCT <= rateCardRecFrame.minFCTTo
    rateCardFrame = rateCardRecFrame[cond0 & cond1 & cond2 & cond3]
    # rateCardFrameAgg = rateCardRecFrame.groupby(['channelName', 'rateType', 'timeBand','timebandType', 'dayOfWeek']).mean()
    return rateCardFrame


# added for sponsorship by ravinder

def getSponsorshipParameters(dbGrid, dealId, channelName):
    # print ('Find Grid',dealId,channelName)
    gridRec = dbGrid.find_one({'dealId': dealId})

    for i in range(0, len(gridRec['channel'])):

        if gridRec['channel'][i]['name'] == channelName:
            # added by ravinder for simulation handling nulls timebands
            gridTB = [x for x in gridRec['channel'][i]['sponsorship']['timebands'] if x is not None]

            dfSponsorshipTimeband = pd.DataFrame(list(gridTB))

    isSponsorship = False if dfSponsorshipTimeband.empty else True

    return dfSponsorshipTimeband, isSponsorship

# will not work for sponsorship
def getNewRCSponsorshipClosest(rateCardFrame, timeband, channelName, dow):
    timebandErrorValues = ['0', '0:00', '', '0:0', '00:00', np.nan]
    try:
        rateCardPrice = float(rateCardFrame.loc[(channelName, timeband, dow), "rate"])
    except:
        if timeband not in timebandErrorValues:
            startHr = getHourValue(timeband.split("-")[0])
            endHr = getHourValue(timeband.split("-")[1])
            rateCardFrame = rateCardFrame.reset_index()
            print("sssss", startHr, endHr)

            rateCardFrame["startHr"] = rateCardFrame["timeBand"].apply(lambda x: getHourValue(x.split("-")[0]))
            rateCardFrame["endHr"] = rateCardFrame["timeBand"].apply(lambda x: getHourValue(x.split("-")[1]))
            rateCardFrame = rateCardFrame[cond0]

            cond1 = rateCardFrame.startHr <= startHr
            cond2 = rateCardFrame.endHr > startHr

            cond4 = rateCardFrame.startHr >= startHr
            cond3 = rateCardFrame.endHr <= endHr
            cond5 = rateCardFrame.endHr >= endHr
            cond6 = rateCardFrame.startHr < endHr

            rateCardFrame["baseTB"] = timeband
            # tempDF = rateCardFrame[cond1&cond2]
            # tempdf2 = rateCardFrame[cond3&cond4]
            # print("tttt",tempDF)

            rateCardFrame = rateCardFrame[((cond1 & cond2) | (cond3 & cond4) | (cond4 & cond5 & cond6))]
            print("columns", rateCardFrame)

            rateCardFrame["overlapHrs"] = rateCardFrame.apply(
                lambda x: getOverlapNonOverlapHrs(x["baseTB"], x["timeBand"])[0], axis=1)
            rateCardFrame["nonOverlapHrs"] = rateCardFrame.apply(
                lambda x: getOverlapNonOverlapHrs(x["baseTB"], x["timeBand"])[1], axis=1)
            # rateCardFrame["product"] = rateCardFrame.apply(lambda x: x.overlapHrs * x.rate, axis=1)
            rateCardFrame = rateCardFrame[rateCardFrame["overlapHrs"] > 3]
            # print("rrrrrrrrrrrrrrr",rateCardFrame["timeBand"].tolist())

            # rateCardFrame.drop(columns=["startHr", "endHr", "rate"], axis=1, inplace=True)
            # rateCardFrameAgg = rateCardFrame.groupby(["channelName", "rateType", "dayOfWeek"]).sum()

            rateCardFrameAgg = rateCardFrame.groupby(["channelName", "rateType", "dayOfWeek"]).sum()
            non_overlap = float(rateCardFrameAgg.loc[(channelName, dow), "nonOverlapHrs"])
            # print("raaaaa",rateCardFrameAgg)

            rateCardPrice = float(rateCardFrameAgg.loc[(channelName, dow), "rate"])

        else:
            rateCardPrice = 1000

    return rateCardPrice


# added by ravinder for getting financial YEAR
def getFY(startDate):
    if startDate.month > 3:
        fy = str(startDate.year) + "-" + str(startDate.year + 1)[-2:]
    else:
        fy = str(startDate.year - 1) + "-" + str(startDate.year)[-2:]
    year = int(fy.split("-")[0])
    print("year", year,type(year))
    return fy, year


# sponsorship new rate card logic
def getSponsorshipRates(rateCardFrame, progTB, channelName, dow, rateType):
    print('in sponsorship rates function')
    timebandErrorValues = ['0', '0:00', '', '0:0', '00:00', np.nan]

    if progTB not in timebandErrorValues:
        print('in this if')
        startHr = getHourValue(progTB.split("-")[0])
        endHr = getHourValue(progTB.split("-")[1])
        print('start and end hour->', startHr, endHr)
        rateCardFrame = rateCardFrame.reset_index()
        rateCardFrame["startHr"] = rateCardFrame["timeBand"].apply(lambda x: getHourValue(x.split("-")[0]))
        rateCardFrame["endHr"] = rateCardFrame["timeBand"].apply(lambda x: getHourValue(x.split("-")[1]))
        cond0 = rateCardFrame.rateType == rateType
        rateCardFrame = rateCardFrame[cond0]

        cond1 = rateCardFrame.startHr <= startHr
        cond2 = rateCardFrame.endHr > startHr

        cond4 = rateCardFrame.startHr >= startHr
        cond3 = rateCardFrame.endHr <= endHr
        cond5 = rateCardFrame.endHr >= endHr
        cond6 = rateCardFrame.startHr <= endHr

        rateCardFrame["baseTB"] = progTB
        # tempDF = rateCardFrame[cond1&cond2]
        # tempdf2 = rateCardFrame[cond3&cond4]
        # print("tttt",tempDF)

        rateCardFrame = rateCardFrame[((cond1 & cond2) | (cond3 & cond4) | (cond4 & cond5 & cond6))]
        rateCardFrame["overlapHrs"] = rateCardFrame.apply(
            lambda x: getOverlapNonOverlapHrs(x["baseTB"], x["timeBand"])[0], axis=1)
        rateCardFrame["nonOverlapHrs"] = rateCardFrame.apply(
            lambda x: getOverlapNonOverlapHrs(x["baseTB"], x["timeBand"])[1], axis=1)
        # rateCardFrame["product"] = rateCardFrame.apply(lambda x : x.overlapHrs*x.rate,axis=1)
        # rateCardFrame.to_csv('C:\\Users\\Lenovo\\Desktop\\rateFrameFile1.csv')
        rateCardFrame.drop(columns=["startHr", "endHr"], axis=1, inplace=True)
        if rateCardFrame["overlapHrs"].sum() != 0:
            print('in this if')
            rateCardFrame = rateCardFrame[rateCardFrame["overlapHrs"] != 0]
            rateCardFrameAgg = rateCardFrame.groupby(["channelName", "dayOfWeek"]).mean()
        else:
            print('in else')
            rateCardFrameAgg = rateCardFrame.groupby(["channelName", "dayOfWeek"]).mean()

        # rateCardFrameAgg = rateCardFrame.groupby(["channelName","dayOfWeek"]).mean()
        print('RATE CARD FRAME 2->')
        print(rateCardFrameAgg)

        rateCardPrice = float(rateCardFrameAgg.loc[(channelName, dow), "rate"])
    else:
        rateCardPrice = 1000

    return rateCardPrice

# def getRoundOffTB(timeband):

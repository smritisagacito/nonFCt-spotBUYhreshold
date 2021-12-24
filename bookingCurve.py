#!/usr/bin/env python3
# author : ravinder
# objective : to create deal booking curve
import pandas as pd
import calendar
from pymongo import MongoClient
import pandas as pd
import sys
import math
from datetime import *
from collections import Counter


def getMongoConnection(mode):
    if mode == 'ABPDev':
        client = MongoClient("mongodb://abpuser:1MomentInTime@abpcluster-shard-00-00-iothc.mongodb.net:27017,abpcluster-shard-00-01-iothc.mongodb.net:27017,abpcluster-shard-00-02-iothc.mongodb.net:27017/admin?replicaSet=ABPCluster-shard-0&ssl=true")
        db = client["ABPDev"]
    elif mode == 'ABPUat':
        client = MongoClient("mongodb://abpnonproduser:Hell0Greta@abpnonprod-shard-00-00.fm59u.mongodb.net:27017,abpnonprod-shard-00-01.fm59u.mongodb.net:27017,abpnonprod-shard-00-02.fm59u.mongodb.net:27017/ABPUat?replicaSet=atlas-a2pokt-shard-0&ssl=true&authSource=admin")
        db = client["ABPUat"]
    elif mode == "ABPProd":
        client = MongoClient("mongodb://abpuser:1MomentInTime@abpcluster-shard-00-00-iothc.mongodb.net:27017,abpcluster-shard-00-01-iothc.mongodb.net:27017,abpcluster-shard-00-02-iothc.mongodb.net:27017/admin?replicaSet=ABPCluster-shard-0&ssl=true")
        db = client["ABPProd"]
    else:
        print("NO DATABSE FOUND,******* process failing")
        exit(1)
    return db

# funtion to return dates dataframe between 2 years


def getDateDF(year1, year2):
    #date1 = datetime.strptime(year1+'-02-28', '%Y-%m-%d').date()
    date1 = year1+"-04-01"
    #date2 = datetime.strptime(year2+'-03-31', '%Y-%m-%d').date()
    date2 = year2+"-03-31"
    datesDF = pd.DataFrame(pd.date_range(date1, date2).tolist())
    datesDF.rename(columns={0: "createdAt"}, inplace=True)
    return datesDF


def getMonthDate(inputDate):
    month = (str(inputDate.month) if len(str(inputDate.month))
             == 2 else "0" + str(inputDate.month))
    date = (str(inputDate.day) if len(str(inputDate.day))
            == 2 else "0" + str(inputDate.day))
    #monthYear = (str(inputDate.month) if len(str(inputDate.month)) == 2 else "0" + str(inputDate.month)) + "-" + "2019"
    return month+"-"+date


def getBaseDF(datesDF, distinctChannelDF, distinctRegionDF, distinctInventoryDF):
    finalDF1 = datesDF.assign(foo=1).merge(
        distinctChannelDF.assign(foo=1)).drop("foo", 1)
    finalDF2 = finalDF1.assign(foo=1).merge(
        distinctRegionDF.assign(foo=1)).drop("foo", 1)
    finalDF = finalDF2.assign(foo=1).merge(
        distinctInventoryDF.assign(foo=1)).drop("foo", 1)
    return finalDF


def getFY(startDate):
    if startDate == 0:
        return 0
    startMonth = int(startDate.split("-")[1])
    startYear = int(startDate.split("-")[0])
    if startMonth > 4:
        fy = startYear
    else:
        fy = startYear-1
    return fy


def getDealYearDays(startDate, endDate, createdAt):
    dateList = pd.date_range(startDate, endDate, freq='D').tolist()
    #yearList = [str(getFY(datetime.strftime(i,"%Y-%m-%d")))+"-"+getMonthDate(createdAt) for i in dateList]
    #yearList = [str((datetime.strftime(i,"%Y-%m-%d")).split("-")[0]) for i in dateList]
    yearList = [getFY(datetime.strftime(i, "%Y-%m-%d")) for i in dateList]
    dealYearDays = Counter(yearList)
    return dealYearDays


def safe_div(x, y):
    # print("billing",y)
    if y in [0, None]:
        return 0
    return round((x / y), 7)


def getAllocatedOutlay(counterObj, totalOutlay, totalDays):
    a = dict()
    if counterObj == 0:
        return 0
    for ele in counterObj:
        a[ele] = safe_div(totalOutlay*(counterObj[ele]), 10000000*totalDays)
    return a


def getBillingOutlay(counterObj, createdYear):
    # print(counterObj)
    if counterObj == 0:
        return 0
    else:
        try:
            x = counterObj[createdYear]
        except:
            x = 0.0
    return x


def getStartingPointDF(df, year):
    cumDF = df[df["createdYear"] < year]
    cumDF = cumDF[cumDF.apply(lambda x:False if x["outlayAllocated"]
                              == 0 else year in x["outlayAllocated"], axis=1)]
    cumDF["startingOutlay"] = cumDF.apply(
        lambda x: x["outlayAllocated"][year], axis=1)
    cumDF = cumDF[cumDF.apply(lambda x:False if x["consumedOutlayAllocated"]
                              == 0 else year in x["consumedOutlayAllocated"], axis=1)]
    cumDF["consumedStartingOutlay"] = cumDF.apply(
        lambda x: x["consumedOutlayAllocated"][year], axis=1)
    cumDF["createdYear"] = str(year)
    return cumDF


def main():
    # mode = sys.argv[1]
    #mode = "ABPDev"
    #mode = "ABPUat"
    mode = "ABPProd"

    db = getMongoConnection(mode)
    distinctInventoryDF = pd.DataFrame(
        list(db.dmrTxn.distinct("inventoryType")))
    distinctInventoryDF.rename(columns={0: "inventoryType"}, inplace=True)
    distinctRegionDF = pd.DataFrame(list(db.dmrTxn.distinct("region")))
    distinctRegionDF.rename(columns={0: "region"}, inplace=True)
    distinctChannelDF = pd.DataFrame(list(db.dmrTxn.distinct("channel")))
    distinctChannelDF.rename(columns={0: "channel"}, inplace=True)

    dmrTxnDF = pd.DataFrame(list(db.dmrTxn.find({}, {"_id": 0, "dealId": 1, "region": 1, "channel": 1, "createdAt": 1,
                                                     "fromDate": 1, "toDate": 1, "inventoryType": 1,
                                                     "totalOutlay": 1, "consumedOutlay": 1})))
    maxDate = dmrTxnDF["createdAt"].max()
    maxYear = maxDate.year
    datesDF = getDateDF("2015", str(maxYear + 1))
    print("max date is", maxDate)
    print("getting base DF......")
    baseDF = getBaseDF(datesDF, distinctChannelDF,
                       distinctRegionDF, distinctInventoryDF)
    dmrTxnDF["createdAt"] = dmrTxnDF["createdAt"].apply(lambda x: x.date())
    dmrTxnDF["createdAt"] = pd.to_datetime(dmrTxnDF["createdAt"])
    dmrTxnDF = dmrTxnDF.groupby(["region", "dealId", "channel", "inventoryType", "createdAt", "fromDate", "toDate"])[
        "totalOutlay", "consumedOutlay"].sum()
    dmrTxnDF = dmrTxnDF.reset_index()
    dmrTxnDF["totalDays"] = dmrTxnDF.apply(lambda x: (
        x["toDate"] - x["fromDate"]).days + 1, axis=1)
    dmrTxnDF["dealYearDays"] = dmrTxnDF.apply(lambda x: getDealYearDays(
        x["fromDate"], x["toDate"], x["createdAt"]), axis=1)

    finalDF = pd.merge(baseDF, dmrTxnDF, on=[
                       "createdAt", "channel", "region", "inventoryType"], how="left")
    finalDF.fillna(0, inplace=True)
    finalDF["createdYear"] = finalDF["createdAt"].apply(
        lambda x: str(getFY(datetime.strftime(x, "%Y-%m-%d"))))
    filteredDF = finalDF.copy()
    filteredDF["createdAtStr"] = filteredDF["createdAt"].apply(
        lambda x: datetime.strftime(x, "%Y-%m-%d"))

    filteredDF["createdYear"] = filteredDF.apply(
        lambda x: int(x["createdYear"]) + 1 if x["createdAt"].month == 4 else int(x["createdYear"]), axis=1)

    filteredDF["outlayAllocated"] = filteredDF.apply(
        lambda x: getAllocatedOutlay(x["dealYearDays"], x["totalOutlay"], x["totalDays"]), axis=1)

    filteredDF["billingOutlay"] = filteredDF.apply(
        lambda x: getBillingOutlay(x["outlayAllocated"], x["createdYear"]), axis=1)

    filteredDF["consumedOutlayAllocated"] = filteredDF.apply(
        lambda x: getAllocatedOutlay(x["dealYearDays"], x["consumedOutlay"], x["totalDays"]), axis=1)

    filteredDF["consumedBillingOutlay"] = filteredDF.apply(
        lambda x: getBillingOutlay(x["consumedOutlayAllocated"], x["createdYear"]), axis=1)

    yearList = [maxYear - 2, maxYear - 1, maxYear]

    # cum DF is the dataframe with starting points in booking curve (1 march)
    cumDF = pd.DataFrame()
    for i in yearList:
        print(i)
        cumDF = cumDF.append(getStartingPointDF(filteredDF, i))

    cumDF = cumDF.groupby(["createdYear", "channel", "region", "inventoryType"])[
        "startingOutlay", "consumedStartingOutlay"].sum().reset_index()
    cumDF["createdAtStr"] = cumDF.apply(
        lambda x: x["createdYear"] + "-04-01", axis=1)
    filteredDF = filteredDF.groupby(["createdAtStr", "createdAt", "createdYear", "channel", "inventoryType", "region"])[
        "billingOutlay", "consumedBillingOutlay"].sum()
    filteredDF = filteredDF.reset_index()
    bookingDF = filteredDF.copy()
    bookingDF["isForecast"] = bookingDF["createdAt"].apply(
        lambda x: True if x > maxDate else False)
    bookingDF["monthDay"] = bookingDF["createdAtStr"].apply(lambda x: x[5:])
    bookingDF.reset_index(inplace=True)

    # forecasted value
    print("forecasting the values....")
    pivotedDF = pd.pivot_table(bookingDF, values=["billingOutlay", "consumedBillingOutlay"], index=['monthDay', "channel", "region", "inventoryType"],
                               columns='createdYear').reset_index()

    pivotedDF["avgBilling"] = 1.1 * (pivotedDF["billingOutlay",
                                               maxYear - 1] + pivotedDF["billingOutlay", maxYear - 2]) / 2
    pivotedDF["consumedAvgBilling"] = 1.1 * \
        (pivotedDF["consumedBillingOutlay", maxYear - 1] +
         pivotedDF["consumedBillingOutlay", maxYear - 2]) / 2
    pivotedDF = pivotedDF[["monthDay", "channel", "region",
                           "inventoryType", "avgBilling", "consumedAvgBilling"]]

    forecastedDF = pd.merge(bookingDF, pivotedDF, on=[
                            "monthDay", "channel", "region", "inventoryType"], how="left")
    forecastedDF["billingOutlay"] = forecastedDF.apply(
        lambda x: x[("avgBilling", "")] if x["isForecast"] == True else x["billingOutlay"], axis=1)

    forecastedDF["consumedBillingOutlay"] = forecastedDF.apply(
        lambda x: x[("consumedAvgBilling", "")] if x["isForecast"] == True else x["consumedBillingOutlay"], axis=1)

    forecastedDF.drop(columns=["monthDay", ("avgBilling", ""),
                               ("consumedAvgBilling", "")], inplace=True)
    finalBookingDF = pd.merge(forecastedDF, cumDF, on=[
                              "createdAtStr", "channel", "region", "inventoryType"], how="left")
    finalBookingDF.fillna(0, inplace=True)
    finalBookingDF["startingOutlay"] = finalBookingDF.apply(
        lambda x: x["billingOutlay"] if x["startingOutlay"] == 0 else x["startingOutlay"], axis=1)
    finalBookingDF["consumedStartingOutlay"] = finalBookingDF.apply(
        lambda x: x["consumedBillingOutlay"] if x["consumedStartingOutlay"] == 0 else x["consumedStartingOutlay"], axis=1)
    finalBookingDF['cumSum'] = finalBookingDF.groupby(
        ['createdYear_x', "channel", "region", "inventoryType"])['startingOutlay'].cumsum()
    finalBookingDF['consumedCumSum'] = finalBookingDF.groupby(
        ['createdYear_x', "channel", "region", "inventoryType"])["consumedStartingOutlay"].cumsum()
    # loading in mongo
    # renaming columns
    finalBookingDF = finalBookingDF.rename(columns={"createdYear_x": "fy"})
    print(finalBookingDF)
    finalBookingDF = finalBookingDF.drop(
        ["createdYear_y", "startingOutlay", "consumedStartingOutlay", "index"], axis=1)
    db.dsBookingCurve.drop()
    print("loading in mongo....")
    # db.dsBookingCurve.insert_many(finalBookingDF.to_dict('records'))


# Calling main function
if __name__ == "__main__":
    main()

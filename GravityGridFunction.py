import pandas as pd
import ccxt
import json
import csv
import os
from os import environ
import time


apiKey = "eqg-7JV0"
secret = "lPj4ExgDKshIZE_7BlKAt-GDbKeZyZVPQQ--xzG00xg"
exchange = ccxt.deribit({'apiKey': apiKey ,'secret': secret,'enableRateLimit': True,"urls": {"api": "https://test.deribit.com"}})

pair = "BTC-PERPETUAL"
size_order = 10 # USD
types = 'limit'
side = 'buy'
price = 1000
postOnly =  False
reduceOnly = False
ioc = False
timestampUntil = 1615184598249

dfZone = pd.read_csv("ZoneTestCSV.csv")
dfTransaction = pd.read_csv("TransactionTestCSV.csv")

#ราคาตลาด (ต้องแก้ไข)
r1 = json.dumps(exchange.fetch_ticker('BTC-PERPETUAL'))
dataPriceBTC = json.loads(r1)
MarketPrice = dataPriceBTC['last']
print('BTC-PERPETUAL=',MarketPrice,'$')
#-------------------------------------------------------------------------------------------------------------------------

def dfMatchOrderListFN():
    dfMatchOrder1 = pd.DataFrame(exchange.fetchMyTrades(pair, timestampUntil, limit=1000),
                                 columns=['id', 'datetime', 'symbol', 'side', 'price', 'amount', "fee"])
    dfMatchOrderList1 = dfMatchOrder1.values.tolist()

    dfFee1 = pd.DataFrame(exchange.fetchMyTrades(pair, timestampUntil, limit=1000), columns=['fee'])
    dfFeeList1 = dfFee1.values.tolist()
    cost1 = []
    for i in range(len(dfFeeList1)):
        cost1.append(dfFeeList1[i][0]["cost"])  # ใน fee เอาแค่ cost

    for j in range(len(dfMatchOrderList1)):
        dfMatchOrderList1[j][6] = cost1[j]

    dfMatchOrderListA = dfMatchOrderList1
    return dfMatchOrderListA

def updateTransaction():
    checkId = []
    with open("TransactionTestCSV.csv", newline='') as f:
        reader = csv.reader(f)
        data = list(reader)
    for i in range(len(data)):
        checkId.append(data[i][0])

    for i in range(len(dfMatchOrderListFN())):
        MatchOrderId = dfMatchOrderListFN()[i][0]
        if MatchOrderId not in checkId:  # check id ที่ซ้ำกัน
            with open("TransactionTestCSV.csv", "a", newline='') as fp:
                wr = csv.writer(fp, dialect='excel')
                wr.writerow(dfMatchOrderListFN()[i])
                print("OrderUPDATE")
        elif MatchOrderId in checkId:
            pass

#-------------------------------------------------------------------------------------------------------------------------



def getsumExposure():
    with open('TransactionTestCSV.csv', newline='') as f:
        reader = csv.reader(f)
        data = list(reader)
    dfMyOrder = pd.DataFrame(exchange.fetchOpenOrders(pair, limit=1000),
                             columns=['id', 'timestamp', 'datetime', 'symbol', 'side', 'price', 'amount'])
    dfMyOrderList = dfMyOrder.values.tolist()

    sumExposure = []
    totalSellExposure = 0
    totalBuyExposure = 0
    countSellExposure = 0
    countBuyExposure = 0

    totalSellLimitExposure = 0
    totalBuyLimitExposure = 0
    countBuyLimitExposure = 0
    countSellLimitExposure = 0

    for i in range(len(data)):
        if data[i][3] == "sell":
            totalSellExposure += float(data[i][5])
            countSellExposure += 1
        elif data[i][3] == "buy":
            totalBuyExposure += float(data[i][5])
            countBuyExposure += 1

    for i in range(len(dfMyOrderList)):
        if dfMyOrderList[i][4] == "sell":
            totalSellLimitExposure += dfMyOrderList[i][6]
            countSellLimitExposure += 1
        elif dfMyOrderList[i][4] == "buy":
            totalBuyLimitExposure += dfMyOrderList[i][6]
            countBuyLimitExposure += 1

    sumExposureValue = totalBuyExposure - totalSellExposure
    sumExposurePending = totalBuyLimitExposure - totalSellLimitExposure
    allExposure = sumExposureValue + sumExposurePending

    # เก็บ DATA เอาไว้ใน LIST เพื่อดึงไปใช้งานใน FUNCTION อื่นๆ
    sumExposure.append(totalBuyExposure)  # sumExposure[0]
    sumExposure.append(countBuyExposure)  # sumExposure[1]
    sumExposure.append(totalSellExposure)  # sumExposure[2]
    sumExposure.append(countSellExposure)  # sumExposure[3]
    sumExposure.append(sumExposureValue)  # sumExposure[4]

    sumExposure.append(totalBuyLimitExposure)  # sumExposure[5]
    sumExposure.append(countBuyLimitExposure)  # sumExposure[6]
    sumExposure.append(totalSellLimitExposure)  # sumExposure[7]
    sumExposure.append(countSellLimitExposure)  # sumExposure[8]

    sumExposure.append(sumExposurePending)  # sumExposure[9]
    sumExposure.append(allExposure)  # sumExposure[10]

    return sumExposure


# -------------------------------------------------------------------------------------------------------------------------

def writeExposure():
    listqqq = [
        ["totalBuyExposure", "countBuyExposure", "totalSellExposure", "countSellExposure", "sumExposureValue",
         "totalBuyLimitExposure", "countBuyLimitExposure", "totalSellLimitExposure", "countSellLimitExposure",
         "sumExposurePending", "allExposure"], getsumExposure()]
    with open("ExposureCSV.csv", "w", newline='') as fp:
        wr = csv.writer(fp, dialect='excel')
        wr.writerows(listqqq)
#-------------------------------------------------------------------------------------------------------------------------


def buyAllZone(): # ซื้อรวบโซน รันครั้งแรกครั้งเดียว รันอีกจะซ้ำ และ update orderด้วย
    for i in range(1, len(dfZone)):
        if MarketPrice < dfZone.at[i, "zone"]:
            # Buy รวบโซนราคาตลาด และตั้ง TP ทีละตัว
            BuyMarket = exchange.create_order(pair, "market", "buy", size_order, price)
            print("Buy Market :", BuyMarket["price"], "$")
            BuyMarketTp = exchange.create_order(pair, "limit", "sell", size_order, dfZone.at[i - 1, "zone"])
            print("TP :", BuyMarketTp["price"], "$")

            # กรอกข้อมูลงใน ZoneSheet
            dfSellPendingId = pd.DataFrame(filter(lambda x: x['side'] == "sell", exchange.fetch_open_orders(pair,limit=1000)),
                                           columns=["id", "price"])
            s = dfSellPendingId[dfSellPendingId.price == dfZone.at[i - 1, "zone"]].id
            listA = list(s)
            sellLimitID = ''.join(listA)
            dfZone.loc[i, "buy.status"] = "buy"
            dfZone.loc[i, "sell.status"] = "sell.limit"
            dfZone.loc[i, "buy.limit.id"] = "none"
            dfZone.loc[i, "sell.limit.id"] = sellLimitID #เป็น str
            dfZone.to_csv("ZoneTestCSV.csv",index=False)
            print("Filled ZoneSheet")
        elif MarketPrice > dfZone.at[i, "zone"]:
            # Buy limit โซนล่าง
            BuyLimit = exchange.create_order(pair, "limit", "buy", size_order, dfZone.at[i, "zone"])
            print("Buy Limit :", BuyLimit["price"], "$")

            # กรอกข้อมูลงใน ZoneSheet
            dfBuyPendingId = pd.DataFrame(filter(lambda x: x['side'] == "buy", exchange.fetch_open_orders(pair,limit=1000)),
                                          columns=["id", "price"])
            b = dfBuyPendingId[dfBuyPendingId.price == dfZone.at[i, "zone"]].id
            listA = list(b)
            buyLimitId = ''.join(listA)
            dfZone.loc[i, "buy.status"] = "buy.limit"
            dfZone.loc[i, "sell.status"] = "none"
            dfZone.loc[i, "buy.limit.id"] = buyLimitId  #เป็น str
            dfZone.loc[i, "sell.limit.id"] = "none"
            dfZone.to_csv("ZoneTestCSV.csv",index=False)
            print("Filled ZoneSheet")

    # update Match ในTransactionSheet
    updateTransaction()
#-------------------------------------------------------------------------------------------------------------------------

def checkBuy():
    for i in range(1, len(dfZone)):
        if dfZone.loc[i, "buy.status"] == "buy":#check  buy.status ******
            Atom = 13
        elif dfZone.loc[i, "buy.status"] == "none": #check  buy.status ******
            #Check buylimit ซ้ำ
            d = pd.DataFrame(filter(lambda x: x['side'] == "buy", exchange.fetch_open_orders(pair,timestampUntil, limit=1000)),
                             columns=['price'])
            f = d.values.tolist()
            buyPendingCheck = [] #buy pending price เก็บใส่list เพื่อเช็ค
            for k in range(len(f)):
                buyPendingCheck.append(f[k][0])

            buyLimitzonePrice = float(dfZone.loc[i, "zone"])

            if buyLimitzonePrice not in buyPendingCheck:
                # Buy limit
                BuyLimit = exchange.create_order(pair, "limit", "buy", size_order, dfZone.at[i, "zone"])
                print("Buy Limit :", BuyLimit["price"], "$")
                # กรอกข้อมูลงใน ZoneSheet
                dfBuyPendingId = pd.DataFrame(filter(lambda x: x['side'] == "buy",
                                                     exchange.fetch_open_orders(pair, timestampUntil, limit=1000)),
                                              columns=["id", "price"])
                b = dfBuyPendingId[dfBuyPendingId.price == dfZone.at[i, "zone"]].id
                listA = list(b)
                buyLimitId = ''.join(listA)
                dfZone.loc[i, "buy.status"] = "buy.limit"
                dfZone.loc[i, "buy.limit.id"] = buyLimitId  # เป็น str
                dfZone.to_csv("ZoneTestCSV.csv", index=False)
                print("Filled ZoneSheet")
            elif buyLimitzonePrice in buyPendingCheck:
                pass




        elif dfZone.loc[i, "buy.status"] == "buy.limit": #check  buy.status ******
            b = pd.DataFrame(filter(lambda x: x['side'] == "buy", exchange.fetch_open_orders(pair,timestampUntil,limit=1000)), columns=["id"])
            c = b.values.tolist()
            buyLimitZoneId = dfZone.loc[i, "buy.limit.id"]
            buyLimitIdOrder = []
            for j in range(len(c)):
                buyLimitIdOrder.append(c[j][0])

            if buyLimitZoneId not in buyLimitIdOrder:  # ถ้า buylimit Match

                # Check selllimit ซ้ำ
                d = pd.DataFrame(filter(lambda x: x['side'] == "sell", exchange.fetch_open_orders(pair,timestampUntil,limit=1000)),
                                 columns=['price'])
                f = d.values.tolist()
                sellPendingCheck = []  # sell pending price เก็บใส่list เพื่อเช็ค
                for k in range(len(f)):
                    sellPendingCheck.append(f[k][0])

                sellLimitzonePrice = float(dfZone.loc[i - 1, "zone"])

                if sellLimitzonePrice not in sellPendingCheck:

                    # ตั้ง sell limit เป็น tp
                    sellLimitTp = exchange.create_order(pair, "limit", "sell", size_order, dfZone.at[i - 1, "zone"])
                    print("sell Limit :", sellLimitTp["price"], "$")

                    # กรอกข้อมูลงใน ZoneSheet
                    dfSellPendingId = pd.DataFrame(filter(lambda x: x['side'] == "sell",
                                                          exchange.fetch_open_orders(pair, timestampUntil,
                                                                                     limit=1000)),
                                                   columns=["id", "price"])
                    s = dfSellPendingId[dfSellPendingId.price == dfZone.at[i - 1, "zone"]].id
                    listA = list(s)
                    sellLimitID = ''.join(listA)
                    dfZone.loc[i, "buy.status"] = "buy"
                    dfZone.loc[i, "sell.status"] = "sell.limit"
                    dfZone.loc[i, "buy.limit.id"] = "none"
                    dfZone.loc[i, "sell.limit.id"] = sellLimitID  # เป็น str
                    dfZone.to_csv("ZoneTestCSV.csv", index=False)
                    print("Filled ZoneSheet")
                elif sellLimitzonePrice in sellPendingCheck:
                    pass
#-------------------------------------------------------------------------------------------------------------------------

def checkSell():
    for i in range(1, len(dfZone)):
        if dfZone.loc[i, "sell.status"] == "none":  # check sell.status ******
            pass
        elif dfZone.loc[i, "sell.status"] == "sell.limit": # check sell.status ******
            b = pd.DataFrame(filter(lambda x: x['side'] == "sell", exchange.fetch_open_orders(pair,timestampUntil,limit=1000)), columns=["id"])
            c = b.values.tolist()
            sellLimitZoneId = dfZone.loc[i, "sell.limit.id"]
            sellLimitIdOrder = []
            for j in range(len(c)):
                sellLimitIdOrder.append(c[j][0])

            if sellLimitZoneId not in sellLimitIdOrder: # ถ้า selllimit Match
                dfZone.loc[i, "buy.status"] = "none"
                dfZone.loc[i, "sell.status"] = "none"
                dfZone.loc[i, "sell.limit.id"] = "none"
                dfZone.to_csv("ZoneTestCSV.csv", index=False)
                print("Take ProFit ")
                print("Filled ZoneSheet")
#-------------------------------------------------------------------------------------------------------------------------
# while True:
#     try:
#         while True:
#             checkBuy()
#             checkSell()
#             updateTransaction()
#             writeExposure()
#             time.sleep(30)
#     except ccxt.NetworkError as e:
#         print(exchange.id, 'fetch_order_book failed due to a network error:', str(e))
#     except ccxt.ExchangeError as e:
#         print(exchange.id, 'fetch_order_book failed due to exchange error:', str(e))
#     finally:
#         print("continue")



while True:
    checkBuy()
    checkSell()
    updateTransaction()
    writeExposure()
    time.sleep(30)
            
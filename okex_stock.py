import time, datetime
import json, requests
import logging

from kafka import KafkaProducer

"""                                    *****BTC爆仓爬虫*****
当周未成交
status: 0    必须参数
contractId: 201806290000034   必须参数

次周未成交
status: 0
contractId: 201807060000013

季度未成交
status: 0
contractId: 201809280000012

最近七天已成交 (status:1)
"""
logging.basicConfig(filemode='okex_stock.log', level=logging.INFO)
status_ls = [0, 1]
contract_ls = [201806290000034, 201807060000013, 201809280000012]
base_url = 'https://www.okex.com/v2/futures/pc/public/blastingOrders.do'


# request连接
def request_con(url, data=None):
    header = {'content-type': 'application/x-www-form-urlencoded; charset=UTF-8'}
    con = requests.post(url, headers=header, data=data)
    return con.text


# kafka连接(topic: stock-dev)
def kafka_con():
    global producer
    producer = KafkaProducer(bootstrap_servers='47.75.116.175:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

"""                 返回的json如下所示：
{'msg': 'success',
 'code': 0, 
 'detailMsg': '', 
 'data': {
     'contractId': 201809280000012, 
     'futureContractOrdersList': [
         {'symbol': 0, 
          'amount': 1, 
          'modifyDate': 1529656405000, 
          'dealAmount': 0, 
          'btcAmount': 0.0157, 
          'type': 3, 
          'loss': '-0.0013', 
          'price': 6370.24, 
          'contractId': 201809280000012, 
          'systemType': 5, 
          'unitAmount': 100, 
          'contractName': None, 
          'orderFrom': 0, 
          'createDate': 1529656405000, 
          'status': 0
          },
        ...
"""
# 未成交循环爬取
i = 0
kafka_con()
while True:
    try:
        for st in status_ls:
            for _ in contract_ls:
                res = request_con(base_url, data={'status': st, 'contractId': _})
                detail = json.loads(res)['data']['futureContractOrdersList']
                # print(res)
                if detail is None:
                    continue
                else:
                    for d in detail:
                        dic = {
                            'coin': 'BTC',
                            'amount': d['amount'],
                            'modifyDate': d['modifyDate'],
                            'dealAmount': d['dealAmount'],
                            'btcAmount': d['btcAmount'],
                            'type': d['type'],
                            'loss': d['loss'],
                            'price': d['price'],
                            'contractId': d['contractId'],
                            'unitAmount': d['unitAmount'],
                            'createDate': d['createDate'],
                            'status': json.loads(res)['data']['status'],
                            'id': '{}'.format(d['createDate'])+'{}'.format(d['amount'])+'{}'.format(d['price']),
                        }
                        logging.info(dic)
                        producer.send('stock-dev', [dic])
        now = datetime.datetime.now()
        fTime = now.strftime("%Y-%m-%d %H:%M:%S")
        i += 1
        print("第%s次执行完毕----%s" % (i, fTime))
        logging.info("第%s次执行完毕----%s" % (i, fTime))
        time.sleep(20)
    except Exception as e:
        logging.info(e)
        print(e)



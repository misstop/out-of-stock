import time, datetime
import json, requests
import logging

from kafka import KafkaProducer
from collections import defaultdict
"""                                           *****OKEX爆仓爬虫*****
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

contract_ls 动态获取
contract_ls={
    BTC	当周	2018	0706	000	0013
    LTC	当周	2018	0706	001	0016
    ETH	当周	2018	0706	002	0042
    ETC	当周	2018	0706	004	0046
    BCH	当周	2018	0706	005	0052
    XRP	当周	2018	0706	015	0048
    EOS	当周	2018	0706	020	0054
    BTG	当周	2018	0706	010	0062
    BTC	次周	2018	0713	000	0034
    LTC	次周	2018	0713	001	0035
    ETH	次周	2018	0713	002	0060
    ETC	次周	2018	0713	004	0063
    BCH	次周	2018	0713	005	0065
    XRP	次周	2018	0713	015	0049
    EOS	次周	2018	0713	020	0057
    BTG	次周	2018	0713	010	0064
    BTC	季度	2018	0928	000	0012
    LTC	季度	2018	0928	001	0015
    ETH	季度	2018	0928	002	0041
    ETC	季度	2018	0928	004	0045
    BCH	季度	2018	0928	005	0051
    XRP	季度	2018	0928	015	0047
    EOS	季度	2018	0928	020	0053
    BTG	季度	2018	0928	010	0061
}
"""

# 币种和中间字符串的映射
maps = {
    'BTC': '000',
    'LTC': '001',
    'ETH': '002',
    'ETC': '004',
    'BCH': '005',
    'XRP': '015',
    'EOS': '020',
    'BTG': '010',
}
# log设置
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s (filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='okex_stock.log',
                    filemode='a')
# 成交/七天未成交
status_ls = [0, 1]
# contract_ls = [201806290000034, 201807060000013, 201809280000012]
# 获取contract的地址
contract_url = 'https://www.okex.com/v2/futures/pc/queryInfo/contractInfo.do'
# 获取爆仓数据的地址
detail_url = 'https://www.okex.com/v2/futures/pc/public/blastingOrders.do'


# 服务器时间13位
def cur_time():
    t1 = time.time()
    t2 = int(t1 * 1000)
    return t2


# request连接
def request_con(url, data=None):
    header = {'content-type': 'application/x-www-form-urlencoded; charset=UTF-8'}
    con = requests.post(url, headers=header, data=data)
    return con.text


# kafka连接(topic: stock-dev)
def kafka_con():
    global producer
    producer = KafkaProducer(
        # bootstrap_servers=['47.75.33.177:9092', '47.75.176.97:9092', '47.75.170.254:9092'],
        bootstrap_servers='47.75.116.175:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

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


# 获取contract_ls(post方式)
def get_contract():
    try:
        r1 = requests.post(contract_url)
        r2 = json.loads(r1.text)['data']
        contract_ls = []
        for contract in r2['contracts']:
            contract_ls.append(contract['id'])
        logging.info('已获取到%s' % contract_ls)
        print(contract_ls)
        return contract_ls
    except Exception as e:
        logging.info("抓不到contractID列表，错误为%s，时间戳--%s" % (e, cur_time()))
        print(e)


# 循环爬取
i = 0
kafka_con()
while True:
    try:
        contract_ls = get_contract()
        for st in status_ls:
            for _ in contract_ls:
                res = request_con(detail_url, data={'status': st, 'contractId': _, 'pageLength': 10000})
                detail = json.loads(res)['data']['futureContractOrdersList']
                # print(res)
                if detail is None:
                    logging.info("contractId为%s，status为%s的请求没有数据" % (_, st))
                    print("contractId为%s，status为%s的请求没有数据" % (_, st))
                    time.sleep(1)
                    continue
                else:
                    for d in detail:
                        v = str(d['contractId'])[8: 11]
                        dic = {
                            'coin': list(maps.keys())[list(maps.values()).index(v)],
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
                        #logging.info('插入一条数据%s' % dic)
                        #print('插入一条数据%s' % dic)
                        producer.send('stock-test', [dic])
                time.sleep(1)
        now = datetime.datetime.now()
        fTime = now.strftime("%Y-%m-%d %H:%M:%S")
        i += 1
        print("第%s次执行完毕----%s" % (i, fTime))
        logging.info("第%s次执行完毕----%s" % (i, fTime))
        time.sleep(20)
    except Exception as e:
        logging.info(e)
        print(e)
        kafka_con()
        continue



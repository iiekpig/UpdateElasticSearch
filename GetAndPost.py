# coding: utf-8
# 数据库备份

import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import sys
import time

#把a 复制 到 b

def DBa_to_DBb(host, port, indexA, indexB):

    esA = Elasticsearch(hosts=host, port=port, timeout=1000)
    rs = esA.search(index=indexA, scroll='1m', search_type='scan', body={"query": {"match_all": {}}})

    esB = Elasticsearch(hosts=host, port=port, timeout=1000)
    esB.indices.create(index=indexB, ignore=400)

    bodys = list()

    n = 0
    scroll_size = rs['hits']['total']
    print(scroll_size)
    scroll_id = rs['_scroll_id']
    print(rs)
    res = esA.scroll(scroll_id=scroll_id, scroll='1m')

    while (scroll_size >= 0):
        try:
            for doc in res['hits']['hits']:
                doc['_index'] = indexB
                bodys.append(doc)
                n += 1
            if (n == 3000):
                helpers.bulk(esB, bodys)
                bodys = []
                n = 0

            scroll_id = rs['_scroll_id']
            res = esA.scroll(scroll_id=scroll_id, scroll='1m')
            scroll_size = len(res['hits']['hits'])
        except:
            break

    for doc in res['hits']['hits']:
        doc['_index'] = indexB
        bodys.append(doc)
    if n != 0:
        helpers.bulk(esB, bodys)
    print('copy finished!')
    return


if __name__ == "__main__":
    time1 = time.time()
    DBa_to_DBb("192.168.120.90", 9200, "scholarkr-backpack", "zjp-index:scholarkr(1)")
    time2 = time.time()
    print(time2 - time1)

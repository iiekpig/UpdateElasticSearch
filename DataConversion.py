# 利用本地已经过处理的数据文件，实现对ES数据库的更新
# ES_Json.dat ： 更新ES数据库的文件
# GStoreTriple.dat：更新GStore数据库的文件
# author zjp 2016.5.5

import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import sys
import time


def completeID(jslist, pes, pindex, rid, simT):  # 补全实体的ID

    xlist = [dict(i) for i in jslist]
    _idlist = []

    for ii, item in enumerate(xlist):
        xx = '#ref:' + str(ii)
        _id = -1
        _name = item.get('name')
        xxr = ''

        if len(_name) < 1:
            continue

        rs = searchES(pes, pindex, _name)
        scroll_size = rs['hits']['total']

        _tpitem = dataformatter(item, pindex, False, '')  # 转化成你从ES中取出来的形式
        if scroll_size == 0:  # 说明这是条新纪录
            rid, _id, xxr = id_distribute(_tpitem, pindex, rid)  # 分配新id
        else:
            scroll_id = rs['_scroll_id']
            res = pes.scroll(scroll_id=scroll_id, scroll='1m')

            tflag = False  # 相似标记
            for doc in res['hits']['hits']:
                if compareData(_tpitem, doc) > simT:  # 比较两条json记录，有相似的
                    _id = doc['_id']
                    xxr = _id + '|' + doc['_source']['name']
                    tflag = True

            if tflag is False:
                rid, _id, xxr = id_distribute(_tpitem, pindex, rid)
        _idlist.append(_id)
        # print(jslist)
        jslist = update_jslist(jslist, xx, xxr)

    return rid, jslist, _idlist


def dataformatter(iitem, pindex, flag, iid):  # 转化成ES中的格式

    x = {}
    xitem = iitem
    # if flag == False:
    x['_type'] = xitem['_type']
    # print(xitem['_type'])
    x['_subtype'] = xitem['_subtype']
    x['_id'] = iid
    x['_index'] = pindex

    if flag is False and '_subtype' in x.keys():
        x['_subtype'] = xitem['_subtype']

    xitem.pop('_subtype')
    xitem.pop('_type')
    x['_source'] = xitem

    return x


def id_distribute(pitem, pindex, rid):  # 给每一个需要分配ID的item分配ID

    if len(pitem['_subtype']) > 0:
        tp_type = pitem['_subtype']
    else:
        tp_type = pitem['_type']

    _id_part = int(rid[tp_type])

    if pitem['_type'] != 'Pers':
        _id = pitem['_subtype'] + '_' + str(_id_part)
    else:
        _id = str(_id_part)

    tag = pitem['_type'] + '/' + _id + '|' + pitem['_source']['name']

    rid[tp_type] = _id_part + 1

    return rid, _id, tag


def update_jslist(pjslist, xx, xxr):  # 更新JSList，把本地ID替换为ES的ID
    # print(xx, xxr, pjslist)
    xlist = []
    for item in pjslist:
        for i in item:  # 如果是个list该怎么处理，如果是个str该怎么处理
            if isinstance(item[i], str):
                if item[i] == xx:
                    item[i] = xxr
            elif isinstance(item[i], list):
                for j in item[i]:
                    if j == xx:
                        item[i].remove(xx)
                        item[i].append(xxr)
        xlist.append(item)
    return xlist


def searchES(es, index, jname):
    rs = es.search(index=index, scroll='1m', search_type='scan', size=10000,
                   body={"query": {"match_phrase": {"name": {"query": jname}}}})
    return rs


def compareData(srcS, srcD):
    a = srcS['_source']  # 要上传的文件
    b = srcD['_source']  # 数据库中的文件
    ll = []

    flag = False  # 是否存在key相同，但value不相同的属性
    for i in a:
        attriflag = False

        for j in b:
            if i == j:  # a.key == b.key
                if isinstance(a[i], str) and isinstance(b[j], str):  # 如果a.value和b.value都是字符串
                    if a[i] != b[j] and a[i].find('#ref:') == -1 and b[j].find('#ref:') == -1 and a[i].find(
                            '|') == -1 and b[j].find('|') == -1:  # a.value != b.value
                        attriflag = True
                        break

        if attriflag is True:
            flag = True
            break

    if flag is True:  # 说明存在key相同，但value不相同的属性，是新纪录，存入本地
        return 0
    else:
        return 1


# 更新ES
def updateES(es, pindex, file, idl):
    bodys = list()
    for i, item in enumerate(file):
        x = dataformatter(item, pindex, True, idl[i])
        bodys.append(x)
    helpers.bulk(es, bodys)


# 更新Gstore
# def updateGStore():
def fetch_Gstore_triple(local, _xx, _ii):
    localappend = local
    return localappend


def updateDataBase(jslist, pes, pindex, rid, localTriple, simT):
    rid, xlist, _idlist = completeID(jslist, pes, pindex, rid, simT)  # 补全ID
    updateES(pes, pindex, xlist, _idlist)  # 更新ES
    fetch_Gstore_triple(localTriple, xlist, _idlist)  # 抽取三元组
    return rid


if __name__ == "__main__":
    time1 = time.time()

    p1 = 'data/idEachClass'
    p2 = 'data/test'
    host = '192.168.120.90'
    port = '9200'
    simT = 0.5
    pindex = 'test1'

    # 读数据记录文件
    rf = open(p1)
    ss = rf.readline()
    rid = json.loads(ss)

    # 建立与ES通信的对象
    es = Elasticsearch(hosts=host, port=port, timeout=1000)
    es.indices.create(index=pindex, ignore=400)

    ks = open(p2, encoding='utf8')
    tt = ks.readlines()

    localT = 'XXX'

    for ii in tt:
        xx = json.loads(ii)
        rid = updateDataBase(xx, es, pindex, rid, localT, simT)
        print('finish')

    f = open('data\idEachClass', 'w')
    f.writelines(json.dumps(rid))
    f.close()

    time2 = time.time()
    print(time2 - time1)

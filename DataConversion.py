# 利用本地已经过处理的数据文件，实现对ES数据库的更新
# ES_Json.dat ： 更新ES数据库的文件
# GStoreTriple.dat：更新GStore数据库的文件
# author zjp 2016.5.5

import json
import time

from elasticsearch import Elasticsearch
from elasticsearch import helpers


def completeID(jslist, pes, pindex, rid, simT):  # 补全实体的ID

    xlist = [dict(i) for i in jslist]
    _idlist = []
    _typel = []
    _type = ''
    for ii, item in enumerate(xlist):
        xx = '#ref:' + str(ii)
        _id = -1
        _name = item.get('name')
        xxr = ''



        addAttr = {}

        rs = searchES(pes, pindex, _name)
        scroll_size = rs['hits']['total']
        _tpitem = dataformatter(item, pindex, False, '')  # 转化成你从ES中取出来的形式
        if scroll_size == 0:  # 说明这是条新纪录
            rid, _id, xxr, _type = id_distribute(_tpitem, pindex, rid)  # 分配新id
        else:
            scroll_id = rs['_scroll_id']
            res = pes.scroll(scroll_id=scroll_id, scroll='1m')
            tflag = False  # 相似标记
            for doc in res['hits']['hits']:
                simV = compareData(_tpitem, doc)  # _tpitem是要上传的文件
                # print(simV)
                # print(_tpitem)
                if simV > simT:  # 比较两条json记录，有相似的

                    addAttr = findDif(_tpitem, doc)
                    _id = doc['_id']
                    _type = doc['_type']
                    xxr = _type + '/' + _id + '|' + doc['_source']['name']
                    tflag = True
                    break
                    # print('AAAAAAAAAAA')

            if tflag is False:
               # print(_tpitem)
                rid, _id, xxr, _type = id_distribute(_tpitem, pindex, rid)
        _idlist.append(_id)
        _typel.append(_type)
        jslist = update_jslist(jslist, xx, xxr, addAttr)


    return rid, jslist, _idlist, _typel


def findDif(srcS, srcD):
    a = srcS['_source']  # 要上传的文件
    b = srcD['_source']  # 数据库中的文件

    t = {}

    flag = False  # 是否存在key相同，但value不相同的属性
    for i in a:
        attriflag = False
        for j in b:
            if i == j:  # a.key == b.key
                attriflag = True
                break

        if attriflag is False:
            t[i] = a[i]

    return t


def dataformatter(iitem, pindex, flag, iid):  # 转化成ES中的格式

    x = {}
    xitem = iitem
    x['_type'] = xitem['_type']

    #print(x)


    if '_subtype' not in xitem:
        if x['_type'] == 'Org':
            xitem['_subtype'] = titleJudge(xitem['name'])
            #print(xitem['name'])
            #print(xitem['_subtype'])
        else:
            xitem['_subtype'] = ""

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

    #if len(pitem['_subtype']) > 0:
    #    tp_type = pitem['_subtype']
    #else:
    tp_type = pitem['_type']

    if pitem['_type'] == 'Org':
        _id_part = int(rid[pitem['_subtype']])
    else:
        _id_part = int(rid[tp_type])

    if pitem['_type'] == 'Org':
        _id = pitem['_subtype'] + '_' + str(_id_part)
    else:
        _id = str(_id_part)

    tag = pitem['_type'] + '/' + _id + '|' + pitem['_source']['name']

    #rid[tp_type] = _id_part + 1

    if pitem['_type'] == 'Org':
     #   _id_part = int(rid[pitem['_subtype']])
        rid[pitem['_subtype']] = _id_part + 1
    else:
      #  _id_part = int(rid[tp_type])
        rid[tp_type] = _id_part + 1

    return rid, _id, tag, tp_type


def update_jslist(pjslist, xx, xxr, ADD):  # 更新JSList，把本地ID替换为ES的ID
    xlist = pjslist

    for item in xlist:
        for i in item:  # 如果是个list该怎么处理，如果是个str该怎么处理
            if isinstance(item[i], str):
                if item[i] == xx:
                    item[i] = xxr
            elif isinstance(item[i], list):
                for j in item[i]:
                    if j == xx:
                        item[i].remove(xx)
                        item[i].append(xxr)

    # xList 要 ADD str
    return xlist


def searchES(es, index, jname):
    rs = es.search(index=index, scroll='1m', search_type='scan', size=10000,
                   body={"query": {"match_phrase": {"name": {"query": jname}}}})
    return rs


def compareData(srcS, srcD):
    a = srcS['_source']  # 要上传的文件
    b = srcD['_source']  # 数据库中的文件

    flag = False  # 是否存在key相同，但value不相同的属性
    for i in a:
        attriflag = False
        for j in b:
            if i == j:  # a.key == b.key
                if isinstance(a[i], str) and isinstance(b[j], str):  # 如果a.value和b.value都是字符串
                    if a[i] != b[j] and a[i].find('#ref:') == -1 and b[j].find('#ref:') == -1 and a[i].find(
                            '|') == -1 and b[j].find('|') == -1:  # a.value != b.value
                        attriflag = True

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
        #if item['name'] is None:
       #     continue
        x = dataformatter(item, pindex, True, idl[i])
        # es.update()
        # print(x)
        # es.update(index=x['_index'], doc_type=x['_type'], id=x['_id'], document=x)
        bodys.append(x)
    #print("bodys", bodys)
    helpers.bulk(es, bodys)
    return bodys
# 更新Gstore
# def updateGStore():
def fetch_Gstore_triple(_xx, _ii, _jj):
    rdf = []

    for i in range(0, len(_ii)):
        j = _xx[i]
        for item in j:
            if isinstance(j[item], str):
                _x = j[item].find('|')
                if _x != -1:
                    _j = j[item][0:_x].replace('<', '_D').replace('>', 'D_')
                    _ss = str('<ex:' + _jj[i] + '/' + _ii[i] + '>\t<ub:' + item + '>\t<ex:' + _j + '>.')
                else:
                    _j = j[item].replace('<', '_D').replace('>', 'D_')
                    _ss = str('<ex:' + _jj[i] + '/' + _ii[i] + '>\t<ub:' + item + '>\t<' + _j + '>.')
                if _ss not in rdf:
                    rdf.append(filterStr(_ss)) #<  &lt;  > &gt;
            else:
                for k in j[item]:
                    _x = k.find('|')
                    if _x != -1:
                        _j = k[0:_x].replace('<', '_D').replace('>', 'D_')
                        _ss = str('<ex:' + _jj[i] + '/' + _ii[i] + '>\t<ub:' + item + '>\t<ex:' + _j + '>.')
                    else:
                        _j = k.replace('<', '_D').replace('>', 'D_')
                        _ss = str('<ex:' + _jj[i] + '/' + _ii[i] + '>\t<ub:' + item + '>\t<' + _j + '>.')
                    if _ss not in rdf:
                        rdf.append(filterStr(_ss))

    return rdf


def updateDataBase(jslist, pes, pindex, rid, prdfs, simT):

   # time1 = time.time()
    rid, xlist, _idlist, _typelist = completeID(jslist, pes, pindex, rid, simT)  # 补全ID
   # time2 = time.time()
    updateES(pes, pindex, xlist, _idlist)  # 更新ES
   # print(rid)
   # time3 = time.time()
    _rr = fetch_Gstore_triple(xlist, _idlist, _typelist)  # 抽取三元组
   # time4 = time.time()

   # print(time2-time1, time3-time2, time4-time3)
    return rid, _rr


def dump_local(pridfile, pridpath, prdfsfile, prdfspath):
    """

    写本地文件，Gstore 的 rdf 文件

    """
    _f = open(prdfspath, 'a', encoding='utf8')
    for line in prdfsfile:
        _f.write(line + '\n')

    """

    写本地文件，rid 的文件

    """
    _f = open(pridpath, 'w', encoding='utf8')
    _f.writelines(json.dumps(pridfile))
    _f.close()

    return

def filterStr(srS):
    Ret = srS.replace('\n', '\\n')
    return Ret


def titleJudge(title):  # 通过标题断定类别
    if title is None:
        return None
    #print('dDDD', title)
    if title.find('公司') != -1 or title.find('集团') != -1 or title.find('厂') != -1:
        return 'Corporation'

    if title.find('大学') != -1:
        if title.find('研究室') != -1 or title.find('实验室')  != -1 or title.find('实验区') != -1:
            return 'Educational'

    x = title.find('(')
    y = title.find(')')


    if x != -1 and y != -1:
       # print(x, y)
        #print(title)
        title = title.replace(title[x: y+1], '')
        #print(title)

    if title.endswith('大学') != -1 or title.endswith('学校') != -1 or title.endswith('分校') != -1 or title.endswith('中学') != -1 or title.endswith(
            '中专') != -1 or title.endswith('学校') != -1 or title.endswith('学院') != -1 or title.endswith('科学院') != -1 or title.endswith(
            '图书馆') != -1 or title.endswith('中心校') != -1 or title.endswith('系') != -1 or title.endswith('学院') != -1:
        return 'Educational'
   # print('XXXXXXX', title)
    return 'Research'

if __name__ == "__main__":
    time1 = time.time()

    p1 = 'Data/idEachClass'
    p2 = 'Data/wanfang-1000.json'
    prdfsp = 'Data/localTriple.dat'

    host = '192.168.120.90'
    port = '9200'
    simT = 0.5
    pindex = 'scholarkr-backpack1'

    # 读数据记录文件
    rf = open(p1, encoding='utf8')
    ss = rf.readline()
    rid = json.loads(ss)

    rf1 = open(prdfsp, encoding='utf8')
    localrdfs = []
    for item in rf1:
        localrdfs.append(item)

    # 建立与ES通信的对象
    es = Elasticsearch(hosts=host, port=port, timeout=1000)
    es.indices.create(index=pindex, ignore=400)

    #ks = open(p2, encoding='utf8')
    #tt = ks.readlines()
    X = open('Data/WhichLine', encoding='utf8')
    T = X.readline()
    C = int(T)

    countFlag = 0
    for ii in open(p2, encoding='utf8'):

        countFlag = countFlag+1
        if countFlag >= C:
            xx = json.loads(ii)

            flag = False
            for jj in xx:
                if jj['name'] is None:
                    flag = True
                    break
            if flag is True:
                continue

            rid, localrdfs = updateDataBase(xx, es, pindex, rid, localrdfs, simT)
            dump_local(rid, p1, localrdfs, prdfsp)


            _f = open('Data/WhichLine', 'w', encoding='utf8')
            _f.writelines(str(countFlag))
            _f.close()


        print(countFlag)
        #
       # print(rid)
        #es.update()

      #  print('finish')

    time2 = time.time()
    print(time2 - time1)

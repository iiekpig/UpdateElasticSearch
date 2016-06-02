# 利用本地已经过处理的数据文件，实现对ES数据库的更新
# ES_Json.dat ： 更新ES数据库的文件
# GStoreTriple.dat：更新GStore数据库的文件
# author zjp 2016.5.5
# 版本号0.1

import json
import time
import os

from elasticsearch import Elasticsearch
from elasticsearch import helpers

"""
为每一项数据补全ID
"""
def completeID(jslist, pes, pindex, rid, simT):  # 补全实体的ID

    xlist = [dict(i) for i in jslist]

    _idlist = []
    _typel = []
    _type = ''
    for ii, item in enumerate(xlist):
        xx = '#ref:' + str(ii)  #原始串
        _id = -1
        _name = item.get('name')
        xxr = ''                #替换串

        addAttr = {}

        rs = searchES(pes, pindex, _name)
        scroll_size = rs['hits']['total']
        item = xlist[ii]
        _tpitem = dataformatter(item, pindex, False, '')  # 转化成你从ES中取出来的形式
        if scroll_size == 0:  # 说明这是条新纪录
            rid, _id, xxr, _type = id_distribute(_tpitem, pindex, rid)  # 分配新id
        else:
            scroll_id = rs['_scroll_id']
            res = pes.scroll(scroll_id=scroll_id, scroll='1m')
            tflag = False  # 相似标记
            for doc in res['hits']['hits']:
                simV = compare_data(_tpitem, doc)  # _tpitem是要上传的文件
                if simV == 1:  # 比较两条json记录，有相似的, （预防同名情况的发生）
                    addAttr = findDif(_tpitem, doc)
                    _id = doc['_id']
                    _type = doc['_type']
                    xxr = _type + '/' + _id + '|' + doc['_source']['name']
                    tflag = True
                    break

            if tflag is False:
                rid, _id, xxr, _type = id_distribute(_tpitem, pindex, rid)

        _idlist.append(_id)
        _typel.append(_type)
        #print('jslist1', jslist)

        #补全该项信息、去除值为空的属性

        for kk in addAttr:
            jslist[ii][kk] = addAttr[kk]  #这里有错
        #jslist[ii] = complete_addAttr(jslist[ii], addAttr)
        #print('jslist2', jslist)
        #解放军报

        for kk in [i for i in jslist[ii].keys()]:
            if jslist[ii][kk] == '':
                del(jslist[ii][kk])


        #字符串替换
        if 'research' in jslist[ii] and _type == 'Org':

           _subT = titleJudge(jslist[ii]['name'])

           if _subT == 'Educational':
                jslist[ii]['researchField'] = jslist[ii]['research']
           if _subT == 'Research':
                jslist[ii]['keyDiscipline'] = jslist[ii]['research']
           if _subT == 'Corporation':
                jslist[ii]['tradeField'] = jslist[ii]['research']
           jslist[ii].pop('research')



        if 'level' in jslist[ii] and jslist[ii]['level'] == '铭牌：':
            jslist[ii]['level'] = '公司'

        #print('jslist', jslist[ii])

        jslist = update_jslist(jslist, xx, xxr)
        xlist = update_jslist(xlist, xx, xxr)
        #print('jslist', jslist)

    return rid, jslist, _idlist, _typel


"""
找不同
srcS: 要上传的文件
srcD：数据库中的文件
"""
def findDif(srcS, srcD):
    a = srcS['_source']  # 要上传的文件
    b = srcD['_source']  # 数据库中的文件

   # print('a=', a)
   # print('b=', b)
    t = {}

    flag = False  # 是否存在key相同，但value不相同的属性
    t = b
    for i in a:
        if isinstance(a[i], str):
            if a[i].find('#ref') == -1:
                t[i] = a[i]
        elif isinstance(a[i], list):
            for j in a[i]:
                if isinstance(j, dict):
                    if j not in t[i]:
                        t[i].append(j)
                else:
                    if j.find('#ref') == -1 and j not in t[i]:
                        t[i].append(j)

    return t

"""
数据格式化
"""
def dataformatter(iitem, pindex, flag, iid):  # 转化成ES中的格式

    x = {}
    xitem = iitem
    #print(x, xitem)
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


def update_jslist(pjslist, xx, xxr):  # 更新JSList，把本地ID替换为ES的ID
    clist = pjslist
    #print(xx, xxr)
    for item in clist:
        for i in item:  # 如果是个list该怎么处理，如果是个str该怎么处理
            if isinstance(item[i], str):
                #print('xx', xxr)
                if item[i] == xx:
                    #print('xx', item[i])
                    item[i] = xxr
            elif isinstance(item[i], list):
                for j in item[i]:
                    if j == xx:
                        item[i].remove(xx)
                        item[i].append(xxr)
    return clist


def searchES(es, index, jname):

    rs = es.search(index=index, scroll='1m', search_type='scan', size=10000,
                   body={"query": {"match_phrase": {"name": {"query": jname}}}})
    return rs

"""
比较两项数据是否相同
"""
def compare_data(srcS, srcD):
    if srcS['_type'] != srcD['_type']: #如果类型都不一样，直接返回0
        return 0

    a = srcS['_source']  # 要上传的文件
    b = srcD['_source']  # 数据库中的文件
   # print('a.source, b.source', a, b)
    flag = False  # 是否存在key相同，但value不相同的属性
    for i in a:
        attriflag = False
        for j in b:
            if i == j:  # a.key == b.key
                if isinstance(a[i], str) and isinstance(b[j], str):  # 如果a.value和b.value都是字符串
                    if a[i] != b[j] and a[i].find('#ref:') == -1 and b[j].find('#ref:') == -1:  # a.value != b.value
                        attriflag = True
                        break

        if attriflag is True:
            flag = True
            break
   # print('a.source, b.source', a, b)
    if flag is True:  # 说明存在key相同，但value不相同的属性，是新纪录，存入本地
        return 0
    else:
        return 1


# 更新ES
def ES_format(es, pindex, file, idl):
    bodys = list()

    for i, item in enumerate(file):
        x = dataformatter(item, pindex, True, idl[i])
        bodys.append(x)

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
            elif isinstance(j[item], list):
                #print('j', j)
                #print('j[item]', j[item])

                for k in j[item]:
                    if isinstance(k, dict):
                        break
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
    rid, xlist, _idlist, _typelist = completeID(json.loads(jslist), pes, pindex, rid, simT)  # 补全ID
   # time2 = time.time()
    es_items = ES_format(pes, pindex, xlist, _idlist)  # 更新ES
    #print(rid)
    #time3 = time.time()
    _rr = fetch_Gstore_triple(xlist, _idlist, _typelist)  # 抽取三元组
    #time4 = time.time()

    #print(time2-time1, time3-time2, time4-time3)
    return rid, _rr, es_items


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
        if title.find('研究室') != -1 or title.find('实验室') != -1 or title.find('实验区') != -1:
            return 'Educational'

    x = title.find('(')
    y = title.find(')')


    if x != -1 and y != -1:
       # print(x, y)
        #print(title)
        title = title.replace(title[x: y+1], '')
        #print(title)

    if title.endswith('大学') \
            or title.endswith('学校') \
            or title.endswith('分校')  \
            or title.endswith('中学')  \
            or title.endswith('中专')  \
            or title.endswith('学校')  \
            or title.endswith('学院') \
            or title.endswith('科学院')  \
            or title.endswith('图书馆')  \
            or title.endswith('中心校')  \
            or title.endswith('系')  \
            or title.endswith('学院') :
        return 'Educational'
   # print('XXXXXXX', title)
    return 'Research'

def bulk_file(fileP, es, rid, localrdfs, threadHold, prdfsp, p1):

    bodys = list()
    for jj in open(fileP, encoding='utf8'):
        rid, localrdfs, es_its = updateDataBase(jj, es, pindex, rid, localrdfs, simT)
        for _b in es_its:
            bodys.append(_b)
            if len(bodys)%3000 == 0:
                helpers.bulk(es, bodys)
                dump_local(rid, p1, localrdfs, prdfsp)
    if len(bodys) > 0:
        helpers.bulk(es, bodys)
        dump_local(rid, p1, localrdfs, prdfsp)

    return

"""
为了不重名，把重名条目分散到不同文件中去
"""
def items_distribute(fileP):

    name_q = list()
    countFlag = -1

    mm = [-1 for i in range(900000)]

    time1 = time.time()
    for ii in open(fileP, encoding='utf8'):

        countFlag = countFlag+1
        xx = json.loads(ii)

        #name属性为空的不做处理
        flag = False                #name为空标记
        for jj in xx:
            if jj['name'] is None or jj['name'] == '' or jj['_type'] is None or jj['_type'] == '':
                flag = True
                break
        if flag is True:            #如果name为空，不做处理
            mm[countFlag] = -1
            continue

        name_set = set()
        #初始化队列
        if len(name_q) == 0:
            for jj in xx:
                name_set.add(jj['name'])

            name_q.append(name_set)
            mm[countFlag] = 0                      #这个今后要放入第0个文件

        else:                                      #入队列操作
            _t = -1
            k = -1

            for k,v in enumerate(name_q):          #名字队列集合
                flag = False
                for jj in xx:                      #判定条目的name
                    if jj['name'] in v:
                        flag = True
                        break
                if flag == False:
                    _t = k                         #第k条记录有名字重复的
                    break

            if _t == -1:                           #前面的名字都有重复的
                name_set.clear()
                for jj in xx:
                    name_set.add(jj['name'])
                mm[countFlag] = len(name_q)

                name_q.append(name_set)
            else:                                  #说明队列不满
                for jj in xx:
                    name_q[_t].add(jj['name'])
                mm[countFlag] = _t
    return mm

def write_item_inFile(fileP, mm):

    countFlag = -1
    for ii in open(fileP, encoding='utf8'):
        countFlag = countFlag+1
        if mm[countFlag] == -1:
            continue
        f = open('tmp_dat/'+str(mm[countFlag]), 'a')
        f.write(ii)
        f.close()
    return

if __name__ == "__main__":
    time1 = time.time()

    dir = 'finalData'
    p1 = dir+'/id_each_class'
    p2 = dir+'/cnki-dongfeng.json'
    prdfsp = dir+'/localTriple.dat'

    host = '192.168.120.90'
    port = '9200'
    simT = 0.5
    pindex = 'zjp-index:scholarkr(1)'

    #先把1文件分到若干个文件中去
    mm = items_distribute(p2)
    write_item_inFile(p2, mm)
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


    #遍历这若干个文件
    fileNum = len(os.listdir('tmp_dat'))
    print(fileNum)
    for ii in range(fileNum):
        fP = 'tmp_dat/'+str(ii)
        time1 = time.time()
        bulk_file(fP, es, rid, localrdfs, 3000, prdfsp, p1)
        time2 = time.time()
        if time2 - time1 < 2:
            time.sleep(2-time2+time1)
        print(ii, time2-time1)
        os.remove(fP)

    #删除生成的临时文件

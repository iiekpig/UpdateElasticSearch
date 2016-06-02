# coding: utf-8

"需安装 elasticsearch 的 python 模块"

"""
QUERY()  -查询or备份,写文件
CREATE()  -新建导入,读文件
INDEX()  -更新or备份还原(带原_id),读文件
UPDATE()  -添加字段,读文件,需要配置代码中action的'doc'字段
"""


from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json,time
#reload(sys)
#sys.setdefaultencoding('utf-8')


#查询，默认无查询条件下为备份，可选是否保留原id
def QUERY(host, port, index, type, file, query=None,id_saved = False,size=10000):
#可设置查询语句，如query={"query":{"match_phrase":{"topic": "信息管理"}}},
    es = Elasticsearch(hosts=host, port=port, timeout=1000)
    scanResp = helpers.scan(
        client=es,
        scroll="1m",
        timeout="10m",
        size=size,
        query=query,
        index=index,
        doc_type=type
        )

    num = 0
    with open(file, 'a', encoding='utf-8') as f:
        if id_saved:#保留_id
            for data in scanResp:
                data["_source"]["_id"] = data["_id"]
                f.write(json.dumps(data, ensure_ascii=False) + '\n')
                num += 1
                if num%size == 0:
                    print(num),
        else:#不保留_id
            for data in scanResp:
                f.write(json.dumps(data, ensure_ascii=False) + '\n')
                num += 1
                if num%size == 0:
                    print(num),
    print("\nSuccessful QUERY hits Num: %d" %num)



#新建，id可指定
def CREATE(host, port, index, type,file,size=10000):
    bulksize = size#每个批次的条数，最好5~15M大小,否则影响性能
    es = Elasticsearch(hosts=host, port=port, timeout=1000)
    actions = []
    num = 0
    with open(file,'r') as f:
        for doc in f:
            data = json.loads(doc.strip())#json格式字符串
            action = {
                    '_op_type': 'create',
                    '_index': index,
                    '_type': type,
                    '_id': None,#可以指定形式
                    "_source": json.dumps(data,ensure_ascii=False)#去掉_id后转成json格式
                    }

            actions.append(action)
            num += 1
            if num%bulksize == 0:
                helpers.bulk(es, actions)
                actions = []
                print(num),
        helpers.bulk(es, actions)
        print("\nSuccessful CREATE Num: %d" %num)


#更新or带原id复原
def INDEX(host, port, index, type,file,size=3000):
    bulksize = size #每个批次的条数，最好5~15M大小,否则影响性能
    es = Elasticsearch(hosts=host, port=port, timeout=1000)

    es.indices.create(index=index, ignore=400)

    actions = []
    num = 0
    with open(file,'r',encoding='utf-8') as f:
        for doc in f:
            data = json.loads(doc.strip())#json格式字符串
            data['_source'].pop('_id')
            data['_index'] = index
            #print(data)
            actions.append(data)
            num += 1
            if num%bulksize == 0:
                helpers.bulk(es, actions)
                actions = []
                print(num),
        helpers.bulk(es, actions)
        print("\nSuccessful INDEX Num: %d" %num)


#添加字段,需要配置代码中action的'doc'字段
def UPDATE(host, port, index,type, file, size=5000):
    bulksize = size#每个批次的条数，最好5~15M大小,否则影响性能
    es = Elasticsearch(hosts=host, port=port, timeout=1000)
    actions = []
    num = 0
    with open(file,'r') as f:
        for doc in f:
            data = json.loads(doc.strip())#json格式字符串
            action = {
                    '_op_type': 'update',
                    '_index': index,
                    '_type': type,
                    '_id': data["_id"],
                    'doc': {"topic" : data["topic"],
                            "topicscore": data["topicscore"]
                            }#根据数据添加
                    }
            actions.append(action)
            num += 1
            if num%bulksize == 0:
                helpers.bulk(es, actions)
                actions = []
                print(num)
        helpers.bulk(es, actions)#把剩余的零头写入
        print("\nSuccessful UPDATE Num: %d" %num)



if __name__ == "__main__":

    time_start = time.time()
    print("Time start:%s" %time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))#开始时间

#备份
    #QUERY("192.168.120.90", 9200, "scholarkr", "", "Data/data.dat",id_saved = True)
#查询
    #query = {"query":{"match_phrase":{"name": "中国科学院信息工程研究所"}}}
    #QUERY("192.168.120.90", 9200, "scholarkr","Org", "Data/data-2.dat",query=query)
#新建
    #CREATE("192.168.120.00", 9200, "test","Paper", "data.dat")
#导入
    #INDEX("192.168.120.90", 9200, "scholarkr","", "Data/data.dat")
#更新
    #UPDATE("192.168.120.00", 9200, "test","Paper", "data.dat")


    time_end = time.time()
    print("Time consumed:%s,(%f sec)" %(time.strftime('%Hh:%Mm:%Ss',time.gmtime(time_end-time_start)),time_end-time_start))#24小时内


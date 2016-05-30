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

if __name__ == "__main__":
    print(titleJudge('涿州市有色金属新技术开发中心'))
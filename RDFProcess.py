import json
import time

if __name__ == "__main__":


    time1 = time.time()
    inPath = 'Data/test'
    outPath = 'Data/Atest'

    rf1 = open(inPath)
    tt = rf1.readlines()

    localrdfs = set()

    for item in tt:
        x = item.strip()
        if x not in localrdfs:
            localrdfs.add(x)

    _f = open(outPath, 'a')
    for line in localrdfs:
        _f.write(line)

    print(len(localrdfs))

    time2 = time.time()
    print(time2 - time1)
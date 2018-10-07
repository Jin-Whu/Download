# -*- coding: utf-8 -*-
"""
Created on Fri Aug 17 11:17:01 2018

@author: zhangzheng
"""
import os
import datetime
import time

# 1.需要手动配置组合line97-111
# 2.dcb文件中的预览"Overview of the DCBs included in the file "
# 3.注意格式可能会变化，更换extractDCB()
# 4.遇到编码错误，删除该文件即可
# 5.用于提取CAS SINEX-BIAS格式文件

'''
def extractDCB(linelist, sys, prn, obs1, obs2):
    if obs1 == 'XXX' or obs2 == 'XXX':
        return 9999
    for i in linelist:
        if sys == i[11:12] and i[12:14] != '  ':
            if prn == int(i[12:14]):
                if obs1 == i[30:33] and obs2 == i[35:38]:
                    return float(i[71:92])
                elif obs2 == i[30:33] and obs1 == i[35:38]:
                    return -float(i[71:92])
    return 9999
'''


def extractDCB(linelist, sys, prn, obs1, obs2):
    if obs1 == 'XXX' or obs2 == 'XXX':
        return 9999
    for i in linelist:
        if sys == i[11:12] and i[12:14] != '  ':
            if prn == int(i[12:14]):
                if obs1 == i[25:28] and obs2 == i[30:33]:
                    return float(i[70:91])
                elif obs2 == i[25:28] and obs1 == i[30:33]:
                    return -float(i[70:91])
    return 9999


def extractDCBFromSNX(filePath, outPath, deleteOldFile):
    if filePath.endswith('.BSX'):
        cmnTime = datetime.datetime.strptime(filePath[-27:-20], '%Y%j')
        fp = open(filePath, "r", encoding='UTF-8')
        print(filePath)

        p1c1Path = outPath + '\P1C1' + cmnTime.strftime("%y%m%d") + '.DCB'
        fp1c1 = open(p1c1Path, 'w')
        p1p2Path = outPath + '\P1P2' + cmnTime.strftime("%y%m%d") + '.DCB'
        fp1p2 = open(p1p2Path, 'w')
        p1p3Path = outPath + '\P1P3' + cmnTime.strftime("%y%m%d") + '.DCB'
        fp1p3 = open(p1p3Path, 'w')

        fp1c1.write(
            'CAS\'S MONTHLY GNSS P1-C1 DCB SOLUTION, YEAR ' + cmnTime.strftime("%Y") + ', MONTH ' + cmnTime.strftime(
                "%m") + ' extract from ' + cmnTime.strftime("%Y%m%d") + '\n')
        fp1c1.write(
            '--------------------------------------------------------------------------------\n\nDIFFERENTIAL (P1-C1) CODE BIASES FOR SATELLITES AND RECEIVERS:\n\n')
        fp1c1.write(
            'PRN / STATION NAME        VALUE (NS)  RMS (NS)\n***   ****************    *****.***   *****.***\n')
        fp1p2.write(
            'CAS\'S MONTHLY GNSS P1-P2 DCB SOLUTION, YEAR ' + cmnTime.strftime("%Y") + ', MONTH ' + cmnTime.strftime(
                "%m") + ' extract from ' + cmnTime.strftime("%Y%m%d") + '\n')
        fp1p2.write(
            '--------------------------------------------------------------------------------\n\nDIFFERENTIAL (P1-P2) CODE BIASES FOR SATELLITES AND RECEIVERS:\n\n')
        fp1p2.write(
            'PRN / STATION NAME        VALUE (NS)  RMS (NS)\n***   ****************    *****.***   *****.***\n')
        fp1p3.write(
            'CAS\'S MONTHLY GNSS P1-P3 DCB SOLUTION, YEAR ' + cmnTime.strftime("%Y") + ', MONTH ' + cmnTime.strftime(
                "%m") + ' extract from ' + cmnTime.strftime("%Y%m%d") + '\n')
        fp1p3.write(
            '--------------------------------------------------------------------------------\n\nDIFFERENTIAL (P1-P3) CODE BIASES FOR SATELLITES AND RECEIVERS:\n\n')
        fp1p3.write(
            'PRN / STATION NAME        VALUE (NS)  RMS (NS)\n***   ****************    *****.***   *****.***\n')

        _linelist = fp.readlines()
        linelist = []
        for j in range(0, len(_linelist)):
            if _linelist[j][1:4] == 'DCB' or _linelist[j][1:4] == 'DSB':
                linelist.append(_linelist[j])

        for j in ['G', 'R', 'E', 'C']:
            for k in range(1, 36):
                if j == 'G':
                    p1c1Value = extractDCB(linelist, j, k, 'C1W', 'C1C')
                    p1p2Value = extractDCB(linelist, j, k, 'C1W', 'C2W')
                    p1p3Value = extractDCB(linelist, j, k, 'C1W', 'C1C') + extractDCB(linelist, j, k, 'C1C', 'C5Q')
                elif j == 'R':
                    p1c1Value = extractDCB(linelist, j, k, 'C1P', 'C1C')
                    p1p2Value = extractDCB(linelist, j, k, 'C1P', 'C2P')
                    p1p3Value = extractDCB(linelist, j, k, 'C1P', 'XXX')
                elif j == 'E':
                    p1c1Value = extractDCB(linelist, j, k, 'C1C', 'XXX')
                    p1p2Value = extractDCB(linelist, j, k, 'C1C', 'C5Q')
                    p1p3Value = extractDCB(linelist, j, k, 'C1C', 'C7Q')
                elif j == 'C':
                    p1c1Value = extractDCB(linelist, j, k, 'C2I', 'XXX')
                    p1p2Value = extractDCB(linelist, j, k, 'C2I', 'C7I')
                    p1p3Value = extractDCB(linelist, j, k, 'C2I', 'C6I')

                if p1c1Value <= 999 and p1c1Value >= -999:
                    line = (j + '%02d                       %9.3f       0.000\n') % (k, p1c1Value)
                    fp1c1.write(line)
                if p1p2Value <= 999 and p1p2Value >= -999:
                    line = (j + '%02d                       %9.3f       0.000\n') % (k, p1p2Value)
                    fp1p2.write(line)
                if p1p3Value <= 999 and p1p3Value >= -999:
                    line = (j + '%02d                       %9.3f       0.000\n') % (k, p1p3Value)
                    fp1p3.write(line)

        fp.close()
        fp1c1.close()
        fp1p2.close()
        fp1p3.close()
        if deleteOldFile:
            os.remove(filePath)


def process(inPath, outPath, deleteOldFile):
    fileList = os.listdir(inPath)
    countFile = 0
    for i in range(0, len(fileList)):
        countFile += 1
        print(("[Finished:%.5f]" % (countFile / len(fileList))))
        filePath = os.path.join(inPath, fileList[i])
        extractDCBFromSNX(filePath, outPath, deleteOldFile)


if __name__ == '__main__':
    print('1.需要手动配置组合line97-111')
    print('2.dcb文件中的预览"Overview of the DCBs included in the file "')
    print('3.注意格式可能会变化，更换extractDCB()')
    print('4.遇到编码错误，删除该文件即可')
    print('5.用于提取CAS SINEX-BIAS格式文件')
    time.sleep(5)
    inPath = 'E:\\FTP\\products\\dcb\\cas'
    outPath = 'E:\\FTP\\products\\dcb\\cas'
    deleteOldFile = 1
    process(inPath, outPath, deleteOldFile)

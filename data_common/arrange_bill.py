'''
Author: your name
Date: 2020-09-28 10:49:59
LastEditTime: 2020-09-28 16:11:19
LastEditors: Please set LastEditors
Description: In User Settings Edit
FilePath: /work/我的工作/薪资/code/arrange_bill.py
'''
import pandas as pd
import os 
import shutil

print('{0} arranging file {0}'.format('*'*20))
# path='./代理商代码.xlsx'

city_code = pd.read_excel('./代理商代码.xlsx')
city_dict = dict()
for i, c in zip(city_code['加盟商ID'], city_code['城市']):
    city_dict[i] = c


path = '.'

file_list = os.listdir(path)

bill_list = [x for x in file_list if '配送费' in x]

for i in bill_list:
    city_code = i.split('-')[0]
    dst_path = path+'/分类'+'/{}'.format(city_dict[city_code])
    os.makedirs(dst_path)
    shutil.copyfile(i, dst_path)

print('{0} concating {0}'.format('*'*20))

def get_file_path(path):
    roots = []
    dirs = []
    files = []

    for r, d, f in os.walk(path):
        roots.append(r)
        dirs.append(d)
        files.append(f)
    return roots, dirs, files

roots, dirs, files = get_file_path(path+'/分类')

vars = locals()
for d
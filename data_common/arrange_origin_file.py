'''
Author: your name
Date: 2020-09-20 20:57:28
LastEditTime: 2020-09-25 00:12:06
LastEditors: Please set LastEditors
Description: In User Settings Edit
FilePath: /work/我的工作/薪资/饿了么取数需求/数据源/8月月账单/arrange_file.py
'''
import os
import shutil
import pandas as pd

# path = "/Users/admin/Documents/work/我的工作/薪资/饿了么取数需求/数据源/8月月账单"
# 读取目录下的文件和文件夹


def get_file_path(path):
    roots = []
    dirs = []
    files = []

    for r, d, f in os.walk(path):
        roots.append(r)
        dirs.append(d)
        files.append(f)
    return roots, dirs, files

# roots, dirs, files = get_file_path(path)

# 创建城市目录


def make_dirs(path, city_dirs, file_dirs):
    for c in city_dirs:
        for f in file_dirs:
            dest_dir = path+"/test/{}/{}".format(c, f)
            os.makedirs(dest_dir)
# make_dirs(path, dirs[1], dirs[0])


# 获取源目录和目标目录
def get_move_path(path, dirs):
    src_dir = []
    for i in dirs[0]:
        path_temp = os.path.join(path, i)
        city_list = os.listdir(path_temp)
        for c in city_list:
            dir2 = os.path.join(path_temp, c)
            if not os.path.isfile(dir2):
                file = os.listdir(dir2)
                for f in file:
                    file_path = os.path.join(dir2, f)
                    # print(file_path)
                    src_dir.append(file_path)
            else:
                src_dir.append(dir2)

    move_dict = dict()
    for i in src_dir:
        path_list = i.split('/')
        city = path_list[-2]
        dir_name = path_list[-3]
        file_name = path_list[-1]
        dst_file = os.path.join(path, '分类', city, dir_name, file_name)
        dst_path = os.path.join(path, '分类', city, dir_name)
        os.makedirs(dst_path, exist_ok=True)
        move_dict[i] = dst_file
        # print(dst_path)
    return move_dict

# move_dict = get_move_path(path, dirs)

# 删除不需要的列名


def remove_cols(file_path, rm_cols):
    df = pd.read_csv(file_path)
    retain_col = list(set(df.columns) - set(rm_cols))
    df_res = df[retain_col].copy()
    return df_res

# 移动文件


def move_file(move_dict, modify_df=None, rm_cols=None):
    for i in move_dict:
        print("处理:{}".format(i))
        if ".DS_Store" in i:
            pass
        if modify_df in i:
            # file_path = [x for x in list(move_dict.keys()) if mondiy_df in x]
            print("删除列:{}".format(i))
            df = remove_cols(i, rm_cols)
            df = df.applymap(lambda x: str(x))
            df.to_csv(move_dict[i])
        else:
            shutil.copyfile(i, move_dict[i])

# move_file(move_dict)
# print('{}over{}'.format('*'*20, '*'*20 ))

def remove_cols(file_path, rm_cols):
    df=pd.read_csv(file_path)
    retain_col=list(set(df.columns) - set(rm_cols))
    df_res=df[retain_col].copy()
    return df_res

if __name__ == '__main__':
    path='.'
    roots, dirs, files=get_file_path(path)
    move_dict=get_move_path(path, dirs)
    modify_df = '运单数据'
    rm_cols = ['商户ID', '流水号', '运单类型', '运单来源', '运单标签', '是否转单', '物流时长', '预计出餐时间', '实际出餐时间', '骑手到店时间', '骑手取餐时间', '运单取消时间', '用户T+8超时状态', '取消发起方', '取消原因', '取消责任方', '违规到店状态', '违规到店申诉状态', '违规取餐状态',
        '违规取餐申诉状态', '违规送达状态', '违规送达申诉状态', '用户投诉状态', '用户投诉责任方', '用户投诉申诉状态', '商户投诉状态', '商户投诉责任方', '是否T+2商户投诉', '商户投诉申诉状态', '商户评价状态', '商户评价责任方', '是否T+2商户评价', '商户评价标签', '商户评价申诉状态', '索赔状态', '是否欺诈']
    move_file(move_dict, modify_df, rm_cols)
    print('{0} over {0}'.format('*'*20))

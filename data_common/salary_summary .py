import pandas as pd
import os
import gc
# import modin.pandas as pd
import re
# qplus_dc = pd.read_csv()


def get_file_path(path):
    roots = []
    dirs = []
    files = []
    for r, d, f in os.walk(path):
        roots.append(r)
        dirs.append(d)
        files.append(f)
    return roots, dirs, files


path = '.'
roots, dirs, files = get_file_path(path)


# 读取文件
def get_file(df=None, dir_path=None, file=None):
    if df:
        df = df
    else:
        file_path = dir_path + '/' + file
        df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
    return df

def clear_data(df):
    df = df.applymap(lambda x: str(x).strip('"'))
    df = df.applymap(lambda x: str(x).strip('='))
    df = df.applymap(lambda x: str(x).strip('"'))
    df = df.applymap(lambda x: str(x).strip('——'))
    if '团队ID' in df.columns:
        df = df.rename(columns={"团队ID": "站点ID"})
    if '骑手id' in df.columns:
        df = df.rename(columns={"骑手id": "骑手ID"})        
    return df


# 统计完成胆量
def complete_order_select(df):
    df = df[(df['业务类型'] == '配送收入')]
    df = df[df['详情'].isin(['差评', '完成单', '完成单-超时'])]
    # df.to_csv(dir_path + '/' + 'complete_order.csv', encoding='utf_8_sig', index=False)
    return df


# time to int
def time_int(x):
    a = x.split(':')[:-1]
    b = int(''.join(a))
    return b

# 时间段
def order_time(complete_df, dir_path=None):
    global qplus_dc, info_col, info
    df = complete_df.copy()
    df['站点ID'] = df['站点ID'].astype('str')
    df['骑手ID'] = df['骑手ID'].astype('str')
    # 处理业务交易时间
    df['时间'] = df['业务交易时间'].str.split(' ', expand=True)[1].copy()
    df['业务交易时间_time']  = df['时间'].apply(time_int).copy()
    # bins = [-1, 29, 59, 129, 159, 229, 259, 329, 359, 429, 459, 529, 559, 629, 659, 729, 759, 829, 859, 929, 959, 1029,
    #         1059, 1129, 1159, 1229, 1259, 1329, 1359, 1429, 1459, 1529, 1559, 1629, 1659, 1729, 1759, 1829, 1859, 1929,
    #         1959, 2029, 2059, 2129, 2159, 2229, 2259, 2329, 2360]
    # labels = [u'0-29', u'29-59', u'100-129', u'130-159', u'200-229', u'230-259', u'300-329', u'330-359', u'400-429',
    #           u'430-459', u'500-529', u'530-559', u'600-629', u'630-659', u'700-729', u'730-759', u'800-829',
    #           u'830-859', u'900-929', u'930-959', u'1000-1029', u'1030-1059', u'1100-1129', u'1130-1159', u'1200-1229',
    #           u'1230-1259', u'1300-1329', u'1330-1359', u'1400-1429', u'1430-1459', u'1500-1529', u'1530-1559',
    #           u'1600-1629', u'1630-1659', u'1700-1729', u'1730-1759', u'1800-1829', u'1830-1859', u'1900-1929',
    #           u'1930-1959', u'2000-2029', u'2030-2059', u'2100-2129', u'2130-2129', u'2200-2229', u'2230-2259',
    #           u'2300-2329', u'2330-2359']
    df[u'time_segment_no']  = (df['业务交易时间_time'] % 100 + df['业务交易时间_time'] // 100 * 60) // 30 + 1
    labels = {
            1:u'00:00-00:30', 2:u'00:30-01:00', 3:u'01:00-01:30', 4:u'01:30-02:00', 5:u'02:00-02:30', 6:u'02:30-03:00', 
            7:u'03:00-03:30', 8:u'03:30-04:00', 9:u'04:00-04:30', 10:u'04:30-05:00', 11:u'05:00-05:30', 12:u'05:30-06:00', 
            13:u'06:00-06:30', 14:u'06:30-07:00', 15:u'07:00-07:30', 16:u'07:30-08:00', 17:u'08:00-08:30', 18:u'08:30-09:00', 
            19:u'09:00-09:30', 20:u'09:30-10:00', 21:u'10:00-10:30', 22:u'10:30-11:00', 23:u'11:00-11:30', 24:u'11:30-12:00', 
            25:u'12:00-12:30', 26:u'12:30-13:00', 27:u'13:00-13:30', 28:u'13:30-14:00', 29:u'14:00-14:30', 30:u'14:30-15:00', 
            31:u'15:00-15:30', 32:u'15:30-16:00', 33:u'16:00-16:30', 34:u'16:30-17:00', 35:u'17:00-17:30', 36:u'17:30-18:00', 
            37:u'18:00-18:30', 38:u'18:30-19:00', 39:u'19:00-19:30', 40:u'19:30-20:00', 41:u'20:00-20:30', 42:u'20:30-21:00', 
            43:u'21:00-21:30', 44:u'21:30-22:00', 45:u'22:00-22:30', 46:u'22:30-23:00', 47:u'23:00-23:30', 48:u'23:30-00:00'}
    df[u'时间段'] = df[u'time_segment_no'].apply(lambda k: labels.get(k))
        # df[u'时间段'] = pd.cut(df[u'业务交易时间_time'], bins=bins, labels=labels)
    # df[[u'起始时间点', u'终止时间点']] = df[u'时间段'].str.split('-', expand=True)
    res = df.groupby(['骑手ID', '站点ID', '时间段'])['运单号'].agg('count').reset_index()
    res = res.rename(columns={'运单号': '该时段内完成的单量'})
    res_info = pd.merge(res, qplus_dc, how='left', on='站点ID')
    res_info[['骑手ID', '站点ID']] = res_info[['骑手ID', '站点ID']].applymap(lambda x: str(x))
    col_arrange = ['骑手ID','站点ID','BOSS主体ID','商圈名称','BOSS城市代码','BOSS商圈ID','城市',
                   '主体','时间段','该时段内完成的单量']
    res_info = res_info[col_arrange]
    res_info.to_excel(dir_path + '/' + '饿了么时段单量.xlsx', encoding='utf_8_sig', index=False, engine='xlsxwriter')
    for i in locals().keys():
        del locals()[i]
    gc.collect()
    print('处理完成：{}'.format('时段单量'))


# 完成单量及补贴
def order_num_allowance_daily(complete_order_df, dir_path):
    global qplus_dc, info_col, info
    df = complete_order_df.copy()
    df['站点ID'] = df['站点ID'].astype('str').copy()
    df['骑手ID'] = df['骑手ID'].astype('str').copy()
    order_num = df.groupby(['骑手ID', '站点ID'])['运单号'].agg('count').reset_index()
    allowance = df.groupby(['骑手ID', '站点ID'])['时段补贴', '距离补贴', '重量补贴'].agg('sum').reset_index()
    work_df = df[['骑手ID', '站点ID','账单时间']].drop_duplicates()
    work_day = work_df.groupby(['骑手ID', '站点ID'])['账单时间'].agg('count').reset_index()
    df_merge_1 = pd.merge(order_num, allowance, how='left', on=['骑手ID', '站点ID'])
    res = pd.merge(df_merge_1, work_day, how='left', on=['骑手ID', '站点ID'])
    res_info = pd.merge(res, qplus_dc, how='left', on='站点ID')
    res_info = res_info.rename(columns={'账单时间':'出勤天数', '运单号':'完成单量'})
    res_info = res_info.rename(columns={"运单号": "完成单量"})
    col_arrange = ['站点ID','BOSS商圈ID','商圈名称','BOSS城市代码','城市','BOSS主体ID',
                   '主体','骑手ID','时段补贴','距离补贴','重量补贴','完成单量','出勤天数']
    res_info = res_info[col_arrange]
    res_info[['骑手ID', '站点ID']] = res_info[['骑手ID', '站点ID']].applymap(lambda x: str(x))
    res_info.to_excel(dir_path + '/' + '饿了么完成单量.xlsx', encoding='utf_8_sig', index=False, engine='xlsxwriter')
    for i in locals().keys():
        del locals()[i]
    gc.collect()
    print('处理完成：{}'.format('完成单量及补贴'))


# 好评单
def good_order(delivery_order, comment_df, save_path):
    global qplus_dc, info_col, info
#     comment_df['站点ID'] = comment_df['站点ID'].astype('str')
#     comment_df['运单号'] = comment_df['运单号'].astype('str')
#     delivery_order = clear_data(delivery_order)
    comment_df = comment_df[comment_df['评价等级'] == '非常满意'].rename(columns={'评价等级': '评价等级_服务奖惩'}).copy()
    comment_df['服务奖惩_评价等级_非常满意'] = 1
    good_delivery = delivery_order[delivery_order['用户评价状态'] == '非常满意'].rename(columns={'用户评价状态': '用户评价状态_运单详情'}).copy()
    good_delivery['运单详情_用户评价状态_非常满意'] = 1
    df = pd.merge(good_delivery, comment_df, how='right', on=['运单号'])
    df[u'送达时间_dt'] = pd.to_datetime(df[u'运单送达时间'])
    df[u'评价时间_dt'] = pd.to_datetime(df[u'评价日期'])
    df[u'评价间隔_dt'] = df[u'评价时间_dt'] - df[u'送达时间_dt']
    df[u'评价间隔_res'] = df[u'评价间隔_dt'].dt.days * 24 + df[u'评价间隔_dt'].dt.seconds / 3600
    res_info = pd.merge(df, qplus_dc, how='left', on='站点ID')
    col_arrange = ['站点ID','BOSS商圈ID','商圈名称','BOSS城市代码','城市','BOSS主体ID','主体','骑手ID','运单送达时间','运单号','用户评价状态_运单详情','运单详情_用户评价状态_非常满意',
               '骑手信息','评价等级_服务奖惩','评价来源','评价日期','服务奖惩_评价等级_非常满意','评价间隔_dt','评价间隔_res']
    res_info = res_info[col_arrange]
    res_info[info_col] = res_info[info_col].applymap(lambda x: str(x))
    res_info.to_excel(save_path + '/' + '饿了么好评单详情.xlsx', encoding='utf_8_sig', index=False, engine='xlsxwriter')
    for i in locals().keys():
        del locals()[i]
    gc.collect()
    print('处理完成：{}'.format('好评单详情'))


# 差评单
def bad_order(problem_order, complete_order_df, comment_df, save_path):
    global qplus_dc, info_col, info
#     comment_df['站点ID'] = comment_df['站点ID'].astype('str')
#     comment_df['运单号'] = comment_df['运单号'].astype('str')
#     problem_order['站点ID'] = problem_order['站点ID'].astype('str')
#     problem_order['骑手ID'] = problem_order['骑手ID'].astype('str')
#     complete_order['站点ID'] = complete_order['站点ID'].astype('str')
#     complete_order['骑手ID'] = complete_order['骑手ID'].astype('str')
#     info = delivery_order[['运单号', '骑手ID', '站点ID']]
#     info = clear_data(info)
    complete_order_df = complete_order_df[['详情', '运单号']].copy()
    problem_bad = problem_order[problem_order['问题单类型'] == '差评'].rename(columns={'问题单类型': '问题单类型_KPI管理站点明细问题单'}).copy()
    problem_bad['kpi管理_站点指标明细_差评'] = 1
    complete_bad = complete_order_df[complete_order_df['详情'] == '差评'].rename(columns={'详情': '详情_配送费账单'}).copy()
    complete_bad['配送费账单_详情_差评'] = 1
    comment_bad = comment_df[
        (comment_df['评价等级'] == '吐槽') & (comment_df['申述状态'] != '二审通过') & (comment_df['判定状态'] != '无效')].rename(
        columns={'评价等级': '评价等级_服务奖惩'}).copy()
    comment_bad['服务奖惩_评价等级_吐槽'] = 1
    df_merge_1 = pd.merge(problem_bad, complete_bad, on=['运单号'], how='left')
    df_merge_2 = pd.merge(df_merge_1, comment_bad, on=['运单号'], how='right')
    df_merge_2_info = pd.merge(df_merge_2, info, on=['运单号'], how='left')
    res_info = pd.merge(df_merge_2_info, qplus_dc, how='left', on='站点ID')
    res_info[info_col] = res_info[info_col].applymap(lambda x: str(x))
    col_arrange = ['站点ID','BOSS商圈ID','商圈名称','BOSS城市代码','城市','BOSS主体ID','主体','骑手ID','运单号','骑手信息','评价等级_服务奖惩','评价来源','申述状态','判定状态','服务奖惩_评价等级_吐槽',
                    '详情_配送费账单','配送费账单_详情_差评','问题单类型_KPI管理站点明细问题单','kpi管理_站点指标明细_差评']
    res_info = res_info[col_arrange]
    res_info[info_col] = res_info[info_col].applymap(lambda x: str(x))
    res_info.to_excel(save_path + '/' + '饿了么差评单详情.xlsx', encoding='utf_8_sig', index=False)
    for i in locals().keys():
        del locals()[i]
    gc.collect()
    print('处理完成：{}'.format('差评单详情'))


# 申诉返款
def refund_order_select(illegal_order_df, delivery_order_df, save_path):
    global qplus_dc, info_col, info
#     delivery_order['站点ID'] = delivery_order['站点ID'].astype('str')
#     delivery_order['骑手ID'] = delivery_order['骑手ID'].astype('str')
#     illegal_order['站点ID'] = illegal_order['站点ID'].astype('str')
#     illegal_order['骑手ID'] = illegal_order['骑手ID'].astype('str')    
    illegal_order = illegal_order_df.copy()
    delivery_order = delivery_order_df.copy()
#     info = delivery_order[['运单号', '骑手ID', '站点ID']]
#     info = clear_data(info)
    refund_df = illegal_order[illegal_order['账单类型'] == '申诉返款'].rename(
        columns={'金额': '申诉返款_金额', '账单类型': '申诉返款_账单类型', '账单时间': '申诉返款_账单时间', '详情': '申诉返款_详情', '业务类型': '申诉返款_业务类型'})
    illegal_df = illegal_order[illegal_order['账单类型'] == '违规扣款'].rename(
        columns={'金额': '违规扣款_金额', '账单类型': '违规扣款_账单类型', '账单时间': '违规扣款_账单时间', '详情': '违规扣款_详情', '业务类型': '违规扣款_业务类型'})
    refund_res = pd.merge(illegal_df, refund_df, on=['运单号'], how='left')
    refund_res_id = pd.merge(refund_res, info, on=['运单号'], how='left')
    res_info = pd.merge(refund_res_id, qplus_dc, how='left', on='站点ID')
    col_arrange = ['站点ID','BOSS商圈ID','商圈名称','BOSS城市代码','城市','BOSS主体ID','主体','运单号','骑手ID','违规扣款_金额',
               '违规扣款_账单类型','违规扣款_账单时间','违规扣款_详情','违规扣款_业务类型','申诉返款_金额','申诉返款_账单类型','申诉返款_账单时间','申诉返款_详情','申诉返款_业务类型']
    res_info = res_info[col_arrange]
    res_info[info_col] = res_info[info_col].applymap(lambda x: str(x))
    res_info.to_excel(save_path + '/' + '饿了么违规扣款及申诉返款详情.xlsx', encoding='utf_8_sig', index=False, engine='xlsxwriter')
    for i in locals().keys():
        del locals()[i]
    gc.collect()
    print('处理完成：{}'.format('申诉返款详情'))


# 超时单
def over_time(problem_df, delivery_df, complete_df, save_path):
#     delivery_order['站点ID'] = delivery_order['站点ID'].astype('str')
#     delivery_order['骑手ID'] = delivery_order['骑手ID'].astype('str')
#     problem_order['站点ID'] = problem_order['站点ID'].astype('str')
#     problem_order['骑手ID'] = problem_order['骑手ID'].astype('str')

#     info = delivery_order[['运单号', '骑手ID', '站点ID']]
# #     info = clear_data(info)
    problem_order = problem_df.copy()
    delivery_order = delivery_df.copy()
    complete_order_df = complete_df.copy()
    overtime_problem = problem_order[problem_order['问题单类型'] == '骑手T+12超时'].rename(columns={'问题单类型': '问题单类型_骑手T12超时_KPI管理站点明细问题单'})
    overtime_problem['骑手T_12超时_KPI管理站点明细问题单'] = 1
    overtime_delivery = delivery_order[delivery_order['骑手T超时时长'].str.contains('超时')].rename(columns={'骑手T超时时长': '骑手T超时时长_运单详情'})
    overtime_delivery['运单详情_骑手T超时时长'] = 1
    overtime_complete = complete_order_df[complete_order_df['详情'] == '完成单-超时'].rename(columns={'详情': '详情_配送费账单'})
    overtime_complete['配送费账单_详情_完成单超时'] = 1
    df_merge_1 = pd.merge(overtime_problem, overtime_delivery, on='运单号', how='right')
    df_merge_2 = pd.merge(df_merge_1, overtime_complete, on='运单号', how='right')
    df_merge_2_info = pd.merge(df_merge_2, info, on=['运单号'], how='left')
    res_info = pd.merge(df_merge_2_info, qplus_dc, how='left', on='站点ID')
    col_arrange = ['站点ID','BOSS商圈ID','商圈名称','BOSS城市代码','城市','BOSS主体ID','主体','运单号','骑手ID','详情_配送费账单','配送费账单_详情_完成单超时','骑手T超时时长_运单详情',
                   '运单详情_骑手T超时时长','问题单类型_骑手T12超时_KPI管理站点明细问题单','骑手T_12超时_KPI管理站点明细问题单']
    res_info = res_info[col_arrange]
    res_info[info_col] = res_info[info_col].applymap(lambda x: str(x))
    res_info.to_excel(save_path + '/' + '饿了么超时单详情.xlsx', encoding='utf_8_sig', index=False, engine='xlsxwriter')
    for i in locals().keys():
        del locals()[i]
    gc.collect()
    print('处理完成：{}'.format('超时单详情'))




# 汇总同一目录下的所有数据
# fee: '-配送费-', refund:'-申诉返款-','-违规扣款-'
def concat_data(path, usecols=None):
    file_list = os.listdir(path)
    df_lst = []
    for f in file_list:
        file_path = os.path.join(path,f)
        print('合并{}'.format(file_path))
        if 'csv' in f:
            temp_df = pd.read_csv(file_path, encoding='utf_8_sig', low_memory=False, usecols=usecols)
        if 'xls' in f or 'xlsx' in f:
            temp_df = pd.read_excel(file_path, encoding='utf_8_sig', usecols=usecols)
        df_lst.append(temp_df)
    df = pd.concat(df_lst)
    return df
    
def fee_concat(path, usecols=None):
    file_list = os.listdir(path)
    df_lst = []
    for f in file_list:
        if '-配送费-' in f:
            file_path = os.path.join(path,f)
            print('合并{}'.format(file_path))
            if 'csv' in f: 
                temp_df = pd.read_csv(file_path, encoding='utf_8_sig', low_memory=False, usecols=usecols)
            if 'xls' in f or 'xlsx' in f:
                print(file_path)
                temp_df = pd.read_excel(file_path, encoding='utf_8_sig', usecols=usecols)
            df_lst.append(temp_df)
    df = pd.concat(df_lst)
    return df

def refund_concat(path, usecols=None):
    file_list = os.listdir(path)
    df_lst = []
    for f in file_list:
        if '-申诉返款-' in f or '-违规扣款-' in f:
            file_path = os.path.join(path,f)
            print('合并{}'.format(file_path))
            if 'csv' in f:
                temp_df = pd.read_csv(file_path, encoding='utf_8_sig', low_memory=False, usecols=usecols)
            if 'xls' in f or 'xlsx' in f:
                temp_df = pd.read_excel(file_path, encoding='utf_8_sig', usecols=usecols)
            df_lst.append(temp_df)
    df = pd.concat(df_lst)
    return df

def problem_concat(path, usecols=None):
    file_list = os.listdir(path)
    df_lst = []
    for f in file_list:
        if 'KPI问题单导出数据' in f:
            file_path = os.path.join(path,f)
            print('合并{}'.format(file_path))
            if 'csv' in f:
                temp_df = pd.read_csv(file_path, encoding='utf_8_sig', low_memory=False, usecols=usecols)
            if 'xls' in f or 'xlsx' in f:
                excel = pd.ExcelFile(file_path)
                sh_name = excel.sheet_names
                for sh in sh_name:
                    temp_df = pd.read_excel(file_path, encoding='utf_8_sig', sheet_name=sh)
                    temp_df['运单号'] = temp_df['运单号'].map(lambda x: str(x))
            df_lst.append(temp_df)
    df = pd.concat(df_lst)
    return df


def summary_data(city):
    # 合并该城市的配送费账单
    complete_order_path = './原始数据/正式整理/{}/代理商账单账单（未出账单）'.format(city)
    complete_col = ['团队ID', '骑手ID', '账单时间','时段补贴', '距离补贴', '重量补贴','运单号', '详情', '业务类型', '业务交易时间']
    complete_order = fee_concat(complete_order_path, usecols=complete_col)
    complete_order['运单号'] = complete_order['运单号'].map(lambda x: str(x).strip("'")) 
    complete_order = modify_col(complete_order)
    complete_order['站点ID'] = complete_order['站点ID'].astype('str')
    complete_order['骑手ID'] = complete_order['骑手ID'].astype('str')
    # 合并该城市运单数据
    delivery_order_path = './原始数据/正式整理/{}/5数据分析-运单详情（日数据）'.format(city)
    delivery_col = ['骑手id', '团队ID', '运单送达时间', '运单号', '用户评价状态', '骑手T超时时长']
    delivery_order = concat_data(delivery_order_path, usecols=delivery_col)
    delivery_order = modify_col(delivery_order)
    delivery_order['站点ID'] = delivery_order['站点ID'].astype('str')
    delivery_order['骑手ID'] = delivery_order['骑手ID'].astype('str')
    delivery_order = clear_data(delivery_order)
    # 合并问题单
    problem_order_path = './原始数据/正式整理/{}/20AKPI管理-站点指标明细-问题单（不用拆分）'.format(city)
    problem_col = ['运单号', '问题单类型']
    problem_order = problem_concat(problem_order_path, usecols=problem_col)
    problem_order = modify_col(problem_order)
    # 合并违规和申诉
    refund_order_path = './原始数据/正式整理/{}/代理商账单账单（未出账单）'.format(city)
    refund_col = ['团队ID', '骑手ID', '运单号', '金额', '账单类型', '账单时间', '详情', '业务类型']
    refund_order = refund_concat(refund_order_path, usecols=refund_col)
    refund_order = modify_col(refund_order) 
    refund_order['运单号'] = refund_order['运单号'].map(lambda x: str(x).strip("'"))
    refund_order['站点ID'] = refund_order['站点ID'].astype('str')
    refund_order['骑手ID'] = refund_order['骑手ID'].astype('str')
    # 合并评价数据
    comment_order_path = './原始数据/正式整理/{}/申诉管理-服务奖惩-评价等级'.format(city)
    comment_col = ['运单号', '骑手信息', '评价等级', '评价来源', '评价日期', '申述状态', '判定状态']
    comment_order = concat_data(comment_order_path, usecols=comment_col)
    comment_order['运单号'] = comment_order['运单号'].astype('str')
    
    return complete_order, delivery_order, problem_order, refund_order, comment_order

def modify_col(df):
#     df = df.applymap(lambda x: str(x).strip('"'))
#     df = df.applymap(lambda x: str(x).strip('='))
#     df = df.applymap(lambda x: str(x).strip('"'))
#     df = df.applymap(lambda x: str(x).strip('——'))
    if '团队ID' in df.columns:
        df = df.rename(columns={"团队ID": "站点ID"})
    if '骑手id' in df.columns:
        df = df.rename(columns={"骑手id": "骑手ID"})        
    return df

if __name__ == "__main__":
    path = './原始数据/正式整理/'
    roots, dirs, files = get_file_path(path)

    use_cols = ['vendor_dc_id', 'dc_id', 'dc_name', 'city_code', 'city_name', 'supplier_id', 'supplier_name']
    qplus_dc = pd.read_csv('std_qplus_dc.csv', usecols=use_cols)
    qplus_dc.rename(columns={'vendor_dc_id':'站点ID', 'city_name':'城市', 'dc_name': '商圈名称', 'dc_id': 'BOSS商圈ID', 'supplier_id': 'BOSS主体ID', "city_code":"BOSS城市代码", "supplier_name":"主体"}, inplace=True)
   
    city_list = dirs[0]
    
for i in city_list:
    # 读取数据
    complete_order, delivery_order, problem_order, refund_order, comment_order = summary_data(i)
    info_col = ['运单号', '骑手ID', '站点ID']
    info = delivery_order[info_col].copy()
    info = clear_data(info)
    
    # 整理数据

    
    # 统计数据
    save_path = './中间表/{}/'.format(i)
    os.makedirs(save_path, exist_ok=True)
    complete_order = complete_order_select(complete_order)
    order_time(complete_order, save_path)
    order_num_allowance_daily(complete_order, save_path)
    good_order(delivery_order, comment_order, save_path)
    bad_order(problem_order, complete_order, comment_order,save_path)
    refund_order_select(refund_order, delivery_order, save_path)
    over_time(problem_order, delivery_order, complete_order, save_path)
    
    del complete_order, delivery_order, problem_order, refund_order, comment_order
    gc.collect()    

print('{0} over {0}'.format('*'*20))
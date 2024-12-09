import os
import pymysql.cursors
import sys
from datetime import *
import time as astime
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, filename='app.log', encoding='utf-8', format='%(levelname)s:%(asctime)s:%(message)s')


# 初始化数据列表
department_lv3_list = []  # 三级部门信息
push_list = []  # 高意向线索系统推送数量
going_list = []  # 高意向线索门店跟进数量
going_time_list = []  # 跟进时效


def find_yk_intenter_sql(start_time, end_time):
    """
    SELECT `员工微信ID`, `客户微信ID`, `意向消息内容` FROM `云客意向客户` WHERE `是否推送`=1 AND `聊天日期` BETWEEN %s AND %s
    """
    connection = pymysql.connect(host=os.getenv('rpa_host'),
                                port=int(os.getenv('rpa_port')),
                                user=os.getenv('rpa_user'),
                                password=os.getenv('rpa_passwd'),
                                database=os.getenv('rpa_db'),
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)
    with connection:
        with connection.cursor() as cursor:
            sql = "SELECT `员工微信ID`, `客户微信ID`, `意向消息内容` FROM `云客意向客户` WHERE `是否推送`=1 AND `聊天日期` BETWEEN %s AND %s"
            cursor.execute(sql, (start_time, end_time))
            return cursor.fetchall()


def find_yk_push_msg_sql(staff_wechet_id, customer_wechet_id, msg, start_time, end_time):
    """
    SELECT `员工微信ID`, `客户微信ID`, `消息时间` FROM `云客聊天记录` WHERE `员工微信ID`=%s AND `客户微信ID`=%s AND `消息内容`=%s AND `消息时间` BETWEEN %s AND %s
    """
    connection = pymysql.connect(host=os.getenv('rpa_host'),
                                port=int(os.getenv('rpa_port')),
                                user=os.getenv('rpa_user'),
                                password=os.getenv('rpa_passwd'),
                                database=os.getenv('rpa_db'),
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)
    with connection:
        with connection.cursor() as cursor:
            sql = "SELECT `员工微信ID`, `客户微信ID`, `消息时间` FROM `云客聊天记录` WHERE `员工微信ID`=%s AND `客户微信ID`=%s AND `消息内容`=%s AND `消息时间` BETWEEN %s AND %s"
            cursor.execute(sql, (staff_wechet_id, customer_wechet_id, msg, start_time + " 00:00:00", end_time + " 23:59:59"))
            return cursor.fetchall()


def find_yk_going_msg_sql(staff_wechet_id, customer_wechet_id, push_msg_time, end_time):
    """
    SELECT `消息时间` FROM `云客聊天记录` WHERE `员工微信ID`=%s AND `客户微信ID`=%s AND `消息内容`=%s AND `消息时间` BETWEEN %s AND %s
    """
    connection = pymysql.connect(host=os.getenv('rpa_host'),
                                port=int(os.getenv('rpa_port')),
                                user=os.getenv('rpa_user'),
                                password=os.getenv('rpa_passwd'),
                                database=os.getenv('rpa_db'),
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)
    with connection:
        with connection.cursor() as cursor:
            sql = "SELECT `消息时间` FROM `云客聊天记录` WHERE `员工微信ID`=%s AND `客户微信ID`=%s AND `消息时间` BETWEEN %s AND %s"
            cursor.execute(sql, (staff_wechet_id, customer_wechet_id, push_msg_time, end_time + " 23:59:59"))
            return cursor.fetchall()


def run():
    logging.info('执行中,请等待...')

    # 查询周一到周六的云客客户数据
    start_time = "2024-12-02"
    end_time = "2024-12-07"

    # 读取员工信息
    df_user_list = pd.read_csv('user_list.csv')
    # 将 员工微信ID 设置为索引
    df_user_list.set_index('员工微信ID', inplace=True)

    logging.info('查询时间范围: %s %s', start_time, end_time)
    
    yk_intenter_list = find_yk_intenter_sql(start_time, end_time)
    if len(yk_intenter_list) == 0:
        logging.info('没有查询到当前时间范围的意向客户数据: %s %s', start_time, end_time)
        sys.exit(0)
    else:
        for i in yk_intenter_list:
            logging.info('===========================================================================')
            logging.info('当前查询的跟进信息: %s %s %s', i['员工微信ID'], i['客户微信ID'], i['意向消息内容'])
            if i['员工微信ID'] == i['客户微信ID']:
                logging.info('跳过相同的微信ID')
                continue
            else:
                # 查询员工所在部门信息
                try:
                    employee = df_user_list.loc[i['员工微信ID']]
                    department_name = employee['三级部门']
                except:
                    logging.info('未找到该员工的微信ID')
                    continue

                # 查询员工跟进信息
                find_push_time = find_yk_push_msg_sql(i['员工微信ID'], i['客户微信ID'], i['意向消息内容'], start_time, end_time)
                if len(find_push_time) == 0:
                    logging.info('没有查询到推送时间')
                    continue
                else:
                    department_lv3_list.append(department_name)  # 添加部门信息
                    push_list.append(1)  # 推送数量+1

                    push_msg_time = find_push_time[0]['消息时间']
                    msg_time_list = find_yk_going_msg_sql(i['员工微信ID'], i['客户微信ID'], push_msg_time, end_time)
                    if len(msg_time_list) <= 1:
                        logging.info('没有客服跟进信息')
                        going_list.append(0)  # 跟进数量+0
                        going_time_list.append(0)  # 跟进时效+0
                    else:
                        going_time = msg_time_list[1]['消息时间'].strftime('%Y-%m-%d %H:%M:%S')
                        logging.info('客服跟进时间: %s', going_time)

                        # 查询跟进时效
                        push_msg_time = datetime.strptime(str(push_msg_time), "%Y-%m-%d %H:%M:%S")
                        going_time = datetime.strptime(str(going_time), "%Y-%m-%d %H:%M:%S")

                        time_difference = going_time - push_msg_time
                        time_difference = time_difference.seconds/60 # 单位分钟
                        logging.info('时效: %s', time_difference)
                        going_list.append(1)  # 跟进数量+1
                        going_time_list.append(time_difference)  # 添加跟进时效
                                
                        

    df = pd.DataFrame({
        "三级部门": department_lv3_list,
        "高意向线索系统推送数量": push_list,
        "高意向线索门店跟进数量": going_list,
        "跟进时效[分钟]": going_time_list,
        },
    )

    # 处理表格数据
    result = df.groupby('三级部门').agg({
        '高意向线索系统推送数量': 'sum',
        '高意向线索门店跟进数量': 'sum',
        '跟进时效[分钟]': 'sum',
    })
    result["平均跟进时效[分钟]"] = (result["跟进时效[分钟]"] * result["高意向线索系统推送数量"] / result["高意向线索门店跟进数量"]).round(0)  # 不保留小数位

    result.drop(columns=['跟进时效[分钟]'], inplace=True) # 删除原始列
    result = result.reset_index()  # 重置索引

    result.to_excel(f'高意向顾客跟进数据[{start_time}][{end_time}].xlsx', index=False)
    logging.info('程序执行完毕')

if __name__ == '__main__':
    run()
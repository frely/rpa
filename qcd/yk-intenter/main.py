import os
import pymysql.cursors
import sys
from datetime import *
import pyodbc
import time as astime
import pandas as pd


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
        

def get_staff_name(staff_wechet_id):
    """
    SELECT `员工姓名` FROM `云客员工微信列表视图` WHERE `员工微信ID`=%s
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
            sql = "SELECT `员工姓名` FROM `云客员工微信列表视图` WHERE `员工微信ID`=%s"
            cursor.execute(sql, (staff_wechet_id))
            return cursor.fetchall()
        

def find_department_name_sql(staff_name):
    """
    SELECT 姓名, 一级部门, 二级部门, 三级部门 FROM 人力明细_明细 WHERE 人员状态='在职' AND 姓名=%s
    """
    sqlserver_host = os.getenv("sqlserver_host")
    sqlserver_db = os.getenv("sqlserver_db")
    sqlserver_user = os.getenv("sqlserver_user")
    sqlserver_password = os.getenv("sqlserver_password")
    DRIVER = "{ODBC Driver 18 for SQL Server}"
    conn = pyodbc.connect(f'DRIVER={DRIVER};SERVER={sqlserver_host};DATABASE={sqlserver_db};UID={sqlserver_user};PWD={sqlserver_password};TrustServerCertificate=yes')
    cursor = conn.cursor()
    sql = f"SELECT 姓名, 一级部门, 二级部门, 三级部门 FROM 人力明细_明细 WHERE 人员状态='在职' AND 姓名='{staff_name}'"
    cursor.execute(sql)
    return cursor.fetchall()
        
def run():
    print(astime.strftime("%Y-%m-%d %H:%M:%S", astime.localtime()), "执行中,请等待...")

    # 查询周一到周六的云客客户数据
    start_time = "2024-12-02"
    end_time = "2024-12-07"

    print("查询时间范围: ", start_time, end_time)
    
    yk_intenter_list = find_yk_intenter_sql(start_time, end_time)
    if len(yk_intenter_list) == 0:
        print("没有查询到当前时间范围的意向客户数据: ", start_time, end_time)
        sys.exit(0)
    else:
        for i in yk_intenter_list:
            print("=========================================================================================================")
            print("当前查询的跟进信息:", i['员工微信ID'], i['客户微信ID'], i['意向消息内容'])
            if i['员工微信ID'] == i['客户微信ID']:
                print("跳过相同的微信ID")
                continue
            else:
                # 查询员工姓名
                staff_name_list = get_staff_name(i['员工微信ID'])
                if len(staff_name_list) == 0:
                    print("未查询到员工姓名")
                    continue
                elif len(staff_name_list) > 1:
                    print("查询到多个员工姓名", staff_name_list)
                    sys.exit(1)
                else:
                    staff_name = staff_name_list[0]['员工姓名']

                    # 查询员工所在部门信息
                    department_name = find_department_name_sql(staff_name)
                    if len(department_name) == 0:
                        print("未查询到部门信息:", staff_name)
                        continue
                    elif len(department_name) > 1:
                        print("查询到多个部门信息:", staff_name, department_name)
                        sys.exit(1)
                    else:
                        print("员工信息:", department_name[0])

                        # 查询员工跟进信息
                        find_push_time = find_yk_push_msg_sql(i['员工微信ID'], i['客户微信ID'], i['意向消息内容'], start_time, end_time)
                        if len(find_push_time) == 0:
                            print("没有查询到推送信息")
                            continue
                        elif department_name[0][3] == "":
                            print("三级部门为空")
                            continue
                        else:
                            department_lv3_list.append(department_name[0][3])  # 添加部门信息
                            push_list.append(1)  # 推送数量+1

                            push_msg_time = find_push_time[0]['消息时间']
                            msg_time_list = find_yk_going_msg_sql(i['员工微信ID'], i['客户微信ID'], push_msg_time, end_time)
                            if len(msg_time_list) <= 1:
                                print("没有客服跟进信息")
                                going_list.append(0)  # 跟进数量+0
                                going_time_list.append(0)  # 跟进时效+0
                            else:
                                going_time = msg_time_list[1]['消息时间'].strftime('%Y-%m-%d %H:%M:%S')
                                print("客服跟进时间:", going_time)

                                # 查询跟进时效
                                push_msg_time = datetime.strptime(str(push_msg_time), "%Y-%m-%d %H:%M:%S")
                                going_time = datetime.strptime(str(going_time), "%Y-%m-%d %H:%M:%S")

                                time_difference = going_time - push_msg_time
                                time_difference = time_difference.seconds/60 # 单位分钟
                                print("时效:", time_difference)
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
    print(astime.strftime("%Y-%m-%d %H:%M:%S", astime.localtime()), "程序执行完毕")

if __name__ == '__main__':
    run()
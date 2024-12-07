import requests
import sys
import os
import time
import pandas as pd


# 初始化表格列表

df_user_list = []  # 员工名称
df_department_list = []  # 部门名称
df_type_list = []  # 类型
df_watch_time_list = []  # 观看时间

headers = {
        'referer': 'https://work.weixin.qq.com/wework_admin/frame',
        'Cookie': os.getenv("wechet_cookie"),
    }


def getcheckindata(start_time_unix, end_time_unix):
    """
    获取考勤信息, 返回考勤记录字典, 元素为字典, 字典{"员工姓名": 数据}
    """
    # 初始化数据
    getcheckindata_dict = {}  # 考勤记录字典
    data = {
        "langType": 1,
        "start_time": start_time_unix,
        "end_time": end_time_unix,
        "start_cnt": 0,
        "end_cnt": 3,
        "vid": 0,
        "status": 0,
        "record_type": 1,
        "tableType": "daily",
        "from": 0,
        "usenewpage": 1,
    }

    url = "https://work.weixin.qq.com/oamng/attendance/sheet/daily"
    response = requests.post(url, headers=headers, data=data)
    if response.status_code != 200:
        print("请求接口失败, 重试中")
        time.sleep(10)
        response = requests.post(url, headers=headers, data=data)
        if response.status_code != 200:
            print("请求接口失败, 重试中")
            time.sleep(10)
            response = requests.post(url, headers=headers, data=data)
            if response.status_code != 200:
                print("请求接口失败", response.json())
                sys.exit(1)
    else:
        rows = response.json()['data']['list']['rows']
        for i in rows:
            dict = {}
            dict["时间"] = i['cellMap']['statDate']['cntText']
            dict["员工姓名"] = i['cellMap']['statName']['cntText']
            dict["部门"] = i['cellMap']['departsName']['cntText']
            dict["班次"] = i['cellMap']['checkintime']['cntText']
            dict["最早打卡"] = i['cellMap']['earliestTime']['cntText']
            dict["最晚打卡"] = i['cellMap']['lastestTime']['cntText']
            dict["打卡次数"] = i['cellMap']['checkinCount']['cntText']
            dict["校准状态"] = i['cellMap']['exceptionInfo']['cntList'][0]
            dict["迟到时长"] = i['cellMap']['exceptionWorkOnDuration']['cntText']
            dict["早退时长"] = i['cellMap']['exceptionWorkOffDuration']['cntText']
            
            # 获取需要参加晨会的名单, 校准状态: 正常, 迟到, 旷工
            if dict["校准状态"] == "正常" or dict["校准状态"][0:1] == "迟到" or dict["校准状态"][0:1] == "旷工":
                getcheckindata_dict[i['cellMap']['statName']['cntText']] = dict  # 字典{"员工姓名": 数据}

        return getcheckindata_dict


def get_living_id(start_time_unix, end_time_unix):
    """
    获取直播记录id
    """
    url = f'https://work.weixin.qq.com/wework_admin/liveroom/mng/list?begin_ts={start_time_unix}&end_ts={end_time_unix}&limit=1'
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print("请求接口失败, 重试中")
        time.sleep(10)
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print("请求接口失败, 重试中")
            time.sleep(10)
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                print("请求接口失败", response.json())
                sys.exit(1)
    else:
        res = response.json()
        return res['data']['items']


def get_liveroom(living_id):
    """
    获取直播观看信息, 数据为json列表
    """
    url = f'https://work.weixin.qq.com/wework_admin/liveroom/h5/watch_list?living_id={living_id}&type=normal'
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print("请求接口失败, 重试中")
        time.sleep(10)
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print("请求接口失败, 重试中")
            time.sleep(10)
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                print("请求接口失败", response.json())
                sys.exit(1)
    else:
        return response.json()['data']['watch_list']


def run():
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "执行中, 请等待...")

    # 获取程序运行时当天日期范围
    start_time = time.strftime("%Y-%m-%d 00:00:00", time.localtime())
    start_time = "2024-12-06 00:00:00"
    start_time_unix = int(str(time.mktime(time.strptime(start_time,"%Y-%m-%d %H:%M:%S")))[0:-2])
    end_time = time.strftime("%Y-%m-%d 23:59:59", time.localtime())
    end_time = "2024-12-06 23:59:59"
    end_time_unix = int(str(time.mktime(time.strptime(end_time,"%Y-%m-%d %H:%M:%S")))[0:-2])
    
    print("查询日期范围:", start_time, end_time)
    dict_list = getcheckindata(start_time_unix, end_time_unix)

    # 晨会信息查询
    living_id_list = get_living_id(start_time_unix, end_time_unix)
    if len(living_id_list) == 0:
        print("没有查询到直播记录")
    else:
        living_id = living_id_list[-1]['living_id']  # 只需要查询每天第一个直播(晨会)
        watch_list = get_liveroom(living_id)
        if len(watch_list) == 0:
            print("没有观看信息")
        else:
            for i in watch_list:

                ## 判断晨会参与信息
                print("==========================================================")
                df_user_list.append(i['user_name'])

                if i['user_name'] not in dict_list:
                    try:
                        print(i['user_name'], dict_list[i['user_name']]['部门'], '未参加晨会')
                        df_department_list.append(dict_list[i['user_name']]['部门'])
                        df_type_list.append('未参加晨会')
                        df_watch_time_list.append('')
                    except:
                        print(i['user_name'], '不在部门信息中')
                        df_department_list.append('')
                        df_type_list.append('未参加晨会')
                        df_watch_time_list.append('')
                else:
                    if dict_list[i['user_name']]['迟到时长'] != '--':
                        print(i['user_name'], dict_list[i['user_name']]['部门'], '迟到:', dict_list[i['user_name']]['迟到时长'], '观看时间:', i['watch_time'])
                        df_department_list.append(dict_list[i['user_name']]['部门'])
                        df_type_list.append('迟到' + dict_list[i['user_name']]['迟到时长'])
                        df_watch_time_list.append(i['watch_time'])
                    else:
                        print(i['user_name'], dict_list[i['user_name']]['部门'], '观看时间:', i['watch_time'])
                        df_department_list.append(dict_list[i['user_name']]['部门'])
                        df_type_list.append('正常')
                        df_watch_time_list.append(i['watch_time'])

    data_time = time.strftime("%Y-%m-%d", time.localtime())
    df = pd.DataFrame({
        '员工姓名': df_user_list,
        '部门': df_department_list,
        '类型': df_type_list,
        '观看时间': df_watch_time_list,
        '日期': data_time
        },
    )
    df.to_excel(f'晨会直播数据[{data_time}].xlsx', index=False)

    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "程序执行完成")


if __name__ == '__main__':
    run()
import requests
import sys
import os
import time

headers = {
        'Content-Type': 'application/json',
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
        "end_cnt": 10000,
        "vid": 0,
        "status": 0,
        "record_type": 1,
        "passing_context": "",
        "isDailyInMonth": "",
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


def getapprovalinfo():
    """
    批量获取审批单号
    """
    url = f'https://work.weixin.qq.com/oamng/approval_v2/commQueryData?template_id=C4WsQU8Ypaz6gYDzceiPTHnH6KoPxjKHVp7DgWbTG&smart_starttime={start_time_unix}&smart_endtime={end_time_unix}'
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
        if res['data']['response']['errcode'] != 0:
            print("接口请求失败")
            sys.exit(1)
        else:
            list = res['data']['mngdata']
            if len(list) == 0:
                print("指定时间范围无请假信息")
                sys.exit(0)
            else:
                for i in list:
                    print("申请人", i['req_name'])
                    print("部门", i['req_org'])
                    print("审批状态", i['event']['sp_status'])  # 1：审批中 2：未通过
                    print("申请内容", i['event']['comm_content']['detail'])
                    print("====================================================")


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
        return res['data']['items'][0]['living_id']  # 只需要查询每天第一个直播(晨会)


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
    start_time_unix = int(str(time.mktime(time.strptime(start_time,"%Y-%m-%d %H:%M:%S")))[0:-2])
    end_time = time.strftime("%Y-%m-%d 23:59:59", time.localtime())
    end_time_unix = int(str(time.mktime(time.strptime(end_time,"%Y-%m-%d %H:%M:%S")))[0:-2])
    
    print("查询日期范围:", start_time_unix, end_time_unix)
    dict_list = getcheckindata(start_time_unix, end_time_unix)
    print("考勤信息:\n", dict_list)
    #living_id = get_living_id(start_time_unix, end_time_unix)
    #get_liveroom(living_id)

if __name__ == '__main__':
    run()
import requests
import sys
import os
import time
import pandas as pd
import logging

# 配置日志
logger = logging.getLogger("app")
logger.setLevel(logging.DEBUG)  # 设置最低日志级别
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler = logging.FileHandler("app.log", encoding="utf-8")
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(console_handler)


# 初始化数据
getcheckindata_dict = {}  # 考勤记录字典
df_user_list = []  # 员工名称
df_department_list = []  # 部门名称
df_type_list = []  # 类型
df_watch_time_list = []  # 观看时间

living_start_time = time.strftime("%Y-%m-%d 08:30:00", time.localtime())  # 晨会开始时间
living_start_time_unix = int(str(time.mktime(time.strptime(living_start_time,"%Y-%m-%d %H:%M:%S")))[0:-2])
living_end_time = time.strftime("%Y-%m-%d 09:01:00", time.localtime())  # 晨会结束时间
living_end_time_unix = int(str(time.mktime(time.strptime(living_end_time,"%Y-%m-%d %H:%M:%S")))[0:-2])

check_start_time = time.strftime("%Y-%m-%d 00:00:00", time.localtime())  # 考勤开始时间
check_start_time_unix = int(str(time.mktime(time.strptime(check_start_time,"%Y-%m-%d %H:%M:%S")))[0:-2])
check_end_time = time.strftime("%Y-%m-%d 23:59:59", time.localtime())  # 考勤结束时间
check_end_time_unix = int(str(time.mktime(time.strptime(check_end_time,"%Y-%m-%d %H:%M:%S")))[0:-2])


def getcheckindata(headers):
    """
    获取考勤信息, 返回考勤记录字典, 元素为字典, 字典{"员工姓名": 数据}
    """
    data = {
        "langType": 1,
        "start_time": check_start_time_unix,
        "end_time": check_end_time_unix,
        "start_cnt": 0,
        "end_cnt": 1000,
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
        logger.error("请求接口失败, 重试中")
        time.sleep(10)
        response = requests.post(url, headers=headers, data=data)
        if response.status_code != 200:
            logger.error("请求接口失败, 重试中")
            time.sleep(10)
            response = requests.post(url, headers=headers, data=data)
            if response.status_code != 200:
                logger.error("请求接口失败", response.json())
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
            
            logger.info('%s', dict)
            # 获取需要参加晨会的名单, 校准状态: 正常, 迟到, 旷工              
            if dict["校准状态"] == "正常" or dict["校准状态"] == "核算中" or dict["校准状态"][0:1] == "迟到" or dict["校准状态"][0:1] == "旷工":
                getcheckindata_dict[i['cellMap']['statName']['cntText']] = dict  # 字典{"员工姓名": 数据}


def get_living_id(headers):
    """
    获取直播记录id, 返回值: []
    """
    url = f'https://work.weixin.qq.com/wework_admin/liveroom/mng/list?begin_ts={living_start_time_unix}&end_ts={living_end_time_unix}&limit=1'
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        logger.error("请求接口失败, 重试中")
        time.sleep(10)
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            logger.error("请求接口失败, 重试中")
            time.sleep(10)
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                logger.error("请求接口失败", response.json())
                sys.exit(1)
    else:
        res = response.json()
        return res['data']['items']


def get_liveroom(living_id, headers):
    """
    获取直播观看信息, 数据为json列表
    """
    url = f'https://work.weixin.qq.com/wework_admin/liveroom/h5/watch_list?living_id={living_id}&type=normal'
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        logger.error("请求接口失败, 重试中")
        time.sleep(10)
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            logger.error("请求接口失败, 重试中")
            time.sleep(10)
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                logger.error("请求接口失败", response.json())
                sys.exit(1)
    else:
        return response.json()['data']['watch_list']

def run():
    logger.info('执行中, 请等待...')

    wechet_cookie_weidao = os.getenv('wechet_cookie_weidao')
    wechet_cookie_xiaochen = os.getenv('wechet_cookie_xiaochen')
    wechet_cookie_ycfz = os.getenv('wechet_cookie_ycfz')
    wechet_cookie_vertu = os.getenv('wechet_cookie_vertu')

    list = [
        {'name': '成都微岛', 'cookie': wechet_cookie_weidao},
        {'name': '晓晨科技', 'cookie': wechet_cookie_xiaochen},
        {'name': '易成方中', 'cookie': wechet_cookie_ycfz},
        {'name': 'VERTU纬图', 'cookie': wechet_cookie_vertu},
    ]
    
    logger.info("查询日期范围: %s %s", check_start_time, check_end_time)

    # 晨会人员读取
    check_user_list = {}  # 需要参加晨会的人员名单
    df = pd.read_csv('线上参会名单.csv')
    for row in df.itertuples():
        check_user_list[row.姓名] = row.部门

    # 考勤信息查询
    for i in list:
        headers = {
            'referer': 'https://work.weixin.qq.com/wework_admin/frame',
            'Cookie': i['cookie'],
            'Content-type': 'application/x-www-form-urlencoded',
        }
        logger.info('当前查询考勤公司: %s', i['name'])
        getcheckindata(headers)
    logger.info("当前考勤总人数:%s", len(getcheckindata_dict))

    # 晨会信息查询
    living_id_list_num = []
    for i in list:
        headers = {
            'referer': 'https://work.weixin.qq.com/wework_admin/frame',
            'Cookie': i['cookie'],
        }
        logger.info('当前查询晨会企微直播信息公司: %s', i['name'])
        living_id_dict = get_living_id(headers)
        if len(living_id_dict) == 1:
            logger.info("%s: 查询到直播记录", i['name'])
            dict = {}
            dict['cookie'] = i['cookie']
            dict['living_id'] = living_id_dict[0]['living_id']
            living_id_list_num.append(dict)  # 只需要查询当天第一个直播信息(晨会)

    if len(living_id_list_num) == 0:
        logger.info('当天未查询到直播记录')
        sys.exit(0)
    elif len(living_id_list_num) > 1:
        logger.error('当天查询到多个直播记录')
        sys.exit(1)
    else:
        living_id = living_id_list_num[0]['living_id']
        headers = {
            'referer': 'https://work.weixin.qq.com/wework_admin/frame',
            'Cookie': living_id_list_num[0]['cookie'],
        }

        watch_list_dict = {}
        watch_list_json = get_liveroom(living_id, headers)
        # 格式化数据方便后续判断
        for i in watch_list_json:
            watch_list_dict[i['user_name']] = i

        if len(watch_list_dict) == 0:
            logger.error("没有观看信息")
            sys.exit(1)
        else:
            for i in check_user_list:
                # 判断晨会参与信息
                logger.info("==========================================================")
                user_name = i
                if user_name not in getcheckindata_dict:
                    logger.info('%s 请假中', i)
                else:
                    logger.info('当前考勤人员: %s', user_name)
                    df_user_list.append(user_name)
                    print(getcheckindata_dict[user_name])

                    if user_name not in watch_list_dict:
                        logger.info('未参加晨会')
                        df_department_list.append(getcheckindata_dict[user_name]['部门'])
                        df_type_list.append('未参加晨会')
                        df_watch_time_list.append('')
                    else:
                        if getcheckindata_dict[user_name]['迟到时长'] != '--':
                            logger.info('%s %s 迟到: %s 观看时间: %s',user_name, getcheckindata_dict[user_name]['部门'], getcheckindata_dict[user_name]['迟到时长'], watch_list_dict[user_name]['watch_time'])
                            df_department_list.append(getcheckindata_dict[user_name]['部门'])
                            df_type_list.append('迟到' + getcheckindata_dict[user_name]['迟到时长'])
                            df_watch_time_list.append(watch_list_dict[user_name]['watch_time'])
                        else:
                            logger.info('%s %s 观看时间: %s',user_name, getcheckindata_dict[user_name]['部门'], watch_list_dict[user_name]['watch_time'])
                            df_department_list.append(getcheckindata_dict[user_name]['部门'])
                            df_type_list.append('正常')
                            df_watch_time_list.append(watch_list_dict[user_name]['watch_time'])

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

    logger.info('程序执行完成')


if __name__ == '__main__':
    run()
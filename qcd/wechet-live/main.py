import requests
import json
import sys, os
import time
import arrow

headers = {
        'Content-Type': 'application/json',
        'Cookie': os.getenv("wechet_cookie"),
    }

def getcheckindata():
    """
    获取打卡记录数据
    """
    body = {
    "opencheckindatatype": 3,
    "starttime": 1733184000,
    "endtime": 1733194800,
    "useridlist": [userid]
    }

    response = requests.post(f'https://qyapi.weixin.qq.com/cgi-bin/checkin/getcheckindata?access_token={token}', json=body)
    if response.json()['errcode'] == 0:
        checkin_list = response.json()['checkindata']
        return checkin_list[0]['checkin_time'] # 上班打卡，只需要当天第一次的记录
    else:
        print("请求接口失败:", response.json())
        sys.exit(1)

def getapprovalinfo():
    """
    批量获取审批单号
    """
    
    # 请假时间配置
    smart_starttime = "1732809600"
    smart_endtime = "1732895999"

    response = requests.get(f'https://work.weixin.qq.com/oamng/approval_v2/commQueryData?template_id=C4WsQU8Ypaz6gYDzceiPTHnH6KoPxjKHVp7DgWbTG&smart_starttime={smart_starttime}&smart_endtime={smart_endtime}',headers=headers)
    res = response.json()
    print(res)
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
                print("审批状态", i['event']['sp_status']) # 1：审批中 2：未通过
                print("申请内容", i['event']['comm_content']['detail'])
                print("====================================================")

def run():
    #userid = get_userid_by_email()
    #print(userid)
    #livingid_list = get_user_all_livingid(userid)
    #print(livingid_list)
    
    check_time = arrow.get(getcheckindata()).to('Asia/Shanghai')  # 打卡时间
    print(check_time)

    fact_time = arrow.get('2024-12-03T09:00:00+08:00')  # 考勤时间
    print(fact_time)

    
    delta = check_time - fact_time  # 迟到时间
    delta = delta.total_seconds() / 60  # 转换为分钟

    if delta < 0:
        print("未迟到")
    else:
        print(f"晨会迟到{delta}分钟")

if __name__ == '__main__':
    #run()
    getapprovalinfo()

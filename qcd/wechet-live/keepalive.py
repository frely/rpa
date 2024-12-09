import requests
import os
import time

def keepalive(i):
    """
    #防止cookie过期, 定时请求接口
    """
    headers = {
        'Content-Type': 'application/json',
        'Cookie': i['cookie'],
    }
    url = "https://work.weixin.qq.com/wework_admin/wwAuth/getEnterpriseVerifyInfo"

    try:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),"请求接口失败, 重试中")
            time.sleep(10)
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),"请求接口失败, 重试中")
                time.sleep(10)
                response = requests.get(url, headers=headers)
                if response.status_code != 200:
                    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),"请求接口失败", response.json())
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "当前账号所属企业: ", response.json()['data']['subject_name'])
    except:
        msg = i['name'] + ': 登录异常'
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), msg)
        wechet_push(msg)


def wechet_push(msg):
    """
    异常微信推送
    """
    url = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=b838aba9-d25c-48e1-987c-a8b98af80dcc'

    data = {
	    "msgtype": "text",
	    "text": {
			"content": "晨会企微直播数据job wechet-live-keepalive:" + msg
	    }
	}
    response = requests.post(url, json=data)
    if response.status_code != 200:
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),"请求接口失败, 重试中")
        time.sleep(10)
        response = requests.post(url, json=data)
        if response.status_code != 200:
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),"请求接口失败, 重试中")
            time.sleep(10)
            response = requests.post(url, json=data)
            if response.status_code != 200:
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),"请求接口失败", response.json())


def run():
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

    for i in list:
        keepalive(i)

if __name__ == '__main__':
    run()
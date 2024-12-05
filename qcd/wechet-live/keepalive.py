import requests
import os
import time
import sys

def keepalive():
    """
    #防止cookie过期, 定时请求接口
    """
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "keepalive执行中, 请等待...")

    wechet_cookie_list = []
    wechet_cookie_list.append(os.getenv("wechet_cookie_xiaochen"))
    
    for i in wechet_cookie_list:
        headers = {
            'Content-Type': 'application/json',
            'Cookie': i,
        }
        url = "https://work.weixin.qq.com/wework_admin/wwAuth/getEnterpriseVerifyInfo"

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
                    sys.exit(1)
        else:
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "当前账号所属企业: ", response.json()['data']['subject_name'])


if __name__ == '__main__':
    keepalive()
import pandas as pd
import pymysql.cursors
import os


def staff_wechet_id(staff_wechet_name):
    """
    SELECT `员工微信ID` FROM `云客员工微信列表视图` WHERE `员工微信号`=%s
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
            sql = "SELECT `员工微信ID` FROM `云客员工微信列表视图` WHERE `员工微信号`=%s"
            cursor.execute(sql, (staff_wechet_name))
            return cursor.fetchall()
        

def run():
    # 初始化员工微信id
    wechet_id_list = []

    # 读取员工信息
    df = pd.read_csv('user_list.csv')
    for row in df.itertuples():
        list = staff_wechet_id(row.员工微信号)
        if len(list) != 1:
            print('查询微信id错误:', row.员工微信号)
            wechet_id_list.append('')
        else:
            wx_id = list[0]['员工微信ID']
            wechet_id_list.append(wx_id)
    
    df['员工微信ID'] = wechet_id_list
    df.to_csv('user_list.csv', index=False, encoding='utf_8_sig')

if __name__ == '__main__':
    run()
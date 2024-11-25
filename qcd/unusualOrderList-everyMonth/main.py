import os, sys
import psycopg
import logging

# 配置信息
#### odoo ####
odoo_dbhost = os.getenv('odoo_dbhost')
odoo_dbport = os.getenv('odoo_dbport')
odoo_dbname = os.getenv('odoo_dbname')
odoo_dbuser = os.getenv('odoo_dbuser')
odoo_dbpassword = os.getenv('odoo_dbpassword')

# 检查必要信息
def init_check():
    if not odoo_dbhost or not odoo_dbport or not odoo_dbname or not odoo_dbuser or not odoo_dbpassword:
        logging.error("odoo数据库信息配置错误, 请检查环境变量")
        sys.exit(1)

# 查询odoo订单
def odoo_Find():
    with psycopg.connect(f"host={odoo_dbhost} port={odoo_dbport} dbname={odoo_dbname} user={odoo_dbuser} password={odoo_dbpassword}") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM account_incoterms")
            for record in cur:
                print(record)
            conn.commit()

# 查询云客数据


if __name__ == '__main__':
    init_check()
    odoo_Find()
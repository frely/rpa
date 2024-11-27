# 每月异常订单清单

## 使用
构建镜像:
```shell
docker build -t app .
```
运行:
```shell
docker run --rm \
    -e odoo_host="127.0.0.1" \
    -e odoo_port=5432 \
    -e odoo_dbname=odoo \
    -e odoo_dbuser=read_only_user \
    -e odoo_dbpassword=passwd \
    app
```
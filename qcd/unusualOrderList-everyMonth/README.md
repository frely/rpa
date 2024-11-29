# 每月异常订单清单

## 使用
构建镜像:
```shell
docker build -t app .
```
运行:
```shell
docker run --rm \
    -e odoo_host="http://localhost" \
    -e odoo_db=odoo \
    -e odoo_username=read_only_user \
    -e rpa_host="http://localhost" \
    -e rpa_port=3306 \
    -e rpa_db=db \
    -e rpa_user=user \
    -e rpa_passwd=passwd
    app
```
# 云客高意向顾客跟进数据

## 使用

构建镜像:
```shell
docker build -t app .
```
运行:
```shell
docker run --rm \
    -e rpa_host="localhost" \
    -e rpa_port=3306 \
    -e rpa_db=db \
    -e rpa_user=user \
    -e rpa_passwd=passwd
    app
```
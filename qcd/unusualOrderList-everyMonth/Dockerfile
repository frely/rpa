FORM docker.io/python:3.11.10-alpine3.20
WORKDIR /app
RUN apk --no-cache add tzdata && cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && apk del tzdata
COPY requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt
COPY main.py main.py
CMD ["python", "main.py"]
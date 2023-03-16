FROM python:3.8-buster

WORKDIR /usr/src

# install requirements
COPY requirements.txt ./requirements.txt
RUN pip install -r ./requirements.txt

# copy sourcecode
COPY src .

CMD ["python", "-u", "main.py"]

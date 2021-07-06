FROM python:3.6

WORKDIR /src
COPY . .

RUN pip install --upgrade pip
COPY src/requirements.txt /requirements.txt
RUN pip install -r src/requirements.txt

CMD [ "python", "./src/twitter_supervisor.py" ]
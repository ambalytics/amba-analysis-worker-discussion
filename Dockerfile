FROM python:3.5

WORKDIR /src
COPY . .


RUN pip install --upgrade pip

# todo requirements.txt
RUN pip install lxml && \
    pip install kafka && \
    pip install requests

CMD [ "python", "./src/twitter_supervisor.py" ]
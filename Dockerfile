FROM python:3.6

WORKDIR /src
COPY . .

RUN pip install --upgrade pip
COPY src/requirements.txt /requirements.txt
RUN pip install -r src/requirements.txt
RUN python3 -m spacy download en_core_web_md
RUN python3 -m spacy download de_core_news_md
RUN python3 -m spacy download es_core_news_md

#ENTRYPOINT ["/bin/bash", "-c", "./scripts/entrypoint.sh"]
ENTRYPOINT ["sh", "./scripts/entrypoint.sh"]
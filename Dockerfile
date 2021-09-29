FROM python:3.6

WORKDIR /src
COPY . .

RUN pip install --upgrade pip
COPY src/requirements.txt /requirements.txt
RUN pip install -r src/requirements.txt ;\
 python3 -m spacy download en_core_web_md ;\
 python3 -m spacy download de_core_news_md ;\
 python3 -m spacy download es_core_news_md ;\
 python3 -m spacy download fr_core_news_md ;\
 python3 -m spacy download it_core_news_md ;\
 python3 -m spacy download ru_core_news_md ;\
 python3 -m spacy download ja_core_news_md ;\
 python3 -m spacy download pl_core_news_md

#ENTRYPOINT ["/bin/bash", "-c", "./scripts/entrypoint.sh"]
ENTRYPOINT ["sh", "./scripts/entrypoint.sh"]
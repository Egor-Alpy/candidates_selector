FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    python -m spacy download en_core_web_sm && \
    python -c "import nltk; nltk.download('stopwords', download_dir='/usr/local/share/nltk_data')" && \
    rm -rf /root/.cache/pip

COPY . .

ENV NLTK_DATA=/usr/local/share/nltk_data

EXPOSE 8000

CMD ["python", "-m", "app.main"]
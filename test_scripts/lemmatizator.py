import time
import spacy
import pymorphy3
from nltk import SnowballStemmer
from nltk.corpus import stopwords
import re
import string
import nltk
import os

# Установка пути для NLTK данных ПЕРЕД любыми операциями
os.makedirs('/tmp/nltk_data', exist_ok=True)
nltk.data.path.insert(0, '/tmp/nltk_data')

print("Загрузка моделей...")

# Загрузка стоп-слов с правильным путем
try:
    stop_words_ru = set(stopwords.words("russian"))
    stop_words_en = set(stopwords.words("english"))
    print("✓ Стоп-слова уже загружены")
except LookupError:
    print("Загрузка стоп-слов...")
    nltk.download("stopwords", quiet=True, download_dir='/tmp/nltk_data')
    stop_words_ru = set(stopwords.words("russian"))
    stop_words_en = set(stopwords.words("english"))
    print("✓ Стоп-слова загружены")

nlp_en = spacy.load("en_core_web_sm")
morph_ru = pymorphy3.MorphAnalyzer()
stemmer_ru = SnowballStemmer("russian")
stemmer_en = SnowballStemmer("english")
print("✓ Модели загружены")


def detect_language(text):
    cyrillic_pattern = re.compile("[а-яА-ЯёЁ]")
    latin_pattern = re.compile("[a-zA-Z]")

    has_cyrillic = bool(cyrillic_pattern.search(text))
    has_latin = bool(latin_pattern.search(text))

    if has_cyrillic and not has_latin:
        return "ru"
    elif has_latin and not has_cyrillic:
        return "en"
    elif has_cyrillic and has_latin:
        return "mixed"
    return "unknown"


def tokenize(text):
    text = text.replace("-", " ")

    punctuation = string.punctuation + '«»—–""' "„…"
    text_clean = text.translate(str.maketrans("", "", punctuation))
    return re.findall(r"\b\w+\b", text_clean.lower())


def handle_word(word, lang="ru"):
    try:
        if lang == "ru":
            lemma_processing_time_start = time.time()
            lemma = morph_ru.parse(word)[0].normal_form
            lemma_processing_time = time.time() - lemma_processing_time_start

            stem_processing_time_start = time.time()
            stem = stemmer_ru.stem(word)
            stem_processing_time = time.time() - stem_processing_time_start

            return {
                "lemma": lemma,
                "stem": stem,
                "lemma_processing_time": lemma_processing_time,
                "stem_processing_time": stem_processing_time,
            }
        elif lang == "en":
            lemma_processing_time_start = time.time()
            lemma = nlp_en(word)[0].lemma_
            lemma_processing_time = time.time() - lemma_processing_time_start

            stem_processing_time_start = time.time()
            stem = stemmer_en.stem(word)
            stem_processing_time = time.time() - stem_processing_time_start

            return {
                "lemma": lemma,
                "stem": stem,
                "lemma_processing_time": lemma_processing_time,
                "stem_processing_time": stem_processing_time,
            }
        else:
            return {
                "lemma": word,
                "stem": word,
                "lemma_processing_time": 0,
                "stem_processing_time": 0,
            }
    except Exception as e:
        print(f"Error while handling word: {word} | {e}")
        return {
            "lemma": word,
            "stem": word,
            "lemma_processing_time": 0,
            "stem_processing_time": 0,
        }


def lemmatizate_and_stemm(text):
    words = tokenize(text)
    lang = detect_language(text)

    stop_words = (
        stop_words_ru if lang == "ru" else stop_words_en if lang == "en" else set()
    )

    filtered_words = [w for w in words if w not in stop_words]

    lemmas = []
    stems = []
    total_lemma_time = 0
    total_stem_time = 0

    for word in filtered_words:
        word_lang = detect_language(word)
        handled = handle_word(word, lang=word_lang)
        lemmas.append(handled["lemma"])
        stems.append(handled["stem"])
        total_lemma_time += handled["lemma_processing_time"]
        total_stem_time += handled["stem_processing_time"]

    return {
        "lang": lang,
        "word": " ".join(filtered_words),
        "lemma": " ".join(lemmas),
        "stem": " ".join(stems),
        "lemma_processing_time": total_lemma_time,
        "stem_processing_time": total_stem_time,
    }


if __name__ == "__main__":
    while True:
        user_input = input("input text to handle: ")
        result = lemmatizate_and_stemm(user_input)
        print(result)
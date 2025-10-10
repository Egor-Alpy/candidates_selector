import time
import spacy
import pymorphy3
from nltk import SnowballStemmer
import re

# Загрузка моделей
print("Загрузка моделей...")
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


def handle_word(word, lang="ru"):
    try:
        ts = time.time()

        if lang == "ru":
            lemma_processing_time_start = time.time()
            lemma = morph_ru.parse(word)[0].normal_form,
            lemma_processing_time = time.time() - lemma_processing_time_start


            stem_processing_time_start = time.time()
            stem = stemmer_ru.stem(word),
            stem_processing_time = time.time() - stem_processing_time_start
            return {
                "lang": lang,
                "lemma": lemma[0],
                "lemma_processing_time": lemma_processing_time,
                "stem": stem[0],
                "stem_processing_time": stem_processing_time
            }
        elif lang == "en":
            lemma_processing_time_start = time.time()
            lemma = nlp_en(word)[0].lemma_,
            lemma_processing_time = time.time() - lemma_processing_time_start


            stem_processing_time_start = time.time()
            stem = stemmer_en.stem(word),
            stem_processing_time = time.time() - stem_processing_time_start
            return {
                "lang": lang,
                "lemma": lemma[0],
                "lemma_processing_time": lemma_processing_time,
                "stem": stem[0],
                "stem_processing_time": stem_processing_time
            }
        else:
            return {
                "lang": lang,
                "lemma": word,
                "lemma_processing_time": 0,
                "stem": word,
                "stem_processing_time": 0,
            }
    except Exception as e:
        print(f"Error while handling word: {word} | {e}")
        return {"error": str(e), "word": word}  # ← ИСПРАВЛЕНО


def lemmatizate_and_stemm(user_word):
    lang = detect_language(user_word)
    handled_word = handle_word(user_word, lang=lang)
    return handled_word


if __name__ == "__main__":
    while True:
        user_word = input('input word to handel: ')
        result = lemmatizate_and_stemm(user_word)
        print(result)

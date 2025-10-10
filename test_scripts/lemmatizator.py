import time
import spacy
import pymorphy3
from nltk import SnowballStemmer
from nltk.corpus import stopwords
import re
import string

print("Загрузка моделей...")
nlp_en = spacy.load("en_core_web_sm")
morph_ru = pymorphy3.MorphAnalyzer()
stemmer_ru = SnowballStemmer("russian")
stemmer_en = SnowballStemmer("english")
stop_words_ru = set(stopwords.words("russian"))
stop_words_en = set(stopwords.words("english"))
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
    punctuation = string.punctuation + '«»—–""' "„…"
    text_clean = text.translate(str.maketrans("", "", punctuation))
    return re.findall(r"\b\w+\b", text_clean.lower())


def handle_word(word, lang="ru"):
    try:
        if lang == "ru":
            lemma = morph_ru.parse(word)[0].normal_form
            stem = stemmer_ru.stem(word)
            return lemma
        elif lang == "en":
            lemma = nlp_en(word)[0].lemma_
            stem = stemmer_en.stem(word)
            return lemma
        else:
            return word
    except Exception as e:
        print(f"Error while handling word: {word} | {e}")
        return word


def lemmatizate_and_stemm(text):
    words = tokenize(text)
    lang = detect_language(text)

    stop_words = (
        stop_words_ru if lang == "ru" else stop_words_en if lang == "en" else set()
    )

    filtered_words = [w for w in words if w not in stop_words]

    lemmas = []
    for word in filtered_words:
        word_lang = detect_language(word)
        lemma = handle_word(word, lang=word_lang)
        lemmas.append(lemma)

    return " ".join(lemmas)


if __name__ == "__main__":
    while True:
        user_input = input("input text to handle: ")
        result = lemmatizate_and_stemm(user_input)
        print(result)
import re
import string
import spacy
import pymorphy3
from nltk.corpus import stopwords
import nltk
import os

from app.core.logger import get_logger

logger = get_logger(name=__name__)


class LemmatizationService:
    """Сервис для лемматизации строк"""

    def __init__(self):
        """Инициализация сервиса лемматизации"""
        logger.info("Инициализация LemmatizationService...")

        # Настройка путей для NLTK
        nltk.data.path.append("/usr/local/share/nltk_data")
        os.makedirs("/tmp/nltk_data", exist_ok=True)
        nltk.data.path.insert(0, "/tmp/nltk_data")

        # Загрузка стоп-слов
        try:
            self.stop_words_ru = set(stopwords.words("russian"))
            self.stop_words_en = set(stopwords.words("english"))
            logger.info("✓ Стоп-слова загружены")
        except LookupError:
            logger.info("Загрузка стоп-слов...")
            nltk.download("stopwords", quiet=True, download_dir="/tmp/nltk_data")
            self.stop_words_ru = set(stopwords.words("russian"))
            self.stop_words_en = set(stopwords.words("english"))
            logger.info("✓ Стоп-слова загружены")

        # Загрузка моделей
        self.nlp_en = spacy.load("en_core_web_sm")
        self.morph_ru = pymorphy3.MorphAnalyzer()

        logger.info("✓ LemmatizationService инициализирован")

    def _detect_language(self, text: str) -> str:
        """Определение языка текста"""
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

    def _tokenize(self, text: str) -> list:
        """Токенизация текста"""
        text = text.replace("-", " ")
        punctuation = string.punctuation + '«»—–""' "„…"
        text_clean = text.translate(str.maketrans("", "", punctuation))
        return re.findall(r"\b\w+\b", text_clean.lower())

    def _lemmatize_word(self, word: str, lang: str) -> str:
        """Лемматизация одного слова"""
        try:
            if lang == "ru":
                return self.morph_ru.parse(word)[0].normal_form
            elif lang == "en":
                return self.nlp_en(word)[0].lemma_
            else:
                return word
        except Exception as e:
            logger.warning(f"Ошибка при лемматизации слова '{word}': {e}")
            return word

    def lemmatize(self, text: str) -> str:
        """
        Создает лемму для входной строки

        Args:
            text: Входная строка для лемматизации

        Returns:
            str: Лемматизированная строка
        """
        try:
            if not isinstance(text, str):
                logger.warning(f"Входное значение не является строкой: {type(text)}")
                return str(text)

            if not text or text.strip() == "":
                logger.debug("Пустая строка передана на лемматизацию")
                return text

            # Проверка на булевы значения
            if text.lower() in ["true", "false"]:
                logger.debug(f"Булево значение передано: {text}")
                return text

            # Определяем язык
            lang = self._detect_language(text)

            # Токенизация
            words = self._tokenize(text)

            # Выбор стоп-слов
            stop_words = (
                self.stop_words_ru
                if lang == "ru"
                else self.stop_words_en if lang == "en" else set()
            )

            # Фильтрация стоп-слов
            filtered_words = [w for w in words if w not in stop_words]

            # Лемматизация каждого слова
            lemmas = []
            for word in filtered_words:
                word_lang = self._detect_language(word)
                lemma = self._lemmatize_word(word, lang=word_lang)
                lemmas.append(lemma)

            result = " ".join(lemmas)

            return result

        except Exception as e:
            logger.error(f"Ошибка при лемматизации строки '{text}': {e}")
            return text

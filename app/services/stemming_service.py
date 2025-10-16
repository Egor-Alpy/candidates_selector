import re
import string
from nltk import SnowballStemmer
from nltk.corpus import stopwords
import nltk
import os

from app.core.logger import get_logger

logger = get_logger(name=__name__)


class StemmingService:
    """Сервис для стемминга строк"""

    def __init__(self):
        """Инициализация сервиса стемминга"""
        logger.info("Инициализация StemmingService...")

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

        # Загрузка стеммеров
        self.stemmer_ru = SnowballStemmer("russian")
        self.stemmer_en = SnowballStemmer("english")

        logger.info("✓ StemmingService инициализирован")

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

    def _stem_word(self, word: str, lang: str) -> str:
        """Стемминг одного слова"""
        try:
            if lang == "ru":
                return self.stemmer_ru.stem(word)
            elif lang == "en":
                return self.stemmer_en.stem(word)
            else:
                return word
        except Exception as e:
            logger.warning(f"Ошибка при стемминге слова '{word}': {e}")
            return word

    def stem(self, text: str) -> str:
        """
        Создает стемм для входной строки

        Args:
            text: Входная строка для стемминга

        Returns:
            str: Строка со стеммами
        """
        try:
            if not isinstance(text, str):
                logger.warning(f"Входное значение не является строкой: {type(text)}")
                return str(text)

            if not text or text.strip() == "":
                logger.debug("Пустая строка передана на стемминг")
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

            # Стемминг каждого слова
            stems = []
            for word in filtered_words:
                word_lang = self._detect_language(word)
                stem = self._stem_word(word, lang=word_lang)
                stems.append(stem)

            result = " ".join(stems)

            return result

        except Exception as e:
            logger.error(f"Ошибка при стемминге строки '{text}': {e}")
            return text
import logging
from typing import List, Optional, Any, Set, Tuple, Dict
from dataclasses import dataclass
import re

from app.repository.mongo import MongoRepository

logger = logging.getLogger(__name__)


class Trigrammer:
    """Сервис для создания триграмм"""

    def __init__(self, mongo_repo: Optional[MongoRepository] = None):
        self.mongo_repo = mongo_repo

    def clean_text(self, text, separator: Optional[str] = ''):
        """
        Очищает текст от знаков препинания, оставляя только буквы, цифры и пробелы
        """
        if not isinstance(text, str):
            return text

        # Удаляем все символы кроме букв, цифр и пробелов
        # \w включает буквы, цифры и подчеркивания для любых языков
        cleaned = re.sub(r'[^\w\s]', '', text)

        # Убираем лишние пробелы и приводим к нижнему регистру
        cleaned = re.sub(r'\s+', ' ', cleaned).strip().lower()

        # Заменяем пробелы на подчеркивания
        cleaned = cleaned.replace(' ', separator)

        return cleaned

    def create_ngrams(self, text: str, n: int, padding: Optional[bool]=False) -> Tuple[List[str], Set[str]]:
        """Создает множество n-грамм из текста"""
        if not text or len(text) < n:
            return list(), set()

        # Добавляем паддинг только если n > 1
        if padding:
            padding = '_' * (n - 1)
            text = f"{padding}{text}{padding}"

        # Создаем n-граммы
        ngrams_list = list()
        ngrams_set = set()
        for i in range(len(text) - n + 1):
            ngram = text[i:i + n]
            ngrams_list.append(ngram)
            ngrams_set.add(ngram)

        return ngrams_list, ngrams_set

    def create_trigrams(self, text: str) -> Tuple[List[str], Set[str]]:
        """Создает множество триграмм из текста"""
        return self.create_ngrams(text, 3)

    def create_bigrams(self, text: str) -> Tuple[List[str], Set[str]]:
        """Создает множество биграмм из текста"""
        return self.create_ngrams(text, 2)

    def calculate_trigram_similarity(self, trigrams1: Set[str], trigrams2: Set[str]) -> float:
        """
        Вычисляет коэффициент схожести Жаккара между двумя множествами триграмм
        Возвращает значение от 0 до 1, где 1 - полное совпадение
        """
        if not trigrams1 and not trigrams2:
            return 1.0

        if not trigrams1 or not trigrams2:
            return 0.0

        intersection = trigrams1.intersection(trigrams2)
        union = trigrams1.union(trigrams2)

        return len(intersection) / len(union)

    def calculate_jaccard_similarity(self, set1: Set[str], set2: Set[str]) -> float:
        """
        Вычисляет коэффициент схожести Жаккара между двумя множествами
        Возвращает значение от 0 до 1, где 1 - полное совпадение
        """
        if not set1 and not set2:
            return 1.0

        if not set1 or not set2:
            return 0.0

        intersection = set1.intersection(set2)
        union = set1.union(set2)

        return len(intersection) / len(union)

    async def compare_two_strings(self, string1: str, string2: str) -> float:
        """Сравнение двух строк через триграммы, биграммы и униграммы"""

        # logger.info(f"Сравниваем '{string1}' с '{string2}'")
        # Очищаем тексты
        clean_string1 = self.clean_text(string1)
        clean_string_1 = self.clean_text(string1, separator='_')
        clean_string2 = self.clean_text(string2)
        clean_string_2 = self.clean_text(string2, separator='_')
        # logger.info(f"Отчищенные строки '{clean_string1}' с '{clean_string2}'")

        # Создаем биграммы
        _, bigrams1 = self.create_bigrams(clean_string1)
        _, bi_grams1 = self.create_ngrams(text=clean_string_1, n=2, padding=False)
        _, __bi_grams1__ = self.create_ngrams(text=clean_string_1, n=2, padding=True)

        _, bigrams2 = self.create_bigrams(clean_string2)
        _, bi_grams2 = self.create_ngrams(text=clean_string_2, n=2, padding=False)
        _, __bi_grams2__ = self.create_ngrams(text=clean_string_2, n=2, padding=True)

        bigrams_similarity = self.calculate_jaccard_similarity(bigrams1, bigrams2)
        bi_grams_similarity = self.calculate_jaccard_similarity(bi_grams1, bi_grams2)
        __bi_grams__similarity = self.calculate_jaccard_similarity(__bi_grams1__, __bi_grams2__)
        # logger.info(f'\nсписки нграмм (схожесть {bigrams_similarity}): \n{bigrams1}\n{bigrams2}')

        # Создаем триграммы
        _, trigrams1 = self.create_trigrams(clean_string1)
        _, tri_grams1 = self.create_ngrams(text=clean_string_1, n=3, padding=False)
        _, __tri_grams1__ = self.create_ngrams(text=clean_string_1, n=3, padding=True)

        _, trigrams2 = self.create_trigrams(clean_string2)
        _, tri_grams2 = self.create_ngrams(text=clean_string_2, n=3, padding=False)
        _, __tri_grams2__ = self.create_ngrams(text=clean_string_2, n=3, padding=True)

        # print(bigrams1, bi_grams1, __bi_grams1__)
        # print(bigrams2, bi_grams2, __bi_grams2__)
        # print(trigrams1, tri_grams1, __tri_grams1__)
        # print(trigrams2, tri_grams2, __tri_grams2__)

        trigrams_similarity = self.calculate_jaccard_similarity(trigrams1, trigrams2)
        tri_grams_similarity = self.calculate_jaccard_similarity(tri_grams1, tri_grams2)
        __tri_grams__similarity = self.calculate_jaccard_similarity(__tri_grams1__, __tri_grams2__)
        # logger.info(f'\nсписки нграмм (схожесть {trigrams_similarity}): \n{trigrams1}\n{trigrams2}')

        # if bigrams_similarity < 0.5 or bi_grams_similarity < 0.5 or __bi_grams__similarity < 0.5 or trigrams_similarity < 0.5 or tri_grams_similarity < 0.5 or __tri_grams__similarity < 0.5:
        #     return {}

        return bigrams_similarity + bi_grams_similarity + __bi_grams__similarity + trigrams_similarity + tri_grams_similarity + __tri_grams__similarity

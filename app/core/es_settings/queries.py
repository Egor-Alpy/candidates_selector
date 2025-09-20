from typing import Optional

from app.core.settings import settings
from app.models.tenders import TenderPositions


class ElasticQueries:
    """Агрегация поисковоых запросов для ElasticSearch"""

    @staticmethod
    def get_query_v5(position: TenderPositions, size: Optional[int] = 200):
        """
        Поисковый запрос со СТРОГИМ соответствием категории и мягким поиском по названию/атрибутам
        Обновлен под новую структуру атрибутов в Elasticsearch
        """
        positions_demands = []
        from app.core.logger import get_logger

        logger = get_logger(name=__name__)

        # Поиск по названию (очень мягко, но с высоким весом)
        if position.title:
            positions_demands.append(
                {
                    "multi_match": {
                        "query": position.title,
                        "fields": [
                            "title^5",  # увеличиваем вес названия
                            "title.ngram^3",
                            "description^1",  # снижаем вес описания
                            "description.ngram^0.5",
                        ],
                        "type": "best_fields",
                        "fuzziness": "2",  # увеличиваем fuzziness для мягкости
                        "minimum_should_match": "50%",  # только 50% слов должны совпадать
                    }
                }
            )

        # Мягкий поиск по атрибутам с НОВОЙ структурой полей
        for attribute in position.attributes:
            if attribute.type != "Количественная" and attribute.type != "Диапазон":
                # Ищем по значению атрибута в НОВЫХ полях
                positions_demands.append(
                    {
                        "nested": {
                            "path": "attributes",
                            "query": {
                                "multi_match": {
                                    "query": attribute.value,
                                    "fields": [
                                        # НОВЫЕ поля для значений
                                        "attributes.original_value^3",  # Оригинальные значения
                                        "attributes.original_value.ngram^2",
                                        # НОВЫЕ поля для названий атрибутов
                                        "attributes.original_name^2",  # Оригинальные названия
                                        "attributes.original_name.ngram^1",
                                    ],
                                    "type": "best_fields",
                                    "fuzziness": "1",  # Немного fuzziness для опечаток
                                    "minimum_should_match": "70%",  # 70% слов должны совпадать
                                }
                            },
                        }
                    }
                )

                # Дополнительно ищем по названию атрибута
                positions_demands.append(
                    {
                        "nested": {
                            "path": "attributes",
                            "query": {
                                "multi_match": {
                                    "query": attribute.name,
                                    "fields": [
                                        "attributes.standardized_name^4",
                                        "attributes.standardized_name.ngram^2",
                                        "attributes.original_name^3",
                                        "attributes.original_name.ngram^1",
                                    ],
                                    "type": "best_fields",
                                    "fuzziness": "1",
                                    "minimum_should_match": "80%",  # Более строго для названий
                                }
                            },
                        }
                    }
                )

        # Если нет условий поиска, добавим универсальный
        if not positions_demands:
            positions_demands.append({"match_all": {}})

        # Формируем запрос с мягкими условиями
        bool_query = {
            "should": positions_demands,
            "minimum_should_match": 0,  # Хотя бы одно условие должно выполняться
        }

        # Опционально: добавляем фильтр по категории (если нужен)
        category_filter = None
        if hasattr(position, "category") and position.category:
            category_filter = {
                "bool": {
                    "should": [
                        {"term": {"category.keyword": position.category}},
                        {"term": {"yandex_category.keyword": position.category}},
                        # {
                        #     "match": {
                        #         "category": {
                        #             "query": position.category,
                        #             "minimum_should_match": "100%",
                        #         }
                        #     }
                        # },
                        # {
                        #     "match": {
                        #         "yandex_category": {
                        #             "query": position.category,
                        #             "minimum_should_match": "100%",
                        #         }
                        #     }
                        # },
                    ],
                    "minimum_should_match": 1,
                }
            }

            # Добавляем категорию как мягкое условие (не обязательное)
            if category_filter:
                bool_query["must"] = [category_filter]

        query = {
            "query": {"bool": bool_query},
            "size": size,
        }

        logger.info(f"🔍 Построен запрос для позиции: {position.title}")
        logger.debug(f"🔍 Запрос: {query}")

        return query

    @staticmethod
    def get_query_v6(position: TenderPositions, size: Optional[int] = settings.ES_CANDIDATES_QTY):
        """
        Поисковый запрос со СТРОГИМ соответствием категории и мягким поиском по названию
        """
        positions_demands = []
        from app.core.logger import get_logger

        logger = get_logger(name=__name__)

        # Поиск по названию (мягко, с высоким весом)
        if position.title:
            positions_demands.append(
                {
                    "multi_match": {
                        "query": position.title,
                        "fields": [
                            "title^5",  # увеличиваем вес названия
                            "title.ngram^3",
                            "description^1",  # снижаем вес описания
                            "description.ngram^0.5",
                        ],
                        "type": "best_fields",
                        "fuzziness": "2",  # увеличиваем fuzziness для мягкости
                        "minimum_should_match": "50%",  # только 50% слов должны совпадать
                    }
                }
            )

        # Если нет условий поиска, добавим универсальный
        if not positions_demands:
            positions_demands.append({"match_all": {}})

        # Формируем запрос с мягкими условиями
        bool_query = {
            "should": positions_demands,
            "minimum_should_match": 0,  # Хотя бы одно условие должно выполняться
        }

        # Строгий фильтр по категории (обязательное условие)
        category_filter = None
        if hasattr(position, "category") and position.category:
            category_filter = {
                "bool": {
                    "should": [
                        {"term": {"category.exact": position.category}},
                        {"term": {"yandex_category.exact": position.category}},
                    ],
                    "minimum_should_match": 1,
                }
            }

            # Добавляем категорию как обязательное условие
            if category_filter:
                bool_query["must"] = [category_filter]

        query = {
            "query": {"bool": bool_query},
            "size": size,
        }

        logger.info(f"🔍 Построен запрос для позиции: {position.title}")
        logger.debug(f"🔍 Запрос: {query}")

        return query

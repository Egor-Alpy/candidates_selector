from typing import Optional

from app.models.tenders import TenderPositions


class ElasticQueries:
    """Агрегация поисковоых запросов для ElasticSearch"""

    @staticmethod
    def get_query_v1(
        position_title: str,
        yandex_category: Optional[str] = "",
        size: Optional[int] = 100,
    ):
        return {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "title": {
                                    "query": position_title,
                                    "operator": "or",
                                }
                            }
                        },
                        {
                            "bool": {
                                "should": [
                                    {
                                        "term": {
                                            "yandex_category.exact": yandex_category
                                        }
                                    },
                                    {
                                        "match": {
                                            "yandex_category": {
                                                "query": yandex_category,
                                                "operator": "or",
                                            }
                                        }
                                    },
                                ]
                            }
                        },
                    ]
                }
            },
            "size": size,
        }

    @staticmethod
    def get_query_v1_shrink_enums(
        position_info: dict,
        yandex_category: Optional[str] = "",
        size: Optional[int] = 100,
    ):
        return {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "title": {
                                    "query": position_info["name"],
                                    "operator": "or",
                                }
                            }
                        },
                        {
                            "bool": {
                                "should": [
                                    {
                                        "term": {
                                            "yandex_category.exact": yandex_category
                                        }
                                    },
                                    {
                                        "match": {
                                            "yandex_category": {
                                                "query": yandex_category,
                                                "operator": "or",
                                            }
                                        }
                                    },
                                ]
                            }
                        },
                    ]
                }
            },
            "size": size,
        }

    @staticmethod
    def get_query_v2(
        position_title: str,
        yandex_category: Optional[str] = "",
        size: Optional[int] = 100,
    ):
        return {
            "query": {
                "bool": {
                    "should": [
                        {
                            "bool": {
                                "must": [
                                    {"match": {"title": position_title}},
                                    {
                                        "term": {
                                            "yandex_category.exact": yandex_category
                                        }
                                    },
                                ],
                                "boost": 3,
                            }
                        },
                        {"match": {"title": {"query": position_title, "boost": 1}}},
                    ],
                    "minimum_should_match": 1,
                }
            },
            "size": size,
        }

    @staticmethod
    def get_query_for_rabbit(
        position_title: str,
        yandex_category: Optional[str] = "",
        size: Optional[int] = 10000,
    ):
        return {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "title": {
                                    "query": position_title,
                                    "operator": "or",
                                }
                            }
                        },
                        {
                            "bool": {
                                "should": [
                                    {
                                        "term": {
                                            "yandex_category.exact": yandex_category
                                        }
                                    },
                                    {
                                        "match": {
                                            "yandex_category": {
                                                "query": yandex_category,
                                                "operator": "or",
                                            }
                                        }
                                    },
                                ]
                            }
                        },
                    ]
                }
            },
            "size": size,
        }

    @staticmethod
    def get_query_v5(position: TenderPositions, size: Optional[int] = 50):
        """
        Поисковый запрос со СТРОГИМ соответствием категории и мягким поиском по названию/атрибутам
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

        # Категория строго обязательна - выносим в отдельную переменную
        category_filter = None
        if position.category:
            category_filter = {
                "bool": {
                    "should": [
                        {"term": {"category": position.category}},
                        {"term": {"yandex_category.keyword": position.category}},
                        {
                            "match": {
                                "category": {
                                    "query": position.category,
                                    "minimum_should_match": "100%",
                                }
                            }
                        },
                        {
                            "match": {
                                "yandex_category": {
                                    "query": position.category,
                                    "minimum_should_match": "100%",
                                }
                            }
                        },
                    ],
                    "minimum_should_match": 1,
                }
            }

        # Мягкий поиск по атрибутам (оставляем как есть, но делаем еще мягче)
        for attribute in position.attributes:
            if attribute.type != "Количественная" and attribute.type != "Диапазон":
                positions_demands.append(
                    {
                        "nested": {
                            "path": "attributes",
                            "query": {
                                "multi_match": {
                                    "query": attribute.value,
                                    "fields": [
                                        "attributes.attr_value^3",
                                        "attributes.attr_value.ngram^2",
                                        "attributes.attr_name^2",
                                        "attributes.attr_name.ngram^1",
                                    ],
                                    "type": "best_fields",
                                    "minimum_should_match": 1,
                                }
                            },
                        }
                    }
                )

        # Если нет условий поиска, добавим универсальный
        if not positions_demands:
            positions_demands.append({"match_all": {}})

        # Формируем запрос с обязательной категорией и мягкими остальными условиями
        bool_query = {"should": positions_demands, "minimum_should_match": 1}

        # Добавляем обязательное условие по категории
        if category_filter:
            bool_query["must"] = [category_filter]

        query = {
            "query": {"bool": bool_query},
            "size": size,
        }

        return query

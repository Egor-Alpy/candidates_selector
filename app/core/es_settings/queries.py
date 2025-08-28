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
    def get_query_v5(position: TenderPositions, size: Optional[int] = 10000):
        """"""
        # razbor positon data title = position_data['title'] i t.d.

        positions_demands = []
        from app.core.logger import get_logger
        logger = get_logger(name=__name__)

        # Поиск по обычным полям документа
        positions_demands.append(
            {
                "multi_match": {
                    "query": position.title,
                    "fields": [
                        "title^3",
                        "title.ngram^2",
                        "category^2",
                        "category.ngram",
                        "yandex_category^2",
                        "yandex_category.ngram",
                        "description^1.5",
                        "description.ngram",
                    ],
                    "type": "best_fields",
                    "fuzziness": "AUTO",
                }
            },
        )
        positions_demands.append(
            {
                "multi_match": {
                    "query": position.category,
                    "fields": [
                        "category^2",
                        "yandex_category^2",
                    ],
                    "type": "best_fields",
                }
            },
        )

        for attribute in position.attributes:
            continue
            if attribute.type != "Количественная" and attribute.type != "Диапазон":
                positions_demands.append(
                    {
                        "nested": {
                            "path": "attributes",
                            "query": {
                                "multi_match": {
                                    "query": attribute.value,
                                    "fields": [
                                        "attributes.attr_name^2",
                                        "attributes.attr_name.ngram",
                                        "attributes.attr_value^3",
                                        "attributes.attr_value.ngram^1.5",
                                    ],
                                    "type": "best_fields",
                                    "fuzziness": "AUTO",
                                }
                            },
                        }
                    }
)

        query = {
            "query": {
                "bool": {
                    "must": positions_demands
                }
            },
            "size": size,
        }

        return query

from typing import Optional


class ElasticQueries:
    """Агрегация поисковоых запросов для ElasticSearch"""

    @staticmethod
    def get_query_v1(position_title: str, yandex_category: Optional[str] = '', size: Optional[int]=100):
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
                                    {"term": {"yandex_category.exact": yandex_category}},
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

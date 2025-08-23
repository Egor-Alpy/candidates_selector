class ElasticQueries:
    """Агрегация поисковоых запросов для ElasticSearch"""

    @staticmethod
    def search_foo(search_query: str):
        return {"че_искать": search_query}

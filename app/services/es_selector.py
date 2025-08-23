from app.repository.elastic import ElasticRepository


class ElasticSearchSelector:
    def __init__(self):
        self.es_repo = ElasticRepository()



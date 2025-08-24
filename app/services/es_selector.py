from app.repository.elastic import ElasticRepository


class ElasticSearchSelector:
    def __init__(self, es_repo: ElasticRepository = None):
        self.es_repo = es_repo
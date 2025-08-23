class ElasticMappings:
    """Агрегация настроек индекса с mapping для ElasticSearch"""

    @property
    def get_default_mapping(self) -> dict:
        return {"ебейший маппинг": "да согласен"}
    
from typing import Sequence

from app.models.tenders import TenderPositions

from app.core.logger import get_logger

logger = get_logger(name=__name__)

class Shrinker:
    def shrink(self, candidates: Sequence[dict], position: TenderPositions):
        position_attrs = position.attributes
        logger.info(f'Характеристики позиции: {position_attrs}')
        for candidate in candidates:
            numeric_attrs, range_attrs, enums_attrs, boolean_attrs = self.classify_candidate_attrs(candidate, position)


    def classify_candidate_attrs(self, candidate):

        return candidate

    def shrink_candidates(self, candidates):
        pass

    def shrink_with_numeric(self, candidates):
        pass

    def shrink_with_range(self, candidates, position):
        pass

    def shrink_with_enums(self, candidates, position):
        for candidate in candidates:
            rated_candidate = self.rate_candidate_enum(candidate, position)

    def rate_candidate_enum(self, candidate, position):
        for candidate_attribute in candidate['a']:
            pass

    def shrink_with_boolean(self, candidates):
        pass


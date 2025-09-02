from typing import Optional, List, Dict, Any
import re

from app.core.logger import get_logger
from app.models.tenders import TenderPositions
from app.services.attrs_sorter import AttrsSorter
from app.services.trigrammer import Trigrammer
from app.services.unit_normalizer import UnitNormalizer
from app.services.vectorizer import Vectorizer

logger = get_logger(name=__name__)

class Shrinker:
    def __init__(
            self,
            trigrammer: Optional[Trigrammer] = None,
            vectorizer: Optional[Vectorizer] = None,
            attrs_sorter: Optional[AttrsSorter] = None,
            unit_normalizer: Optional[UnitNormalizer] = None
    ):
        self.trigrammer = trigrammer
        self.vectorizer = vectorizer
        self.attrs_sorter = attrs_sorter
        self.unit_normalizer = unit_normalizer

    async def shrink(self, candidates: dict, position: TenderPositions):
        position_attrs = position.attributes
        position_max_points = len(position.attributes)

        logger.info(f'Название позиции: {position.title}')
        logger.info(f'Характеристики позиции: {position_attrs}')

        # Разбираем характеристики позиции через attrs_sorter
        position_parsed_attrs = []
        for attr in position_attrs:
            try:
                attr_str = f"{attr.name}: {attr.value}"
                parsed = await self.attrs_sorter.extract_attr_data(attr_str)
                if parsed and len(parsed) > 0:
                    position_parsed_attrs.append({
                        'original': attr,
                        'parsed': parsed[0]
                    })
                else:
                    logger.warning(f'Не удалось распарсить характеристику позиции: {attr_str}')
            except Exception as e:
                logger.error(f'Ошибка при разборе характеристики позиции "{attr.name}": {e}')

        for candidate in candidates['hits']['hits']:
            candidate_attrs = candidate["_source"]["attributes"]
            candidate['points'] = position_max_points

            logger.info(f'Обрабатываем кандидата: {candidate["_source"]["title"]}')

            # Разбираем и группируем характеристики кандидата по типам
            candidate_grouped_attrs = await self._group_candidate_attrs_by_type(candidate_attrs)

            # Обрабатываем каждую характеристику позиции
            for pos_attr_data in position_parsed_attrs:
                pos_type = pos_attr_data['parsed']['type']
                found_match = False

                if pos_type == 'simple':
                    found_match = await self._process_simple_attribute(
                        pos_attr_data, candidate_grouped_attrs
                    )
                elif pos_type == 'range':
                    found_match = await self._process_range_attribute(
                        pos_attr_data, candidate_grouped_attrs
                    )
                elif pos_type == 'multiple':
                    found_match = await self._process_multiple_attribute(
                        pos_attr_data, candidate_grouped_attrs
                    )

                # Если не нашли совпадение - минус балл
                if not found_match:
                    candidate['points'] -= 1
                    logger.info(f'Не найдено совпадение для характеристики: {pos_attr_data["parsed"]["name"]}')
                if candidate['points'] >= position_max_points // 2:
                    pass

            logger.info(f"Итоговые баллы кандидата {candidate['_source']['title']}: {candidate['points']}")

    async def _group_candidate_attrs_by_type(self, candidate_attrs: list) -> dict:
        """Группировка характеристик кандидата по типам"""
        grouped = {
            'numeric': [],
            'text': [],
            'boolean': [],
            'range': [],
            'multiple': []
        }

        for attr in candidate_attrs:
            try:
                attr_str = f"{attr['attr_name']}: {attr.get('attr_value', '')}"
                parsed = await self.attrs_sorter.extract_attr_data(attr_str)

                if parsed and len(parsed) > 0:
                    parsed_data = parsed[0]
                    attr_type = parsed_data['type']

                    # Определяем подтип для simple
                    if attr_type == 'simple':
                        value = parsed_data['value']['value']
                        if isinstance(value, bool):
                            grouped['boolean'].append({
                                'original': attr,
                                'parsed': parsed_data
                            })
                        elif isinstance(value, (int, float)):
                            grouped['numeric'].append({
                                'original': attr,
                                'parsed': parsed_data
                            })
                        else:
                            grouped['text'].append({
                                'original': attr,
                                'parsed': parsed_data
                            })
                    else:
                        grouped[attr_type].append({
                            'original': attr,
                            'parsed': parsed_data
                        })

            except Exception as e:
                logger.error(f'Ошибка при группировке характеристики: {e}')

        return grouped

    async def _process_simple_attribute(self, pos_attr_data: dict, candidate_grouped_attrs: dict) -> bool:
        """Обработка простых характеристик позиции"""
        pos_value = pos_attr_data['parsed']['value']['value']

        # Определяем тип значения позиции
        if isinstance(pos_value, bool):
            return await self._process_boolean_attribute(pos_attr_data, candidate_grouped_attrs)
        elif isinstance(pos_value, (int, float)):
            return await self._process_numeric_attribute(pos_attr_data, candidate_grouped_attrs)
        else:
            return await self._process_text_attribute(pos_attr_data, candidate_grouped_attrs)

    async def _process_numeric_attribute(self, pos_attr_data: dict, candidate_grouped_attrs: dict) -> bool:
        """Обработка числовых характеристик"""
        pos_value = pos_attr_data['parsed']['value']['value']
        pos_unit = pos_attr_data['parsed']['value'].get('unit')
        pos_name = pos_attr_data['parsed']['name']

        logger.info(f'числовые кандидаты: {candidate_grouped_attrs["numeric"]}')
        # Проверяем числовые характеристики кандидатов
        for cand_attr in candidate_grouped_attrs['numeric']:
            cand_value = cand_attr['parsed']['value']['value']
            cand_unit = cand_attr['parsed']['value'].get('unit')
            cand_name = cand_attr['parsed']['name']

            # Стандартизируем юниты
            values_match = await self._compare_numeric_values_with_normalization(
                pos_value, pos_unit, cand_value, cand_unit
            )

            if values_match:
                # Проверяем векторную схожесть названий
                name_similarity = await self.vectorizer.compare_two_strings(pos_name, cand_name)
                if name_similarity >= 0.6:  # Порог для хорошей схожести
                    logger.info(f'Найдено числовое совпадение: {pos_name}={pos_value} vs {cand_name}={cand_value}')
                    return True

        # Проверяем диапазоны кандидатов
        for cand_attr in candidate_grouped_attrs['range']:
            if await self._value_fits_in_range(pos_value, pos_unit, cand_attr):
                name_similarity = await self.vectorizer.compare_two_strings(
                    pos_name, cand_attr['parsed']['name']
                )
                if name_similarity >= 0.6:
                    logger.info(f'Найдено совпадение с диапазоном: {pos_name}={pos_value}')
                    return True

        return False

    async def _process_text_attribute(self, pos_attr_data: dict, candidate_grouped_attrs: dict) -> bool:
        """Обработка текстовых характеристик"""
        pos_value = str(pos_attr_data['parsed']['value']['value']).lower().strip()

        # Проверяем текстовые характеристики кандидатов
        for cand_attr in candidate_grouped_attrs['text']:
            cand_value = str(cand_attr['parsed']['value']['value']).lower().strip()

            # Сравниваем значения через векторы и триграммы
            vector_similarity = await self.vectorizer.compare_two_strings(pos_value, cand_value)
            ngram_similarity = await self.trigrammer.compare_two_strings(pos_value, cand_value)

            total_similarity = vector_similarity + (ngram_similarity / 6)  # нормализуем ngram к 0-1

            if total_similarity >= 1.5:  # Порог для совпадения текстовых значений
                logger.info(f'Найдено текстовое совпадение: {pos_value} vs {cand_value} (sim={total_similarity})')
                return True

        # Проверяем множественные значения кандидатов
        for cand_attr in candidate_grouped_attrs['multiple']:
            cand_values = [str(item['value']).lower().strip() for item in cand_attr['parsed']['value']]

            for cand_value in cand_values:
                vector_similarity = await self.vectorizer.compare_two_strings(pos_value, cand_value)
                ngram_similarity = await self.trigrammer.compare_two_strings(pos_value, cand_value)
                total_similarity = vector_similarity + (ngram_similarity / 6)

                if total_similarity >= 1.5:
                    logger.info(f'Найдено совпадение с множественным значением: {pos_value} vs {cand_value}')
                    return True

        return False

    async def _process_boolean_attribute(self, pos_attr_data: dict, candidate_grouped_attrs: dict) -> bool:
        """Обработка булевых характеристик - сравниваем название с значениями кандидатов"""
        pos_name = pos_attr_data['parsed']['name'].lower().strip()
        pos_value = pos_attr_data['parsed']['value']['value']

        # Проверяем все типы характеристик кандидатов
        all_candidate_attrs = []
        for attr_type in candidate_grouped_attrs.values():
            all_candidate_attrs.extend(attr_type)

        for cand_attr in all_candidate_attrs:
            cand_value_raw = cand_attr['parsed']['value']

            # Извлекаем значение в зависимости от типа
            if isinstance(cand_value_raw, dict) and 'value' in cand_value_raw:
                cand_value = str(cand_value_raw['value']).lower().strip()
            elif isinstance(cand_value_raw, list):
                # Для множественных значений проверяем все
                cand_values = [str(item.get('value', item)).lower().strip() for item in cand_value_raw]
            else:
                cand_value = str(cand_value_raw).lower().strip()
                cand_values = [cand_value]

            if 'cand_values' not in locals():
                cand_values = [cand_value]

            # Сравниваем название булевой характеристики с каждым значением кандидата
            for cand_val in cand_values:
                name_value_similarity = await self.vectorizer.compare_two_strings(pos_name, cand_val)

                if name_value_similarity >= 0.8:
                    logger.info(f'Найдено булевое совпадение: название "{pos_name}" vs значение "{cand_val}"')
                    return True

        return False

    async def _process_range_attribute(self, pos_attr_data: dict, candidate_grouped_attrs: dict) -> bool:
        """Обработка диапазонных характеристик позиции"""
        # Проверяем пересечения с числовыми значениями и диапазонами кандидатов
        for cand_attr in candidate_grouped_attrs['numeric']:
            if await self._value_fits_in_range_reverse(cand_attr, pos_attr_data):
                name_similarity = await self.vectorizer.compare_two_strings(
                    pos_attr_data['parsed']['name'], cand_attr['parsed']['name']
                )
                if name_similarity >= 0.7:
                    return True

        for cand_attr in candidate_grouped_attrs['range']:
            if await self._ranges_intersect(pos_attr_data, cand_attr):
                name_similarity = await self.vectorizer.compare_two_strings(
                    pos_attr_data['parsed']['name'], cand_attr['parsed']['name']
                )
                if name_similarity >= 0.7:
                    return True

        return False

    async def _process_multiple_attribute(self, pos_attr_data: dict, candidate_grouped_attrs: dict) -> bool:
        """Обработка множественных характеристик позиции"""
        pos_values = [str(item['value']).lower().strip() for item in pos_attr_data['parsed']['value']]

        # Проверяем пересечения с различными типами кандидатов
        for cand_attr in candidate_grouped_attrs['text']:
            cand_value = str(cand_attr['parsed']['value']['value']).lower().strip()

            for pos_val in pos_values:
                similarity = await self.vectorizer.compare_two_strings(pos_val, cand_value)
                if similarity >= 0.8:
                    return True

        for cand_attr in candidate_grouped_attrs['multiple']:
            cand_values = [str(item['value']).lower().strip() for item in cand_attr['parsed']['value']]

            # Ищем пересечения множеств
            for pos_val in pos_values:
                for cand_val in cand_values:
                    similarity = await self.vectorizer.compare_two_strings(pos_val, cand_val)
                    if similarity >= 0.8:
                        return True

        return False

    async def _compare_numeric_values_with_normalization(self, pos_val, pos_unit, cand_val, cand_unit) -> bool:
        """Сравнение числовых значений с нормализацией единиц"""
        try:
            # Если единицы одинаковые или отсутствуют - прямое сравнение
            if pos_unit == cand_unit or (not pos_unit and not cand_unit):
                return abs(pos_val - cand_val) / max(pos_val, cand_val) <= 0.1

            # Если единицы разные - нормализуем
            if pos_unit and cand_unit:
                normalized_pos = await self.unit_normalizer.normalize_unit(str(pos_val), pos_unit)
                normalized_cand = await self.unit_normalizer.normalize_unit(str(cand_val), cand_unit)

                if (isinstance(normalized_pos, dict) and isinstance(normalized_cand, dict) and
                        normalized_pos.get('success') and normalized_cand.get('success')):

                    pos_norm = normalized_pos.get('normalized_value')
                    cand_norm = normalized_cand.get('normalized_value')

                    if pos_norm is not None and cand_norm is not None:
                        return abs(pos_norm - cand_norm) / max(pos_norm, cand_norm) <= 0.1

            return False

        except Exception as e:
            logger.error(f"Ошибка при сравнении числовых значений: {e}")
            return False

    async def _value_fits_in_range(self, value, unit, range_attr) -> bool:
        """Проверка входит ли значение в диапазон кандидата"""
        try:
            range_vals = range_attr['parsed']['value']  # [start, end]
            start = range_vals[0]['value']
            end = range_vals[1]['value']
            start_unit = range_vals[0].get('unit')
            end_unit = range_vals[1].get('unit')

            # Нормализуем значения если нужно
            if unit and start_unit:
                normalized_val = await self.unit_normalizer.normalize_unit(str(value), unit)
                normalized_start = await self.unit_normalizer.normalize_unit(str(start), start_unit)

                if normalized_val.get('success') and normalized_start.get('success'):
                    value = normalized_val['normalized_value']
                    start = normalized_start['normalized_value']

            if start == "_inf-":
                start = float('-inf')
            if end == "_inf+":
                end = float('inf')
        except Exception as e:
            logger.error(e)


    async def _value_fits_in_range_reverse(self, cand_attr, pos_range_attr) -> bool:
        """Проверка входит ли значение кандидата в диапазон позиции"""
        try:
            cand_value = cand_attr['parsed']['value']['value']
            cand_unit = cand_attr['parsed']['value'].get('unit')

            pos_range_vals = pos_range_attr['parsed']['value']  # [start, end]
            start = pos_range_vals[0]['value']
            end = pos_range_vals[1]['value']
            start_unit = pos_range_vals[0].get('unit')

            # Нормализуем значения если нужно
            if cand_unit and start_unit:
                normalized_cand = await self.unit_normalizer.normalize_unit(str(cand_value), cand_unit)
                normalized_start = await self.unit_normalizer.normalize_unit(str(start), start_unit)

                if normalized_cand.get('success') and normalized_start.get('success'):
                    cand_value = normalized_cand['normalized_value']
                    start = normalized_start['normalized_value']

            if start == "_inf-":
                start = float('-inf')
            if end == "_inf+":
                end = float('inf')

            return start <= cand_value <= end

        except Exception as e:
            logger.error(f"Ошибка при проверке значения кандидата в диапазоне позиции: {e}")
            return False

    async def _ranges_intersect(self, pos_range_attr, cand_range_attr) -> bool:
        """Проверка пересечения двух диапазонов"""
        try:
            pos_range = pos_range_attr['parsed']['value']
            cand_range = cand_range_attr['parsed']['value']

            pos_start = pos_range[0]['value']
            pos_end = pos_range[1]['value']
            cand_start = cand_range[0]['value']
            cand_end = cand_range[1]['value']

            # Обрабатываем бесконечности
            if pos_start == "_inf-":
                pos_start = float('-inf')
            if pos_end == "_inf+":
                pos_end = float('inf')
            if cand_start == "_inf-":
                cand_start = float('-inf')
            if cand_end == "_inf+":
                cand_end = float('inf')

            # Проверяем пересечение
            return pos_start <= cand_end and cand_start <= pos_end

        except Exception as e:
            logger.error(f"Ошибка при проверке пересечения диапазонов: {e}")
            return False





    async def _compare_parsed_values(self, pos_data: dict, cand_data: dict) -> bool:
        """Сравнение распарсенных значений по типам"""
        pos_type = pos_data.get('type')
        cand_type = cand_data.get('type')

        # Если типы разные, пытаемся найти совместимость
        if pos_type != cand_type:
            return await self._compare_different_types(pos_data, cand_data)

        # Сравниваем одинаковые типы
        if pos_type == 'simple':
            return await self._compare_simple_values(pos_data, cand_data)
        elif pos_type == 'range':
            return await self._compare_range_values_parsed(pos_data, cand_data)
        elif pos_type == 'multiple':
            return self._compare_multiple_values(pos_data, cand_data)

        return False

    async def _compare_different_types(self, pos_data: dict, cand_data: dict) -> bool:
        """Сравнение значений разных типов"""
        # simple vs range - проверяем входит ли значение в диапазон
        if pos_data['type'] == 'simple' and cand_data['type'] == 'range':
            return await self._value_in_range(pos_data, cand_data)
        elif pos_data['type'] == 'range' and cand_data['type'] == 'simple':
            return await self._value_in_range(cand_data, pos_data)

        # simple vs multiple - проверяем есть ли значение среди множественных
        if pos_data['type'] == 'simple' and cand_data['type'] == 'multiple':
            return self._value_in_multiple(pos_data, cand_data)
        elif pos_data['type'] == 'multiple' and cand_data['type'] == 'simple':
            return self._value_in_multiple(cand_data, pos_data)

        return False

    async def _compare_simple_values(self, pos_data: dict, cand_data: dict) -> bool:
        """Сравнение простых значений"""
        pos_value = pos_data['value']['value']
        cand_value = cand_data['value']['value']
        pos_unit = pos_data['value'].get('unit')
        cand_unit = cand_data['value'].get('unit')

        # Булевые значения
        if isinstance(pos_value, bool) and isinstance(cand_value, bool):
            return pos_value == cand_value

        # Числовые значения
        if isinstance(pos_value, (int, float)) and isinstance(cand_value, (int, float)):
            return await self._compare_numeric_with_units(
                pos_value, pos_unit, cand_value, cand_unit
            )

        # Строковые значения
        if isinstance(pos_value, str) and isinstance(cand_value, str):
            return self._compare_string_values(pos_value, cand_value)

        return False



    def _compare_string_values(self, pos_val: str, cand_val: str) -> bool:
        """Сравнение строковых значений"""
        pos_lower = pos_val.lower().strip()
        cand_lower = cand_val.lower().strip()

        # Точное совпадение
        if pos_lower == cand_lower:
            return True

        # Одно содержится в другом
        if pos_lower in cand_lower or cand_lower in pos_lower:
            return True

        return False

    async def _compare_range_values_parsed(self, pos_data: dict, cand_data: dict) -> bool:
        """Сравнение диапазонов"""
        try:
            pos_range = pos_data['value']  # [start, end]
            cand_range = cand_data['value']

            pos_start = pos_range[0]['value']
            pos_end = pos_range[1]['value']
            cand_start = cand_range[0]['value']
            cand_end = cand_range[1]['value']

            # Обрабатываем бесконечности
            if pos_start == "_inf-":
                pos_start = float('-inf')
            if pos_end == "_inf+":
                pos_end = float('inf')
            if cand_start == "_inf-":
                cand_start = float('-inf')
            if cand_end == "_inf+":
                cand_end = float('inf')

            # Проверяем пересечение диапазонов
            return pos_start <= cand_end and cand_start <= pos_end

        except Exception as e:
            logger.error(f"Ошибка при сравнении диапазонов: {e}")
            return False

    def _compare_multiple_values(self, pos_data: dict, cand_data: dict) -> bool:
        """Сравнение множественных значений"""
        pos_values = [item['value'] for item in pos_data['value']]
        cand_values = [item['value'] for item in cand_data['value']]

        # Ищем пересечение
        pos_set = set(str(v).lower() for v in pos_values)
        cand_set = set(str(v).lower() for v in cand_values)

        return bool(pos_set.intersection(cand_set))

    async def _value_in_range(self, value_data: dict, range_data: dict) -> bool:
        """Проверка входит ли значение в диапазон"""
        try:
            value = value_data['value']['value']
            range_vals = range_data['value']  # [start, end]

            start = range_vals[0]['value']
            end = range_vals[1]['value']

            if start == "_inf-":
                start = float('-inf')
            if end == "_inf+":
                end = float('inf')

            # Учитываем включение границ
            start_included = range_data.get('is_range_start_included', True)
            end_included = range_data.get('is_range_end_included', True)

            if start_included and end_included:
                return start <= value <= end
            elif start_included:
                return start <= value < end
            elif end_included:
                return start < value <= end
            else:
                return start < value < end

        except Exception as e:
            logger.error(f"Ошибка при проверке значения в диапазоне: {e}")
            return False

    def _value_in_multiple(self, value_data: dict, multiple_data: dict) -> bool:
        """Проверка есть ли значение среди множественных"""
        value = str(value_data['value']['value']).lower()
        multiple_values = [str(item['value']).lower() for item in multiple_data['value']]

        return value in multiple_values

    def classify_candidate_attrs(self, candidate):
        """Классификация атрибутов кандидата по типам (если потребуется)"""
        return candidate  # Теперь классификация происходит через attrs_sorter
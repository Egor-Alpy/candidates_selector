import json
import time
from typing import Optional, List, Dict

from app.core.logger import get_logger
from app.models.tenders import TenderPositions
from app.services.attrs_standardizer import AttrsStandardizer
from app.services.trigrammer import Trigrammer
from app.services.unit_standardizer import UnitStandardizer
from app.services.vectorizer import SemanticMatcher

logger = get_logger(name=__name__)


class Shrinker:
    def __init__(
        self,
        trigrammer: Optional[Trigrammer] = None,
        vectorizer: Optional[SemanticMatcher] = None,
        attrs_sorter: Optional[AttrsStandardizer] = None,
        unit_normalizer: Optional[UnitStandardizer] = None,
    ):
        self.trigrammer = trigrammer
        self.vectorizer = vectorizer
        self.attrs_sorter = attrs_sorter
        self.unit_normalizer = unit_normalizer

    async def shrink(self, candidates: dict, position: TenderPositions):
        """Основной метод для оценки кандидатов"""

        # === ЭТАП 1: ПОДГОТОВКА ===
        logger.warning("=" * 60)
        logger.warning("НАЧАЛО ОБРАБОТКИ ПОЗИЦИИ")
        logger.warning("=" * 60)

        position_max_points = len(position.attributes)
        min_required_points = position_max_points // 2  # Половина от максимума

        logger.info(f"📋 Название позиции: {position.title}")
        logger.info(f"📋 Категория позиции: {position.category}")
        logger.info(f"🎯 Максимальные баллы: {position_max_points}")
        logger.info(f"⚡ Минимум для прохода: {min_required_points}")

        # Парсим атрибуты позиции с группировкой
        position_attrs = await self._parse_position_attributes(position.attributes)
        logger.critical(position_attrs)

        if len(position_attrs.get('attrs', [])) == 0:
            logger.warning("❌ Нет атрибутов для сравнения")
            return

        # === ЭТАП 2: ОБРАБОТКА КАНДИДАТОВ ===
        logger.info(f"\n🔍 Начинаем обработку {len(candidates['hits']['hits'])} кандидатов")

        processed_candidates = []
        unmatched_characteristics = set()

        for idx, candidate in enumerate(candidates["hits"]["hits"]):
            logger.info(f"\n--- Кандидат {idx + 1}: {candidate['_source']['title']} ---")

            result = await self._process_single_candidate(
                candidate,
                position_attrs,
                min_required_points,
                unmatched_characteristics,
            )

            if result:
                processed_candidates.append(result)

        # === ЭТАП 3: ФИНАЛЬНАЯ ОБРАБОТКА ===
        await self._finalize_results(
            candidates,
            processed_candidates,
            unmatched_characteristics,
            position,
            min_required_points,
        )

    async def _parse_position_attributes(self, attributes) -> Dict:
        """Парсинг атрибутов позиции с группировкой по типам"""
        logger.info("\n📝 ПАРСИНГ АТРИБУТОВ ПОЗИЦИИ:")

        # Инициализация групп
        attrs_data = {
            "attrs": [],
        }

        for i, attr in enumerate(attributes):
            logger.info(
                f"📊 Attr {i+1}: name='{attr.name}', value='{attr.value}', "
                f"type='{getattr(attr, 'type', 'NO_TYPE')}', unit='{getattr(attr, 'unit', 'NO_UNIT')}'"
            )

        parsed_attrs = []

        for i, attr in enumerate(attributes):
            logger.info(f"\n--- АТРИБУТ ПОЗИЦИИ {i+1}/{len(attributes)} ---")
            try:
                parsed = None
                try:
                    unit = getattr(attr, "unit", "") or ""
                    raw_string = f"{attr.name}: {attr.value} {unit}".strip()
                    logger.info(f"🔄 Raw request: {raw_string}")
                    parsed = await self.attrs_sorter.extract_attr_data(raw_string)
                    logger.info(f"🔄 Raw response: {parsed}")
                except Exception as e:
                    logger.error(f"failed: {e}")

                if parsed and len(parsed) > 0:
                    # Определяем подтип для simple значений
                    original_type = parsed[0].get("type", "simple")
                    if original_type == "simple":
                        value = parsed[0].get("value", {}).get("value")
                        unit = parsed[0].get("value", {}).get("unit")
                        final_type = self._determine_value_subtype(value)
                    else:
                        final_type = original_type

                    # Стандартизация единиц измерения если есть unit_normalizer и единица
                    normalized_parsed = parsed[0].copy()
                    if original_type == "numeric":
                        unit_value = parsed[0].get("value", {}).get("unit")
                        numeric_value = parsed[0].get("value", {}).get("value")

                        if unit_value and isinstance(numeric_value, (int, float)):
                            try:
                                logger.info(
                                    f"🔧 Normalizing unit: {numeric_value} {unit_value}"
                                )
                                normalized_result = (
                                    await self.unit_normalizer.normalize_unit(
                                        str(numeric_value), unit_value
                                    )
                                )

                                if normalized_result.get("success", False):
                                    # Обновляем данные нормализованными значениями
                                    normalized_parsed["value"]["value"] = (
                                        normalized_result.get(
                                            "normalized_value", numeric_value
                                        )
                                    )
                                    normalized_parsed["value"]["unit"] = (
                                        normalized_result.get(
                                            "normalized_unit", unit_value
                                        )
                                    )
                                    logger.info(
                                        f"✅ Unit normalized: {normalized_result.get('normalized_value')} {normalized_result.get('normalized_unit')}"
                                    )
                                else:
                                    logger.warning(
                                        f"⚠️ Unit normalization failed for {numeric_value} {unit_value}"
                                    )
                            except Exception as e:
                                logger.error(f"💥 Error normalizing unit: {e}")
                    elif original_type == "range":
                        range_values = parsed[0].get("value", [])
                        if len(range_values) == 2:
                            for i, range_item in enumerate(range_values):
                                unit_value = range_item.get("unit")
                                numeric_value = range_item.get("value")

                                if unit_value and self.unit_normalizer and (isinstance(numeric_value, (int, float)) or numeric_value in ['_inf+', '_inf-']):
                                    try:
                                        if numeric_value in ["_inf+", "_inf-"]:
                                            # Для бесконечных значений стандартизируем только единицу, значение не трогаем
                                            normalized_result = await self.unit_normalizer.normalize_unit(
                                                "1", unit_value
                                            )
                                            logger.info(f'normalized_result: {normalized_result}')
                                            logger.info(f'normalized_parsed: {normalized_parsed}')
                                            if normalized_result.get("success", False):
                                                normalized_parsed["value"][i]["unit"] = normalized_result.get(
                                                    "normalized_unit", unit_value
                                                )
                                                # Значение остается как было (_inf+ или _inf-)
                                        else:
                                            # Для обычных чисел стандартизируем и значение, и единицу
                                            normalized_result = await self.unit_normalizer.normalize_unit(
                                                str(numeric_value), unit_value
                                            )
                                            if normalized_result.get("success", False):
                                                normalized_parsed["value"][i][
                                                    "value"
                                                ] = normalized_result.get(
                                                    "normalized_value", numeric_value
                                                )
                                                normalized_parsed["value"][i][
                                                    "unit"
                                                ] = normalized_result.get(
                                                    "normalized_unit", unit_value
                                                )
                                    except Exception as e:
                                        logger.error(
                                            f"Error normalizing range unit: {e}"
                                        )
                    parsed_data = {
                        "parsed": normalized_parsed,
                        "type": final_type,
                    }

                    attrs_data["attrs"].append(parsed_data)

                    logger.info(f"  - Тип: {final_type} (оригинал: {original_type})")
                    logger.info(
                        f"  - Значение: {normalized_parsed.get('value', 'unknown')}"
                    )
                else:
                    logger.warning(
                        f"❌ Final parsed result: {parsed} | {attr.name}, {attr.value}"
                    )

            except Exception as e:
                logger.error(f"CRITICAL ERROR for '{attr.name}': {e}")
                logger.error(f"Exception type: {type(e)}")

        # Логирование статистики позиции
        logger.info(f"\n📊 ИТОГОВАЯ СТАТИСТИКА ПОЗИЦИИ:")
        logger.info(f"📊 Всего атрибутов: {len(attributes)}")
        logger.info(f"📊 Успешно распаршено: {len(attrs_data['attrs'])}")
        logger.info(f"📊 Статистика по типам:")
        for attr_type, count in type_stats.items():
            if count > 0:
                logger.info(f"  📊 {attr_type}: {count} атрибутов")

        time.sleep(3)
        # Добавляем метаданные
        attrs_data["_metadata"] = {
                "total_count": len(attrs_data["attrs"]),
                "type_stats": type_stats,
                "processing_timestamp": time.time(),
            }

        return attrs_data

    async def _process_single_candidate(
        self,
        candidate: Dict,
        position_attrs: Dict,
        min_required_points: int,
        unmatched_characteristics: set,
    ) -> Optional[Dict]:
        """Обработка одного кандидата с группировкой"""

        candidate_attrs = candidate["_source"].get("attributes", [])
        position_attrs = position_attrs["attrs"]

        # Инициализация результата
        result = {
            "candidate": candidate,
            "points": 0,
            "max_points": len(position_attrs),
            "matched_attributes": [],
            "unmatched_attributes": [],
            "early_exit": False,
            "attribute_matching_details": {},
        }

        logger.info(
            f'🔎 Категория yandex: {candidate["_source"]["yandex_category"]} | '
            f'Категория: {candidate["_source"]["category"]} | '
            f"кол-во атрибутов: {len(candidate_attrs)}"
        )

        # Парсим атрибуты кандидата с группировкой
        candidate_grouped_attrs = await self._parse_candidate_attributes(
            candidate_attrs
        )

        for a, b in candidate_grouped_attrs.items():
            logger.warning(f"{a, b}")

        # Логируем статистику по типам
        cand_metadata = candidate_grouped_attrs.get("_metadata", {})

        logger.info(f"📊 Кандидат по типам: {cand_metadata.get('type_stats', {})}")

        # Сохраняем детальную информацию
        result["attribute_matching_details"] = {
            "candidate_attrs_by_type": cand_metadata.get("type_stats", {}),
            "total_position_attrs": len(position_attrs),
            "total_candidate_attrs": len(candidate_grouped_attrs["all"]),
            "type_match_strategies": {},
        }

        # Проверяем каждый атрибут позиции
        for pos_attr in position_attrs:
            logger.info(f'pos_attr: {pos_attr}')
            logger.info(f'cand_attrs: {candidate_grouped_attrs}')
            pos_type = pos_attr.get("type", "unknown")

            # Стратегия 1: Точное совпадение типов
            target_group = candidate_grouped_attrs.get(pos_type, [])
            match_found = await self._find_attribute_match_in_group(
                pos_attr, target_group, result, f"exact_type_match_{pos_type}", group_type=pos_type
            )

            # Стратегия 2: Целенаправленное кросс-типовое сравнение
            if not match_found:
                compatible_groups = self._get_compatible_attribute_groups(
                    pos_type, candidate_grouped_attrs
                )
                logger.warning('compatible_groups')
                for group in compatible_groups:
                    logger.warning(group)
                for group_name, group_attrs in compatible_groups:
                    match_found = await self._find_attribute_match_in_group(
                        pos_attr,
                        group_attrs,
                        result,
                        f"cross_type_match_{pos_type}_vs_{group_name}",
                        group_type=group_name
                    )
                    if match_found:
                        break

            # Обновляем результат
            if match_found:
                result["points"] += 1
                logger.info(f"  ✅ +1 балл за: {pos_attr['parsed']['name']}")
            else:
                result["unmatched_attributes"].append(pos_attr['parsed']['name'])
                unmatched_characteristics.add(pos_attr['parsed']['name'])
                logger.info(f"  ❌ Не найдено: {pos_attr['parsed']['name']}")

            # Проверка раннего выхода
            remaining_attrs = (
                len(position_attrs)
                - len(result["matched_attributes"])
                - len(result["unmatched_attributes"])
            )
            max_possible_points = result["points"] + remaining_attrs

            if max_possible_points < min_required_points:
                logger.warning(
                    f"  ⚡ Ранний выход: максимум возможных баллов {max_possible_points} < {min_required_points}"
                )
                result["early_exit"] = True
                break

        # Финальная оценка
        logger.info(f"📈 Итоговый счет: {result['points']}/{result['max_points']}")

        # Фильтрация по минимуму баллов
        if result["points"] < min_required_points:
            logger.warning(
                f"❌ Кандидат отклонен: {result['points']} < {min_required_points}"
            )
            return None

        logger.info(f"✅ Кандидат принят!")
        return result

    def _get_compatible_attribute_groups(
        self, pos_type: str, candidate_grouped_attrs: Dict
    ) -> List[tuple]:
        """Определение совместимых групп атрибутов для кросс-типового сравнения"""
        compatibility_rules = {
            "numeric": ["range"],
            "range": ["numeric"],
            "string": ["multiple", "boolean"],
            "multiple": ["string", "boolean"],
            "boolean": ["string", "multiple"],
        }

        compatible_groups = []
        target_types = compatibility_rules.get(pos_type, [])

        for target_type in target_types:
            group_attrs = candidate_grouped_attrs.get(target_type, [])
            if group_attrs:
                compatible_groups.append((target_type, group_attrs))

        logger.info(
            f"For {pos_type} found compatible groups: {[name for name, _ in compatible_groups]}"
        )
        return compatible_groups

    async def _parse_candidate_attributes(
        self, candidate_attrs: List[Dict]
    ) -> Dict[str, List[Dict]]:
        """Парсинг атрибутов кандидата с группировкой по типам"""

        # Инициализация групп
        grouped_attrs = {
            "boolean": [],
            "numeric": [],
            "string": [],
            "range": [],
            "multiple": [],
            "unknown": [],
            "all": [],
        }

        # Статистика по типам
        type_stats = {
            "boolean": 0,
            "numeric": 0,
            "string": 0,
            "range": 0,
            "multiple": 0,
            "unknown": 0,
        }

        for attr in candidate_attrs:
            try:
                # Атрибуты кандидатов УЖЕ ПРЕДОБРАБОТАНЫ в Elasticsearch
                standardized_name = attr.get(
                    "standardized_name", attr.get("original_name", "")
                )
                standardized_value = attr.get(
                    "standardized_value", attr.get("original_value", "")
                )
                attribute_type = attr.get("attribute_type", "simple")

                # Определение единицы измерения в зависимости от типа
                if attribute_type == "simple":
                    standardized_unit = attr.get("standardized_unit")
                else:
                    # Для range/multiple пытаемся извлечь unit из первого элемента
                    if (
                        isinstance(standardized_value, list)
                        and len(standardized_value) > 0
                    ):
                        first_item = standardized_value[0]
                        standardized_unit = (
                            first_item.get("unit")
                            if isinstance(first_item, dict)
                            else None
                        )
                    else:
                        standardized_unit = attr.get("standardized_unit")

                # Создаем структуру совместимую с attrs_sorter
                parsed_structure = {
                    "name": standardized_name,
                    "type": attribute_type,
                    "value": self._convert_to_attrs_sorter_format(
                        standardized_value, standardized_unit, attribute_type
                    ),
                    "raw_data": attr,
                }

                # Определяем подтип для simple значений
                if attribute_type == "simple":
                    value_subtype = self._determine_value_subtype(
                        standardized_value
                    )
                    final_type = value_subtype
                else:
                    final_type = attribute_type

                parsed_attr = {
                    "parsed": parsed_structure,
                    "display_name": f"{standardized_name}: {standardized_value}",
                    "type": final_type
                }

                # Группировка по типам
                if final_type in grouped_attrs:
                    grouped_attrs[final_type].append(parsed_attr)
                    type_stats[final_type] += 1
                else:
                    grouped_attrs["unknown"].append(parsed_attr)
                    type_stats["unknown"] += 1
                    logger.warning(f"⚠️ Unknown attribute type: {final_type} for {attr.get('original_name', 'name does not defined')}")

                # Добавляем в общий список для обратной совместимости
                grouped_attrs["all"].append(parsed_attr)

            except Exception as e:
                logger.error(f"💥 Ошибка конвертации атрибута кандидата: {e}")
                logger.error(f"Проблемный атрибут: {attr}")

        # Логирование статистики
        logger.info(f"📊 Converted {len(grouped_attrs['all'])} candidate attributes:")
        for attr_type, count in type_stats.items():
            if count > 0:
                logger.info(f"  📊 {attr_type}: {count} атрибутов")

        # Добавляем метаданные
        grouped_attrs["_metadata"] = {
            "total_count": len(grouped_attrs["all"]),
            "type_stats": type_stats,
            "processing_timestamp": time.time(),
        }

        return grouped_attrs

    def _convert_to_attrs_sorter_format(self, value, unit, attr_type):
        """Конвертация предобработанных атрибутов в формат attrs_sorter"""
        try:
            if attr_type == "simple":
                return {"value": value, "unit": unit}

            elif attr_type == "range":
                if isinstance(value, list) and len(value) == 2:
                    return [
                        {
                            "value": (
                                value[0].get("value")
                                if isinstance(value[0], dict)
                                else value[0]
                            ),
                            "unit": unit,
                        },
                        {
                            "value": (
                                value[1].get("value")
                                if isinstance(value[1], dict)
                                else value[1]
                            ),
                            "unit": unit,
                        },
                    ]
                else:
                    return [
                        {"value": value, "unit": unit},
                        {"value": value, "unit": unit},
                    ]

            elif attr_type == "multiple":
                if isinstance(value, list):
                    return [
                        {
                            "value": (
                                item.get("value") if isinstance(item, dict) else item
                            ),
                            "unit": unit,
                        }
                        for item in value
                    ]
                else:
                    return [{"value": value, "unit": unit}]

            else:
                return {"value": value, "unit": unit}

        except Exception as e:
            logger.error(f"Error converting value {value}: {e}")
            return {"value": str(value), "unit": unit}

    def _determine_value_subtype(self, value) -> str:
        """Определение подтипа простого значения: boolean, numeric или string"""
        try:
            # Проверка на boolean
            if isinstance(value, bool):
                return "boolean"

            # Проверка на числовые значения
            if isinstance(value, (int, float)):
                return "numeric"

            # Для строковых значений
            if isinstance(value, str):
                # Попытка преобразовать в число
                try:
                    cleaned_value = str(value).strip().replace(",", ".")
                    float(cleaned_value)
                    return "numeric"
                except (ValueError, TypeError):
                    pass

                # Проверка на булевы значения в текстовом виде
                boolean_values = {
                    "да",
                    "нет",
                    "true",
                    "false",
                    "yes",
                    "no",
                    "есть",
                    "отсутствует",
                    "имеется",
                    "не имеется",
                    "1",
                    "0",
                    "вкл",
                    "выкл",
                    "включено",
                    "выключено",
                }
                if value.lower().strip() in boolean_values:
                    return "boolean"

                return "string"

            if value is None:
                return "string"

            logger.warning(f"Unknown value type for: {value} (type: {type(value)})")
            return "string"

        except Exception as e:
            logger.error(f"Error determining value subtype for {value}: {e}")
            return "string"

    async def _find_attribute_match_in_group(
        self,
        pos_attr: Dict,
        candidate_attrs_group: List[Dict],
        result: Dict,
        match_type: str,
        group_type: str
    ) -> bool:
        """Поиск совпадения атрибута в конкретной группе кандидатов"""

        pos_type = pos_attr.get("type")
        pos_name = pos_attr["parsed"].get("name", "")

        logger.warning(f'pos_type = {pos_type} | cand_type = {group_type}')

        for cand_attr in candidate_attrs_group:
            cand_type = group_type
            cand_name = cand_attr["parsed"].get("name", "")

            # Проверка совместимости по названию
            name_similarity = await self._check_name_similarity(pos_name, cand_name)
            logger.info(f'name_similarity: {name_similarity} | {pos_name} - {cand_name}')

            if name_similarity < 0.7:
                continue

            # Проверка совместимости по типу и значению
            value_match = await self._check_value_compatibility(
                pos_attr["parsed"], pos_type=pos_type, cand_parsed=cand_attr["parsed"], cand_type=cand_type
            )

            if value_match:
                result["matched_attributes"].append(
                    {
                        "position_attr": f"'{pos_attr['parsed']['name']}': {pos_attr['parsed']['value']}",
                        "candidate_attr": f"'{cand_attr['parsed']['name']}': {cand_attr['parsed']['value']}",
                        "name_similarity": name_similarity,
                        "match_type": f"{pos_attr.get('type', 'unknown')} vs {cand_attr.get('type', 'unknown')}",
                        "matching_strategy": match_type,
                        "position_attr_type": pos_attr.get("type", "unknown"),
                        "candidate_attr_type": cand_attr.get("type", "unknown"),
                    }
                )
                return True

        return False

    async def _check_name_similarity(self, name1: str, name2: str) -> float:
        """Проверка схожести названий атрибутов"""
        try:
            if not name1 or not name2:
                return 0.0

            similarity = await self.vectorizer.compare_two_strings(name1, name2)
            return similarity

        except Exception as e:
            logger.error(f"Ошибка сравнения названий: {e}")
            return 0.0

    async def _check_value_compatibility(
        self, pos_parsed: Dict, pos_type: str, cand_parsed: Dict, cand_type: str
    ) -> bool:
        """Проверка совместимости значений атрибутов (обновленная версия)"""
        try:
            # Boolean значения - сравниваем названия, а не значения
            if pos_type == "boolean" and cand_type == "boolean":
                return await self._compare_boolean_names(pos_parsed, cand_parsed)

            # Boolean кросс-типовое сравнение - также сравниваем названия
            elif pos_type == "boolean" and cand_type in ["string", "multiple"]:
                return await self._compare_boolean_with_other_types(
                    pos_parsed, cand_parsed
                )

            elif pos_type in ["string", "multiple"] and cand_type == "boolean":
                return await self._compare_boolean_with_other_types(
                    cand_parsed, pos_parsed
                )

            # Numeric значения
            elif pos_type == "numeric" and cand_type == "numeric":
                return await self._compare_numeric_values(pos_parsed, cand_parsed)

            # String значения
            elif pos_type == "string" and cand_type == "string":
                return await self._compare_string_values(pos_parsed, cand_parsed)

            # Диапазоны
            elif pos_type == "range" and cand_type == "range":
                return await self._compare_ranges(pos_parsed, cand_parsed)

            # Numeric ↔ Range
            elif pos_type == "numeric" and cand_type == "range":
                return await self._value_in_range(pos_parsed, cand_parsed)

            elif pos_type == "range" and cand_type == "numeric":
                return await self._value_in_range(cand_parsed, pos_parsed)

            # Multiple значения
            elif pos_type == "multiple" or cand_type == "multiple":
                return await self._compare_multiple_values(pos_parsed, cand_parsed)

            return False

        except Exception as e:
            logger.error(f"Ошибка сравнения значений: {e}")
            return False

    async def _compare_boolean_names(self, pos_data: Dict, cand_data: Dict) -> bool:
        """Сравнение булевых атрибутов по названиям, а не по значениям"""
        try:
            pos_name = pos_data.get("name", "")
            cand_name = cand_data.get("name", "")

            if not pos_name or not cand_name:
                return False

            similarity = await self.vectorizer.compare_two_strings(pos_name, cand_name)
            logger.debug(
                f"Boolean names comparison: '{pos_name}' vs '{cand_name}' = {similarity}"
            )

            return similarity >= 0.8

        except Exception as e:
            logger.error(f"Ошибка сравнения названий булевых атрибутов: {e}")
            return False

    async def _compare_boolean_with_other_types(
        self, bool_data: Dict, other_data: Dict
    ) -> bool:
        """Сравнение булевого атрибута с другими типами по названию"""
        try:
            bool_name = bool_data.get("name", "")
            other_name = other_data.get("name", "")

            if not bool_name or not other_name:
                return False

            similarity = await self.vectorizer.compare_two_strings(
                bool_name, other_name
            )
            logger.debug(
                f"Boolean vs other type names: '{bool_name}' vs '{other_name}' = {similarity}"
            )

            return similarity >= 0.8

        except Exception as e:
            logger.error(f"Ошибка кросс-типового сравнения с boolean: {e}")
            return False

    async def _compare_boolean_values(self, pos_data: Dict, cand_data: Dict) -> bool:
        """Сравнение булевых значений"""
        try:
            pos_value = pos_data.get("value", {}).get("value")
            cand_value = cand_data.get("value", {}).get("value")

            pos_bool = self._normalize_boolean_value(pos_value)
            cand_bool = self._normalize_boolean_value(cand_value)

            if pos_bool is not None and cand_bool is not None:
                return pos_bool == cand_bool

            return False
        except Exception as e:
            logger.error(f"Ошибка сравнения булевых значений: {e}")
            return False

    def _normalize_boolean_value(self, value) -> bool:
        """Нормализация значения к булевому типу"""
        if isinstance(value, bool):
            return value

        if isinstance(value, str):
            true_values = {
                "да",
                "true",
                "yes",
                "есть",
                "имеется",
                "1",
                "вкл",
                "включено",
            }
            false_values = {
                "нет",
                "false",
                "no",
                "отсутствует",
                "не имеется",
                "0",
                "выкл",
                "выключено",
            }

            normalized = value.lower().strip()
            if normalized in true_values:
                return True
            elif normalized in false_values:
                return False

        if isinstance(value, (int, float)):
            return bool(value)

        return None

    async def _compare_string_values(self, pos_data: Dict, cand_data: Dict) -> bool:
        """Сравнение строковых значений"""
        try:
            pos_value = str(pos_data.get("value", {}).get("value", ""))
            cand_value = str(cand_data.get("value", {}).get("value", ""))

            similarity = await self.vectorizer.compare_two_strings(
                pos_value, cand_value
            )
            logger.info(similarity >= 0.8)
            return similarity >= 0.8

        except Exception as e:
            logger.error(f"Ошибка сравнения строковых значений: {e}")
            return False

    async def _compare_numeric_values(self, pos_data: Dict, cand_data: Dict) -> bool:
        """Сравнение числовых значений с учетом единиц измерения"""
        pos_value = pos_data.get("value", {}).get("value")
        cand_value = cand_data.get("value", {}).get("value")
        pos_unit = pos_data.get("value", {}).get("unit")
        cand_unit = cand_data.get("value", {}).get("unit")

        try:
            if pos_unit == cand_unit:
                tolerance = 0.1
                return (
                    abs(pos_value - cand_value) / max(pos_value, cand_value, 1)
                    <= tolerance
                )

            if pos_unit and cand_unit:
                pos_normalized = await self.unit_normalizer.normalize_unit(
                    str(pos_value), pos_unit
                )
                cand_normalized = await self.unit_normalizer.normalize_unit(
                    str(cand_value), cand_unit
                )

                if pos_normalized.get("success") and cand_normalized.get("success"):
                    pos_norm_val = pos_normalized.get("normalized_value")
                    cand_norm_val = cand_normalized.get("normalized_value")

                    if pos_norm_val is not None and cand_norm_val is not None:
                        tolerance = 0.1
                        return (
                            abs(pos_norm_val - cand_norm_val)
                            / max(pos_norm_val, cand_norm_val, 1)
                            <= tolerance
                        )

            return False

        except Exception as e:
            logger.error(f"Ошибка сравнения числовых значений: {e}")
            return False

    async def _compare_ranges(self, pos_data: Dict, cand_data: Dict) -> bool:
        """Сравнение диапазонов"""
        try:
            pos_range = pos_data.get("value", [])
            cand_range = cand_data.get("value", [])

            logger.critical(f'pos_range: {pos_range}')
            logger.critical(f'cand_range: {cand_range}')

            # Нормализация единиц для диапазонов
            pos_unit = pos_range[0].get("unit") if pos_range else None
            cand_unit = cand_range[0].get("unit") if cand_range else None

            if (
                pos_unit != cand_unit
                and pos_unit
                and cand_unit
                and self.unit_normalizer
            ):
                try:
                    for i, item in enumerate(pos_range):
                        if isinstance(item.get("value"), (int, float)):
                            norm_result = await self.unit_normalizer.normalize_unit(
                                str(item["value"]), pos_unit
                            )
                            if norm_result.get("success"):
                                pos_range[i] = {
                                    "value": norm_result["normalized_value"],
                                    "unit": norm_result["normalized_unit"],
                                }

                    for i, item in enumerate(cand_range):
                        if isinstance(item.get("value"), (int, float)):
                            norm_result = await self.unit_normalizer.normalize_unit(
                                str(item["value"]), cand_unit
                            )
                            if norm_result.get("success"):
                                cand_range[i] = {
                                    "value": norm_result["normalized_value"],
                                    "unit": norm_result["normalized_unit"],
                                }
                except Exception as e:
                    logger.error(f"Error normalizing range units: {e}")

            if len(pos_range) < 2 or len(cand_range) < 2:
                return False

            pos_start = pos_range[0].get("value")
            pos_end = pos_range[1].get("value")
            cand_start = cand_range[0].get("value")
            cand_end = cand_range[1].get("value")

            if pos_start == "_inf-":
                pos_start = float("-inf")
            if pos_end == "_inf+":
                pos_end = float("inf")
            if cand_start == "_inf-":
                cand_start = float("-inf")
            if cand_end == "_inf+":
                cand_end = float("inf")

            return pos_start <= cand_end and cand_start <= pos_end

        except Exception as e:
            logger.error(f"Ошибка сравнения диапазонов: {e}")
            return False

    @staticmethod
    async def _value_in_range(value_data: Dict, range_data: Dict) -> bool:
        """Проверка входит ли значение в диапазон"""
        try:
            value = value_data.get("value", {}).get("value")
            range_vals = range_data.get("value", [])

            if len(range_vals) < 2:
                return False
            # Нормализация единиц для значения в диапазоне
            value_unit = value_data.get("value", {}).get("unit")
            range_unit = range_vals[0].get("unit") if range_vals else None

            if (
                value_unit != range_unit
                and value_unit
                and range_unit
                and hasattr(value_data, "unit_normalizer")
            ):
                try:
                    if isinstance(value, (int, float)):
                        norm_result = await value_data.unit_normalizer.normalize_unit(
                            str(value), value_unit
                        )
                        if norm_result.get("success"):
                            value = norm_result["normalized_value"]

                    for i, item in enumerate(range_vals):
                        if isinstance(item.get("value"), (int, float)):
                            norm_result = (
                                await value_data.unit_normalizer.normalize_unit(
                                    str(item["value"]), range_unit
                                )
                            )
                            if norm_result.get("success"):
                                range_vals[i] = {
                                    "value": norm_result["normalized_value"],
                                    "unit": norm_result["normalized_unit"],
                                }
                except Exception as e:
                    logger.error(f"Error normalizing value-range units: {e}")

            start = range_vals[0].get("value")
            end = range_vals[1].get("value")

            if start == "_inf-":
                start = float("-inf")
            if end == "_inf+":
                end = float("inf")

            return start <= value <= end

        except Exception as e:
            logger.error(f"Ошибка проверки значения в диапазоне: {e}")
            return False

    async def _compare_multiple_values(self, pos_data: Dict, cand_data: Dict) -> bool:
        """Сравнение множественных значений"""
        try:
            pos_values = pos_data.get("value", [])
            cand_values = cand_data.get("value", [])

            if not isinstance(pos_values, list):
                pos_values = [pos_values]
            if not isinstance(cand_values, list):
                cand_values = [cand_values]

            for pos_val in pos_values:
                pos_val_str = str(pos_val.get("value", pos_val)).lower()
                for cand_val in cand_values:
                    cand_val_str = str(cand_val.get("value", cand_val)).lower()

                    similarity = await self.vectorizer.compare_two_strings(
                        pos_val_str, cand_val_str
                    )
                    if similarity >= 0.8:
                        return True

            return False

        except Exception as e:
            logger.error(f"Ошибка сравнения множественных значений: {e}")
            return False

    async def _finalize_results(
        self,
        candidates: dict,
        processed_candidates: List[Dict],
        unmatched_characteristics: set,
        position: TenderPositions,
        min_required_points: int,
    ):
        """Финальная обработка результатов"""

        logger.warning("\n" + "=" * 60)
        logger.warning("ФИНАЛЬНЫЕ РЕЗУЛЬТАТЫ")
        logger.warning("=" * 60)

        processed_candidates.sort(key=lambda x: x["points"], reverse=True)

        logger.info(f"🎯 Прошедших кандидатов: {len(processed_candidates)}")
        logger.info(f"❌ Несметченных характеристик: {len(unmatched_characteristics)}")

        candidates["hits"]["hits"] = [
            item["candidate"] for item in processed_candidates
        ]

        for i, result in enumerate(processed_candidates):
            candidates["hits"]["hits"][i]["points"] = result["points"]
            candidates["hits"]["hits"][i]["matched_attributes"] = result[
                "matched_attributes"
            ]
            candidates["hits"]["hits"][i]["unmatched_attributes"] = result[
                "unmatched_attributes"
            ]
            candidates["hits"]["hits"][i]["attribute_matching_details"] = result[
                "attribute_matching_details"
            ]

        # Создаем расширенный отчет
        report = {
            "position_title": position.title,
            "total_candidates_processed": len(processed_candidates),
            "min_required_points": min_required_points,
            "max_possible_points": len(position.attributes),
            "unmatched_characteristics": list(unmatched_characteristics),
            "attribute_type_analysis": self._analyze_attribute_types(
                processed_candidates
            ),
            "top_candidates": [
                {
                    "title": result["candidate"]["_source"]["title"],
                    "points": result["points"],
                    "matched_attributes": result["matched_attributes"],
                    "unmatched_attributes": result["unmatched_attributes"],
                    "attribute_matching_details": result["attribute_matching_details"],
                }
                for result in processed_candidates[:10]
            ],
        }

        report_filename = f"shrinking_report_{position.id}_{int(time.time())}.json"
        with open(report_filename, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        logger.info(f"📄 Отчет сохранен: {report_filename}")

        logger.info("\n🏆 ТОП КАНДИДАТОВ:")
        for i, result in enumerate(processed_candidates[:5], 1):
            logger.info(
                f"{i}. {result['candidate']['_source']['title']} - {result['points']} баллов"
            )

        if unmatched_characteristics:
            logger.warning("\n❌ НЕСМЕТЧЕННЫЕ ХАРАКТЕРИСТИКИ:")
            for char in unmatched_characteristics:
                logger.warning(f"  • {char}")

    def _analyze_attribute_types(self, processed_candidates: List[Dict]) -> Dict:
        """Анализ эффективности матчинга по типам атрибутов"""
        type_analysis = {
            "boolean": {"total_matches": 0, "successful_matches": 0},
            "numeric": {"total_matches": 0, "successful_matches": 0},
            "string": {"total_matches": 0, "successful_matches": 0},
            "range": {"total_matches": 0, "successful_matches": 0},
            "multiple": {"total_matches": 0, "successful_matches": 0},
        }

        for candidate in processed_candidates:
            for match in candidate.get("matched_attributes", []):
                pos_type = match.get("position_attr_type", "unknown")
                if pos_type in type_analysis:
                    type_analysis[pos_type]["successful_matches"] += 1

        return type_analysis

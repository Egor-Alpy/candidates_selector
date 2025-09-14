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

        # Парсим атрибуты позиции
        position_parsed_attrs = await self._parse_position_attributes(
            position.attributes
        )

        if not position_parsed_attrs:
            logger.warning("❌ Нет атрибутов для сравнения")
            return

        # === ЭТАП 2: ОБРАБОТКА КАНДИДАТОВ ===
        logger.info(
            f"\n🔍 Начинаем обработку {len(candidates['hits']['hits'])} кандидатов"
        )

        processed_candidates = []
        unmatched_characteristics = set()

        for idx, candidate in enumerate(candidates["hits"]["hits"]):
            logger.info(
                f"\n--- Кандидат {idx + 1}: {candidate['_source']['title']} ---"
            )

            result = await self._process_single_candidate(
                candidate,
                position_parsed_attrs,
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

        # logger.critical(f'Уходим в бесконечный сон, чтобы не завершить таску кролика...')
        # while True:
        #     time.sleep(100000)

    async def _parse_position_attributes(self, attributes) -> List[Dict]:
        """Парсинг атрибутов позиции"""
        logger.info("\n📝 ПАРСИНГ АТРИБУТОВ ПОЗИЦИИ:")

        # 🔍 ДЕТАЛЬНАЯ ДИАГНОСТИКА attrs_sorter
        logger.info(
            f"🔧 attrs_sorter api_url: {getattr(self.attrs_sorter, 'api_url', 'NOT_SET')}"
        )

        # 📊 Информация о сырых атрибутах
        logger.info(f"📊 Total attributes count: {len(attributes)}")
        for i, attr in enumerate(attributes):
            logger.info(
                f"📊 Attr {i+1}: name='{attr.name}', value='{attr.value}', type='{getattr(attr, 'type', 'NO_TYPE')}', unit='{getattr(attr, 'unit', 'NO_UNIT')}'"
            )


        parsed_attrs = []

        for i, attr in enumerate(attributes):
            logger.info(f"\n--- АТРИБУТ {i+1}/{len(attributes)} ---")
            try:
                parsed = None
                try:
                    unit = attr.unit
                    raw_string = f"{attr.name}: {attr.value} {unit}"
                    logger.info(f"🔄 Raw request: {raw_string}")
                    parsed = await self.attrs_sorter.extract_attr_data(raw_string)
                    logger.info(f"🔄 Raw response: {parsed}")
                except Exception as e:
                    logger.error(f"failed: {e}")

                if parsed and len(parsed) > 0:
                    parsed_data = {
                        "original": attr,
                        "parsed": parsed[0],
                        "display_name": f"{attr.name}: {attr.value}",
                    }
                    parsed_attrs.append(parsed_data)

                    logger.info(f"  - Тип: {parsed[0].get('type', 'unknown')}")
                    logger.info(f"  - Значение: {parsed[0].get('value', 'unknown')}")
                else:
                    logger.warning(f"❌ Final parsed result: {parsed} | {attr.name}, {attr.value}")

            except Exception as e:
                logger.error(f"CRITICAL ERROR for '{attr.name}': {e}")
                logger.error(f"Exception type: {type(e)}")

        logger.info(f"\n📊 ИТОГОВАЯ СТАТИСТИКА:")
        logger.info(f"📊 Всего атрибутов: {len(attributes)}")
        logger.info(f"📊 Успешно распаршено: {len(parsed_attrs)}")
        logger.info(f"📊 Не удалось распарсить: {len(attributes) - len(parsed_attrs)}")

        return parsed_attrs

    async def _process_single_candidate(
        self,
        candidate: Dict,
        position_attrs: List[Dict],
        min_required_points: int,
        unmatched_characteristics: set,
    ) -> Optional[Dict]:
        """Обработка одного кандидата"""

        candidate_title = candidate["_source"]["title"]
        candidate_attrs = candidate["_source"].get("attributes", [])

        # Инициализация результата
        result = {
            "candidate": candidate,
            "points": 0,
            "max_points": len(position_attrs),
            "matched_attributes": [],
            "unmatched_attributes": [],
            "early_exit": False,
        }

        logger.info(f'🔎 Категория yandex: {candidate["_source"]["yandex_category"]} | Категория: {candidate["_source"]["category"]} | кол-во атрибутов: {len(candidate_attrs)}')

        # Парсим атрибуты кандидата
        candidate_parsed_attrs = await self._parse_candidate_attributes(candidate_attrs)
        logger.warning(f'candidate parsed attrs:')
        for attr in candidate_parsed_attrs:
            logger.warning(f'{attr}')

        # Проверяем каждый атрибут позиции
        for pos_attr in position_attrs:
            match_found = await self._find_attribute_match(
                pos_attr, candidate_parsed_attrs, result
            )

            if match_found:
                result["points"] += 1
                logger.info(f"  ✅ +1 балл за: {pos_attr['display_name']}")
            else:
                result["unmatched_attributes"].append(pos_attr["display_name"])
                unmatched_characteristics.add(pos_attr["display_name"])
                logger.info(f"  ❌ Не найдено: {pos_attr['display_name']}")

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

    async def _parse_candidate_attributes(
        self, candidate_attrs: List[Dict]
    ) -> List[Dict]:
        """Парсинг атрибутов кандидата (они уже предобработаны!)"""
        parsed_attrs = []

        logger.info(
            f"🔄 Parsing {len(candidate_attrs)} candidate attributes (already processed)"
        )

        for attr in candidate_attrs:
            try:
                # Атрибуты кандидатов УЖЕ ПРЕДОБРАБОТАНЫ в Elasticsearch
                # Используем готовые standardized поля
                standardized_name = attr.get(
                    "standardized_name", attr.get("original_name", "")
                )
                standardized_value = attr.get(
                    "standardized_value", attr.get("original_value", "")
                )
                if attr.get('attribute_type', 'simple') == 'simple':
                    standardized_unit = attr.get("standardized_unit")
                else:
                    standardized_unit = attr.get("standardized_value")[0].get("unit")

                attribute_type = attr.get("attribute_type", "simple")

                # Создаем структуру совместимую с attrs_sorter
                parsed_structure = {
                    "name": standardized_name,
                    "type": attribute_type,
                    "value": self._convert_to_attrs_sorter_format(
                        standardized_value, standardized_unit, attribute_type
                    ),
                }

                parsed_attrs.append(
                    {
                        "original": attr,
                        "parsed": parsed_structure,
                        "display_name": f"{standardized_name}: {standardized_value}",
                    }
                )

                logger.debug(
                    f"✅ Converted: {standardized_name} = {standardized_value} (type: {attribute_type})"
                )

            except Exception as e:
                logger.error(f"💥 Ошибка конвертации атрибута кандидата: {e}")

        logger.info(f"📊 Converted {len(parsed_attrs)} candidate attributes")
        return parsed_attrs

    def _convert_to_attrs_sorter_format(self, value, unit, attr_type):
        """Конвертация предобработанных атрибутов в формат attrs_sorter"""
        try:
            if attr_type == "simple":
                return {"value": value, "unit": unit}

            elif attr_type == "range":
                # value уже может быть списком диапазона
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
                    # Fallback: превращаем в диапазон
                    return [
                        {"value": value, "unit": unit},
                        {"value": value, "unit": unit},
                    ]

            elif attr_type == "multiple":
                # value уже может быть списком
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
                # Неизвестный тип - возвращаем как simple
                return {"value": value, "unit": unit}

        except Exception as e:
            logger.error(f"Error converting value {value}: {e}")
            return {"value": str(value), "unit": unit}

    async def _find_attribute_match(
        self, pos_attr: Dict, candidate_attrs: List[Dict], result: Dict
    ) -> bool:
        """Поиск совпадения атрибута позиции с атрибутами кандидата"""

        pos_type = pos_attr["parsed"].get("type")
        pos_name = pos_attr["parsed"].get("name", "")

        for cand_attr in candidate_attrs:
            cand_type = cand_attr["parsed"].get("type")
            cand_name = cand_attr["parsed"].get("name", "")

            # Проверка совместимости по названию
            name_similarity = await self._check_name_similarity(pos_name, cand_name)
            # logger.warning(name_similarity)
            if name_similarity < 0.6:  # Порог схожести названий
                continue

            # Проверка совместимости по типу и значению
            value_match = await self._check_value_compatibility(
                pos_attr["parsed"], cand_attr["parsed"]
            )

            if value_match:
                result["matched_attributes"].append(
                    {
                        "position_attr": pos_attr["display_name"],
                        "candidate_attr": cand_attr["display_name"],
                        "name_similarity": name_similarity,
                        "match_type": f"{pos_type} vs {cand_type}",
                    }
                )
                return True

        return False

    async def _check_name_similarity(self, name1: str, name2: str) -> float:
        """Проверка схожести названий атрибутов"""
        try:
            if not name1 or not name2:
                return 0.0

            # Используем векторизатор для оценки схожести
            similarity = await self.vectorizer.compare_two_strings(name1, name2)
            return similarity

        except Exception as e:
            logger.error(f"Ошибка сравнения названий: {e}")
            return 0.0

    async def _check_value_compatibility(
        self, pos_parsed: Dict, cand_parsed: Dict
    ) -> bool:
        """Проверка совместимости значений атрибутов"""
        pos_type = pos_parsed.get("type")
        cand_type = cand_parsed.get("type")

        try:
            # Простые значения
            if pos_type == "simple" and cand_type == "simple":
                return await self._compare_simple_values(pos_parsed, cand_parsed)

            # Диапазоны
            elif pos_type == "range" and cand_type == "range":
                return await self._compare_ranges(pos_parsed, cand_parsed)

            # Значение в диапазоне
            elif pos_type == "simple" and cand_type == "range":
                return await self._value_in_range(pos_parsed, cand_parsed)

            elif pos_type == "range" and cand_type == "simple":
                return await self._value_in_range(cand_parsed, pos_parsed)

            # Множественные значения
            elif "multiple" in [pos_type, cand_type]:
                return await self._compare_multiple_values(pos_parsed, cand_parsed)

            return False

        except Exception as e:
            logger.error(f"Ошибка сравнения значений: {e}")
            return False

    async def _compare_simple_values(self, pos_data: Dict, cand_data: Dict) -> bool:
        """Сравнение простых значений"""
        pos_value = pos_data.get("value", {}).get("value")
        cand_value = cand_data.get("value", {}).get("value")

        if isinstance(pos_value, str) and isinstance(cand_value, str):
            # Текстовое сравнение
            similarity = await self.vectorizer.compare_two_strings(
                pos_value, cand_value
            )
            return similarity >= 0.8

        elif isinstance(pos_value, (int, float)) and isinstance(
            cand_value, (int, float)
        ):
            # Числовое сравнение
            return await self._compare_numeric_values(pos_data, cand_data)

        elif isinstance(pos_value, bool) and isinstance(cand_value, bool):
            # Булево сравнение
            return pos_value == cand_value

        return False

    async def _compare_numeric_values(self, pos_data: Dict, cand_data: Dict) -> bool:
        """Сравнение числовых значений с учетом единиц измерения"""
        pos_value = pos_data.get("value", {}).get("value")
        cand_value = cand_data.get("value", {}).get("value")
        pos_unit = pos_data.get("value", {}).get("unit")
        cand_unit = cand_data.get("value", {}).get("unit")

        try:
            # Если единицы одинаковые или отсутствуют
            if pos_unit == cand_unit:
                tolerance = 0.1  # 10% допуск
                return (
                    abs(pos_value - cand_value) / max(pos_value, cand_value, 1)
                    <= tolerance
                )

            # Если единицы разные - нормализуем
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

            if len(pos_range) < 2 or len(cand_range) < 2:
                return False

            pos_start = pos_range[0].get("value")
            pos_end = pos_range[1].get("value")
            cand_start = cand_range[0].get("value")
            cand_end = cand_range[1].get("value")

            # Обработка бесконечностей
            if pos_start == "_inf-":
                pos_start = float("-inf")
            if pos_end == "_inf+":
                pos_end = float("inf")
            if cand_start == "_inf-":
                cand_start = float("-inf")
            if cand_end == "_inf+":
                cand_end = float("inf")

            # Проверка пересечения диапазонов
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

            # Приводим к единому формату
            if not isinstance(pos_values, list):
                pos_values = [pos_values]
            if not isinstance(cand_values, list):
                cand_values = [cand_values]

            # Ищем пересечения
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

        # Сортировка по убыванию баллов
        processed_candidates.sort(key=lambda x: x["points"], reverse=True)

        logger.info(f"🎯 Прошедших кандидатов: {len(processed_candidates)}")
        logger.info(f"❌ Несметченных характеристик: {len(unmatched_characteristics)}")

        # Обновляем исходный список кандидатов
        candidates["hits"]["hits"] = [
            item["candidate"] for item in processed_candidates
        ]

        # Добавляем баллы в каждого кандидата
        for i, result in enumerate(processed_candidates):
            candidates["hits"]["hits"][i]["points"] = result["points"]
            candidates["hits"]["hits"][i]["matched_attributes"] = result[
                "matched_attributes"
            ]
            candidates["hits"]["hits"][i]["unmatched_attributes"] = result[
                "unmatched_attributes"
            ]

        # Создаем отчет
        report = {
            "position_title": position.title,
            "total_candidates_processed": len(processed_candidates),
            "min_required_points": min_required_points,
            "max_possible_points": len(position.attributes),
            "unmatched_characteristics": list(unmatched_characteristics),
            "top_candidates": [
                {
                    "title": result["candidate"]["_source"]["title"],
                    "points": result["points"],
                    "matched_attributes": result["matched_attributes"],
                    "unmatched_attributes": result["unmatched_attributes"],
                }
                for result in processed_candidates[:10]  # Топ 10
            ],
        }

        # Сохраняем отчет
        report_filename = f"shrinking_report_{position.id}_{int(time.time())}.json"
        with open(report_filename, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        logger.info(f"📄 Отчет сохранен: {report_filename}")

        # Красивый вывод топа
        logger.info("\n🏆 ТОП КАНДИДАТОВ:")
        for i, result in enumerate(processed_candidates[:5], 1):
            logger.info(
                f"{i}. {result['candidate']['_source']['title']} - {result['points']} баллов"
            )

        if unmatched_characteristics:
            logger.warning("\n❌ НЕСМЕТЧЕННЫЕ ХАРАКТЕРИСТИКИ:")
            for char in unmatched_characteristics:
                logger.warning(f"  • {char}")

from typing import Optional, List, Dict

from app.core.logger import get_logger

from app.services.attrs_standardizer import AttrsStandardizer
from app.services.unit_standardizer import UnitStandardizer

logger = get_logger(name=__name__)


class ShrinkerPositions:
    def __init__(self):
        self.attrs_sorter = AttrsStandardizer()
        self.unit_normalizer = UnitStandardizer()

    async def parse_position_attributes(self, attributes) -> Dict:
        """Парсинг атрибутов позиции с группировкой по типам"""
        logger.info("Этап 1/3: ПАРСИНГ АТРИБУТОВ ПОЗИЦИИ:")
        logger.info(f"на вход получаем attributes позиции: {attributes}")

        # Инициализация групп
        attrs_data = {"attrs": []}

        for i, attr in enumerate(attributes):
            logger.info(f"--- АТРИБУТ ПОЗИЦИИ {i+1}/{len(attributes)} ---")

            try:
                parsed = None
                try:
                    # Распаршиваем характеристику позиции
                    unit = getattr(attr, "unit", "") or ""
                    raw_string = f"{attr.name}: {attr.value} {unit}".strip()
                    parsed = await self.attrs_sorter.extract_attr_data(raw_string)

                    logger.info(f"1 | распаршиваем характеристику позиции\nunit: {unit}\nraw_string: {raw_string}\nparsed: {parsed}")

                except Exception as e:
                    logger.error(f"failed: {e}")

                # Определяем тип разобранной/распаршенной характеристики позиции
                if parsed and len(parsed) > 0:
                    parsed = parsed[0]

                    # Определяем подтип для simple значений
                    parsed_type = parsed.get("type", "simple")
                    if parsed_type == "simple":
                        value = parsed.get("value", {}).get("value")
                        final_type = self._determine_value_subtype(value)
                    else:
                        final_type = parsed_type

                    # Стандартизация единиц измерения если она есть
                    normalized_parsed = parsed.copy()
                    normalized_parsed['original_name'] = attr.name
                    normalized_parsed['original_value'] = attr.value
                    normalized_parsed['original_unit'] = attr.unit
                    normalized_parsed['pg_id'] = attr.id
                    normalized_parsed['type'] = final_type
                    normalized_parsed = await self._standardize_units_and_values(final_type=final_type, parsed=parsed, normalized_parsed=normalized_parsed)
                    logger.info(f'🔄 Parsed response: {normalized_parsed}')

                    attrs_data["attrs"].append(normalized_parsed)
                else:
                    logger.warning(f"❌ Final parsed result: {parsed} | {attr.name}, {attr.value}")

            except Exception as e:
                logger.error(f"CRITICAL ERROR for '{attr.name}': {e}")
                logger.error(f"Exception type: {type(e)}")

        # Логирование статистики позиции
        logger.info(f"\nИтоговая статистика позиции:\n"
                    f"Всего атрибутов: {len(attributes)}\n"
                    f"Успешно распаршено: {len(attrs_data['attrs'])}\n")

        return attrs_data

    @staticmethod
    def _determine_value_subtype(value) -> str:
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

    async def _standardize_units_and_values(self, final_type, parsed, normalized_parsed):
        if final_type == "numeric":
            value = parsed.get("value", {}).get("unit")
            unit = parsed.get("value", {}).get("value")

            if unit and isinstance(value, (int, float)):
                try:
                    normalized_result = await self.unit_normalizer.normalize_unit(
                        str(value), unit
                    )

                    if normalized_result.get("success", False):
                        # Обновляем данные нормализованными значениями
                        normalized_parsed["value"]["value"] = normalized_result.get(
                            "base_value", value
                        )
                        normalized_parsed["value"]["unit"] = normalized_result.get(
                            "base_unit", unit
                        )
                    else:
                        logger.warning(
                            f"⚠️ Unit normalization failed for {value} {unit}"
                        )

                except Exception as e:
                    logger.error(f"💥 Error normalizing unit: {e}")

        elif final_type == "range":
            range_values = parsed.get("value", [])
            if len(range_values) == 2:
                for j, range_item in enumerate(range_values):
                    unit = range_item.get("unit")
                    value = range_item.get("value")

                    if (
                        unit
                        and self.unit_normalizer
                        and (
                            isinstance(value, (int, float))
                            or value in ["_inf+", "_inf-"]
                        )
                    ):
                        try:
                            if value in ["_inf+", "_inf-"]:
                                # Для бесконечных значений стандартизируем только единицу, значение не трогаем
                                normalized_result = (
                                    await self.unit_normalizer.normalize_unit("1", unit)
                                )
                                if normalized_result.get("success", False):
                                    normalized_parsed["value"][j]["unit"] = (
                                        normalized_result.get("base_unit", unit)
                                    )
                            else:
                                # Для обычных чисел стандартизируем и значение, и единицу
                                normalized_result = (
                                    await self.unit_normalizer.normalize_unit(
                                        str(value), unit
                                    )
                                )
                                if normalized_result.get("success", False):
                                    normalized_parsed["value"][j]["value"] = (
                                        normalized_result.get("base_value", value)
                                    )
                                    normalized_parsed["value"][j]["unit"] = (
                                        normalized_result.get("base_unit", unit)
                                    )
                        except Exception as e:
                            logger.error(f"Error normalizing range unit: {e}")
        return normalized_parsed

import inspect
import logging
import os


class ContextLogger:
    def __init__(self, format: str, project_name: str, level: int):
        self.logger = self.setup_logger(format=format, logger_name=project_name, level=level)

    @staticmethod
    def setup_logger(
            format='%(asctime).19s | %(levelname).3s | %(message)s',
            logger_name='base_logger',
            level: int = logging.DEBUG
        ) -> logging.Logger:
        """Настройка логера"""

        # Настраиваем basicConfig только один раз для корневого логера
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=level,
                format=format,
            )
        logger = logging.getLogger(name=logger_name)
        logging.getLogger('elasticsearch').setLevel(logging.CRITICAL)

        logger.setLevel(level)

        return logger

    def _colorize(self, message, color_code):
        """Простое окрашивание сообщений"""
        return f"\033[{color_code}m{message}\033[0m"

    def debug(self, message):
        frame = inspect.currentframe().f_back
        function_name = f'{inspect.currentframe().f_back.f_code.co_name}'
        module_name = os.path.basename(inspect.getmodule(frame).__file__)

        colored_message = self._colorize(f"{module_name} | {function_name} | {message}", "37")  # белый
        self.logger.debug(colored_message)

    def info(self, message):
        frame = inspect.currentframe().f_back
        function_name = f'{inspect.currentframe().f_back.f_code.co_name}'
        module_name = os.path.basename(inspect.getmodule(frame).__file__)

        colored_message = self._colorize(f"{module_name} | {function_name} | {message}", "32")  # зеленый
        self.logger.info(colored_message)

    def warning(self, message):
        frame = inspect.currentframe().f_back
        function_name = f'{inspect.currentframe().f_back.f_code.co_name}'
        module_name = os.path.basename(inspect.getmodule(frame).__file__)

        colored_message = self._colorize(f"{module_name} | {function_name} | {message}", "33")  # желтый
        self.logger.warning(colored_message)

    def error(self, message):
        frame = inspect.currentframe().f_back
        function_name = f'{inspect.currentframe().f_back.f_code.co_name}'
        module_name = os.path.basename(inspect.getmodule(frame).__file__)

        colored_message = self._colorize(f"{module_name} | {function_name} | {message}", "31")  # красный
        self.logger.error(colored_message)

    def critical(self, message):
        frame = inspect.currentframe().f_back
        function_name = f'{inspect.currentframe().f_back.f_code.co_name}'
        module_name = os.path.basename(inspect.getmodule(frame).__file__)

        colored_message = self._colorize(f"{module_name} | {function_name} | {message}", "1;31")  # жирный красный
        self.logger.critical(colored_message)
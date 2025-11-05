# logging_config.py
import logging


def configure_logging(logging_level: int):
    logging.basicConfig(level=logging_level,
                        format="%(asctime)s - %(levelname)s - %(message)s",
                        handlers = [
                            logging.FileHandler("/var/log/grin-siphon/grin_siphon.log"),
                            logging.StreamHandler()
                        ])

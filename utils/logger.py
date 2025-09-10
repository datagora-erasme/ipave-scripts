import logging
import sys
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler


class MaxLevelFilter(logging.Filter):
    """N'accepte que les records dont le levelno <= max_level."""

    def __init__(self, max_level: int) -> None:
        super().__init__()
        self.max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= self.max_level


def _to_level(level: int | str) -> int:
    """Convertit un nom de niveau (ex: 'INFO') ou un int en int."""
    if isinstance(level, int):
        return level
    if isinstance(level, str):
        return logging._nameToLevel.get(level.upper(), logging.INFO)
    return logging.INFO


def setup_logger(
    name: str,
    level: int | str = logging.INFO,
    info_log_file: str | None = None,
    error_log_file: str | None = None,
    when: str = "midnight",
    backup_count: int = 14,
    utc: bool = False,
) -> logging.Logger:
    """
    Configure et retourne un logger avec rotation journalière.

    - Console: affiche tout à partir de `level`
    - info.log: INFO et WARNING (pas d'ERROR/CRITICAL), rotation journalière
    - error.log: ERROR et CRITICAL, rotation journalière

    :param name: Nom du logger (souvent __name__).
    :param level: Niveau global (str ou int).
    :param info_log_file: Chemin fichier pour les infos (None pour désactiver).
    :param error_log_file: Chemin fichier pour les erreurs (None pour désactiver).
    :param when: Clé de rotation TimedRotatingFileHandler (ex: 'midnight', 'D').
    :param backup_count: Nb de fichiers conservés.
    :param utc: True = timestamps de rotation en UTC, False = localtime.
    """
    lvl = _to_level(level)
    logger = logging.getLogger(name)
    logger.setLevel(lvl)
    logger.propagate = False  # éviter la double écriture vers le root

    # Éviter de ré-attacher des handlers si déjà configuré
    if logger.handlers:
        return logger

    # Formatter
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s - %(filename)s:%(lineno)d: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # --- Console ---
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(lvl)
    console.setFormatter(formatter)
    logger.addHandler(console)

    # --- info.log (<= WARNING) ---
    if info_log_file:
        Path(info_log_file).parent.mkdir(parents=True, exist_ok=True)
        info_handler = TimedRotatingFileHandler(
            filename=info_log_file,
            when=when,
            interval=1,
            backupCount=backup_count,
            encoding="utf-8",
            utc=utc,
        )
        info_handler.setLevel(logging.INFO)
        info_handler.setFormatter(formatter)
        # n'accepte que INFO et WARNING (<= WARNING)
        info_handler.addFilter(MaxLevelFilter(logging.WARNING))
        logger.addHandler(info_handler)

    # --- error.log (>= ERROR) ---
    if error_log_file:
        Path(error_log_file).parent.mkdir(parents=True, exist_ok=True)
        error_handler = TimedRotatingFileHandler(
            filename=error_log_file,
            when=when,
            interval=1,
            backupCount=backup_count,
            encoding="utf-8",
            utc=utc,
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        logger.addHandler(error_handler)

    return logger

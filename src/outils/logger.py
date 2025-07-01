# -*- coding: utf-8 -*-
"""
Module de gestion de logs pour le projet.
"""
import logging
import sys

def get_logger(name: str) -> logging.Logger:
    """
    Renvoie un logger configuré avec un StreamHandler sur stdout
    et un format : date — module — niveau — message.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        fmt = "%(asctime)s — %(name)s — %(levelname)s — %(message)s"
        handler.setFormatter(logging.Formatter(fmt))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

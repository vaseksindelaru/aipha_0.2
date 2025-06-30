# aipha/data_system/templates/__init__.py

from .base_data_request_template import BaseDataRequestTemplate
from .klines_data_request_template import KlinesDataRequestTemplate

__all__ = [
    "BaseDataRequestTemplate",
    "KlinesDataRequestTemplate"
]
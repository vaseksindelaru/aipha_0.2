# aipha/data_system/templates/klines_data_request_template.py

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Optional, Dict, Any, List, Type
import logging
# Mantendré la importación absoluta que pusimos antes
from aipha.data_system.templates.base_data_request_template import BaseDataRequestTemplate

logger = logging.getLogger(__name__)

@dataclass(eq=False)
class KlinesDataRequestTemplate(BaseDataRequestTemplate):
    """Plantilla específica para solicitar datos de klines (velas) de Binance."""
    TEMPLATE_TYPE: str = field(default="klines", init=False, repr=False)

    # --- CORRECCIÓN DE ORDEN AQUÍ ---
    # Primero, TODOS los campos que NO tienen valor por defecto
    name: str
    symbol: str
    interval: str
    start_date: date
    end_date: date

    # Después, TODOS los campos que SÍ tienen valor por defecto
    description: Optional[str] = None

    def __post_init__(self):
        # Esta parte no necesita cambios, ahora que el orden de los campos es correcto.
        super().__init__(self.name, self.description)
        
        if not self.symbol or not self.symbol.strip():
            raise ValueError("El símbolo (symbol) no puede estar vacío.")
        if self.end_date < self.start_date:
            raise ValueError(f"La fecha final ({self.end_date}) no puede ser anterior a la inicial ({self.start_date}).")

    @property
    def template_type(self) -> str:
        return self.TEMPLATE_TYPE

    # ... (el resto de los métodos: to_dict, _deserialize_specific, get_date_range) ...
    # No necesitan cambios. Aquí los incluyo por completitud.

    def to_dict(self) -> Dict[str, Any]:
        return {
            "template_type": self.template_type,
            "name": self.name,
            "description": self.description,
            "symbol": self.symbol,
            "interval": self.interval,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
        }

    @classmethod
    def _deserialize_specific(cls: Type['KlinesDataRequestTemplate'], data: Dict[str, Any]) -> 'KlinesDataRequestTemplate':
        try:
            return cls(
                name=data['name'],
                symbol=data['symbol'],
                interval=data['interval'],
                start_date=date.fromisoformat(data['start_date']),
                end_date=date.fromisoformat(data['end_date']),
                description=data.get('description'), # Los argumentos con valor por defecto van al final
            )
        except KeyError as e:
            raise ValueError(f"Datos incompletos para KlinesDataRequestTemplate: falta {e}") from e

    def get_date_range(self) -> List[date]:
        dates = []
        current_date = self.start_date
        while current_date <= self.end_date:
            dates.append(current_date)
            current_date += timedelta(days=1)
        return dates
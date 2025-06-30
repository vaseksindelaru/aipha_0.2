# aipha/data_system/templates/base_data_request_template.py

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, Type, TypeVar, ClassVar
import logging

logger = logging.getLogger(__name__)

T_BaseTemplate = TypeVar('T_BaseTemplate', bound='BaseDataRequestTemplate')

class BaseDataRequestTemplate(ABC):
    """Clase base abstracta para todas las plantillas de solicitud de datos."""
    _registry: ClassVar[Dict[str, Type['BaseDataRequestTemplate']]] = {}

    name: str
    description: Optional[str]

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        template_type_name = getattr(cls, 'TEMPLATE_TYPE', None)
        if template_type_name:
            cls._registry[template_type_name] = cls
            logger.debug(f"Plantilla tipo '{template_type_name}' registrada para la clase {cls.__name__}.")

    def __init__(self, name: str, description: Optional[str] = None):
        if not name or not name.strip():
            raise ValueError("El nombre de la plantilla (name) no puede estar vacío.")
        self.name = name
        self.description = description

    @property
    @abstractmethod
    def template_type(self) -> str:
        pass

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        pass

    @classmethod
    def from_dict(cls: Type[T_BaseTemplate], data: Dict[str, Any]) -> T_BaseTemplate:
        template_type_name = data.get('template_type')
        if not template_type_name:
            raise ValueError("El diccionario debe contener 'template_type'.")
        
        subclass = cls._registry.get(template_type_name)
        if not subclass:
            raise ValueError(f"Tipo de plantilla desconocido o no registrado: '{template_type_name}'.")
        
        if hasattr(subclass, '_deserialize_specific'):
            return subclass._deserialize_specific(data) # type: ignore
        else:
            raise NotImplementedError(f"La subclase {subclass.__name__} no implementa _deserialize_specific.")
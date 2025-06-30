# aipha/data_system/template_manager.py

import json
import os
import logging
from typing import Dict, Optional, List
from aipha.data_system.templates import BaseDataRequestTemplate
from aipha.data_system.templates import KlinesDataRequestTemplate  # noqa: F401

logger = logging.getLogger(__name__)

class DataRequestTemplateManager:
    """Gestiona una colección de plantillas de solicitud de datos."""
    def __init__(self, filepath: str = "project_data_templates.json"):
        self._templates: Dict[str, BaseDataRequestTemplate] = {}
        self._templates_filepath = filepath
        self.load_templates()

    def load_templates(self) -> None:
        if not os.path.exists(self._templates_filepath):
            logger.info(f"Archivo de plantillas '{self._templates_filepath}' no encontrado. Iniciando vacío.")
            return

        logger.info(f"Cargando plantillas desde '{self._templates_filepath}'...")
        try:
            with open(self._templates_filepath, 'r', encoding='utf-8') as f:
                data_from_file = json.load(f)
            
            for name, data_dict in data_from_file.items():
                try:
                    template_obj = BaseDataRequestTemplate.from_dict(data_dict)
                    self._templates[name] = template_obj
                except (ValueError, NotImplementedError) as e:
                    logger.error(f"Error al cargar plantilla '{name}': {e}")
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Error al leer o decodificar el archivo de plantillas: {e}")

    def save_templates(self) -> bool:
        logger.info(f"Guardando {len(self._templates)} plantillas en '{self._templates_filepath}'...")
        templates_to_save = {name: template.to_dict() for name, template in self._templates.items()}
        try:
            with open(self._templates_filepath, 'w', encoding='utf-8') as f:
                json.dump(templates_to_save, f, indent=4, ensure_ascii=False)
            logger.info("Plantillas guardadas exitosamente.")
            return True
        except (IOError, TypeError) as e:
            logger.error(f"Error al guardar plantillas: {e}", exc_info=True)
            return False

    def add_template(self, template: BaseDataRequestTemplate, overwrite: bool = False):
        if template.name in self._templates and not overwrite:
            logger.warning(f"Plantilla '{template.name}' ya existe. Use overwrite=True para reemplazar.")
            return False
        self._templates[template.name] = template
        logger.info(f"Plantilla '{template.name}' añadida/actualizada.")
        return True

    def get_template(self, name: str) -> Optional[BaseDataRequestTemplate]:
        return self._templates.get(name)

    def list_template_names(self) -> List[str]:
        return list(self._templates.keys())
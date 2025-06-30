# aipha/data_system/api_client.py

import requests
import os 
import logging
from typing import Any, Dict, Optional, Union

logger = logging.getLogger(__name__)

class ApiClient:
    """
    Un cliente genérico para realizar peticiones HTTP/HTTPS a diferentes APIs.
    Utiliza una sesión de requests para optimizar conexiones y configuraciones comunes.
    """
    
    def __init__(self, base_headers: Optional[Dict[str, str]] = None, timeout: int = 10):
        self._session = requests.Session()
        self._default_timeout = timeout
        
        if base_headers:
            self._session.headers.update(base_headers)
        
        logger.debug(
            f"ApiClient inicializado. Timeout por defecto: {self._default_timeout}s, "
            f"Cabeceras base de sesión: {self._session.headers}"
        )

    def make_request(
        self,
        url: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Union[Dict[str, Any], str]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        custom_headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None
    ) -> Optional[requests.Response]:
        """Realiza una petición HTTP a la URL especificada. La URL debe ser absoluta."""
        final_headers = self._session.headers.copy()
        if custom_headers:
            final_headers.update(custom_headers)

        request_timeout = timeout if timeout is not None else self._default_timeout
        
        logger.debug(
            f"Enviando petición: Method={method.upper()}, URL={url}, Timeout={request_timeout}\n"
            f"Params={params}, JSON Data={json_data if json_data else 'N/A'}\n"
            f"Headers={final_headers}"
        )
        
        try:
            response = self._session.request(
                method=method.upper(),
                url=url,
                params=params,
                data=data,
                json=json_data,
                headers=final_headers,
                timeout=request_timeout
            )
            response.raise_for_status()
            logger.info(f"Petición exitosa a {url}. Status: {response.status_code}")
            return response
        
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"Error HTTP para {url}: {http_err.response.status_code} - {http_err}")
            if http_err.response is not None:
                logger.debug(f"Respuesta del servidor (primeros 200 chars): {http_err.response.text[:200]}")
        except requests.exceptions.ConnectionError as conn_err:
            logger.error(f"Error de conexión para {url}: {conn_err}")
        except requests.exceptions.Timeout as timeout_err:
            logger.error(f"Timeout para {url} (después de {request_timeout}s): {timeout_err}")
        except requests.exceptions.RequestException as req_err:
            logger.error(f"Error de Request para {url}: {req_err}", exc_info=True)
        except Exception as e:
            logger.critical(f"Error crítico inesperado durante la petición a {url}: {e}", exc_info=True)
        
        return None

    def download_file(
        self,
        url: str,
        destination_path: str,
        custom_headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None
    ) -> bool:
        """Descarga un archivo desde una URL y lo guarda en la ruta de destino."""
        logger.info(f"Iniciando descarga de archivo desde '{url}' hacia '{destination_path}'")
        
        final_headers = self._session.headers.copy()
        if custom_headers:
            final_headers.update(custom_headers)
        
        request_timeout = timeout if timeout is not None else self._default_timeout
        
        logger.debug(
            f"Detalles de la descarga: URL={url}, Timeout={request_timeout}, Headers={final_headers}"
        )
        
        try:
            dest_dir = os.path.dirname(destination_path)
            if dest_dir:
                os.makedirs(dest_dir, exist_ok=True)
                logger.debug(f"Directorio de destino '{dest_dir}' asegurado.")

            with self._session.get(url, headers=final_headers, timeout=request_timeout, stream=True) as response:
                response.raise_for_status()
                with open(destination_path, 'wb') as f:
                    logger.debug(f"Escribiendo stream en '{destination_path}'...")
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            
            logger.info(f"Archivo descargado y guardado exitosamente en '{destination_path}'.")
            return True
        
        except (requests.exceptions.RequestException, IOError, OSError) as e:
            logger.error(f"Fallo durante la descarga o guardado de {url}: {e}", exc_info=True)
        
        if os.path.exists(destination_path):
            try:
                if os.path.getsize(destination_path) == 0:
                    os.remove(destination_path)
                    logger.warning(f"Archivo parcial o vacío eliminado: '{destination_path}'")
            except OSError as ose_remove:
                logger.error(f"Error al intentar eliminar el archivo parcial '{destination_path}': {ose_remove}")

        return False
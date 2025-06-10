# api_client.py
import requests
import os 
import logging
from typing import Any, Dict, Optional, Union # List no se usa aquí, pero estaba en el original

logger = logging.getLogger(__name__) # Usar logger de módulo

class ApiClient:
    """
    Un cliente genérico para realizar peticiones HTTP/HTTPS a diferentes APIs.
    Utiliza una sesión de requests para optimizar conexiones y configuraciones comunes.
    """
    
    def __init__(self, base_headers: Optional[Dict[str, str]] = None, timeout: int = 10): # base_url eliminado
        """
        Inicializa el cliente API.

        Args:
            base_headers (Optional[Dict[str, str]]): Headers por defecto para todas las peticiones.
            timeout (int): Tiempo máximo de espera para las peticiones en segundos.
        """
        self._session = requests.Session()
        self._default_timeout = timeout
        
        if base_headers:
            self._session.headers.update(base_headers)
        
        logger.debug( # Usando logger de módulo
            f"ApiClient inicializado. Timeout por defecto: {self._default_timeout}s, "
            f"Cabeceras base de sesión: {self._session.headers}"
        )

    def make_request(
        self,
        url: str, # Esta es ahora la URL completa
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Union[Dict[str, Any], str]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        custom_headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None
    ) -> Optional[requests.Response]:
        """
        Realiza una petición HTTP a la URL especificada. La URL debe ser absoluta.
        # ... (resto del docstring)
        """
        # logger.info(f"Método make_request llamado con: URL='{url}', Method='{method}'") # Este log ya lo teníamos

        final_headers = self._session.headers.copy() # Copia de las cabeceras de la sesión
        if custom_headers:
            final_headers.update(custom_headers)

        request_timeout = timeout if timeout is not None else self._default_timeout
        
        # Loguear los detalles de la petición
        logger.debug( # Usando logger de módulo
            f"Enviando petición: Method={method.upper()}, URL={url}, Timeout={request_timeout}\n" # Usar 'url' directamente
            f"Params={params}, JSON Data={json_data if json_data else 'N/A'}\n" # Mejor que 'None' para legibilidad del log
            f"Headers={final_headers}"
        )
        
        try:
            # Realizar la petición HTTP
            response = self._session.request(
                method=method.upper(),
                url=url, # Usar 'url' directamente
                params=params,
                data=data,
                json=json_data,
                headers=final_headers,
                timeout=request_timeout
            )
            
            response.raise_for_status() # Verificar si la respuesta HTTP fue un error (4xx o 5xx)
            
            logger.info(f"Petición exitosa a {url}. Status: {response.status_code}") # Usar 'url' directamente
            
            return response
        
        except requests.exceptions.HTTPError as http_err:
            logger.error( # Usando logger de módulo
                f"Error HTTP para {url}: {http_err.response.status_code} - {http_err}" # Usar 'url' directamente
            )
            if http_err.response is not None:
                logger.debug( # Usando logger de módulo
                    f"Contenido de la respuesta de error para {url} (primeros 200 caracteres): "
                    f"{http_err.response.text[:200]}"
                )
        except requests.exceptions.ConnectionError as conn_err:
            logger.error(f"Error de conexión para {url}: {conn_err}") # Usar 'url' directamente
        except requests.exceptions.Timeout as timeout_err:
            logger.error(
                f"Timeout para {url} (después de {request_timeout}s): {timeout_err}" # Usar 'url' directamente
            )
        except requests.exceptions.RequestException as req_err: # Captura otras excepciones de requests
            logger.error(
                f"Error de Request para {url}: {req_err}",  # Usar 'url' directamente
                exc_info=True # Incluir traceback para errores más genéricos de requests
            )
        except Exception as e: # Captura cualquier otra excepción inesperada
            logger.critical(
                f"Error crítico inesperado durante la petición a {url}: {e}", # Usar 'url' directamente
                exc_info=True # Incluir traceback
            )
        
        return None # Retornar None si alguna excepción fue capturada y manejada

    # ... (el método download_file se quedaría con su TODO por ahora)
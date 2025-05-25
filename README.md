# Triple Signals Detection

Este repositorio contiene el script `save_triple_signals.py` que identifica y guarda señales de trading basadas en tres criterios clave:

1. **Velas clave** (según criterios de volumen y tamaño de cuerpo)
2. **Zonas de acumulación**
3. **Mini-tendencias**

## Requisitos

- Python 3.7+
- mysql-connector-python
- python-dotenv

## Configuración

1. Copie el archivo `.env.example` a `.env` y configure las credenciales de la base de datos.
2. Asegúrese de que las tablas requeridas existan en la base de datos.

## Uso

```bash
python save_triple_signals.py --symbol BTCUSDT --timeframe 5m
```

## Estructura del Proyecto

- `save_triple_signals.py`: Script principal que detecta y guarda las señales.
- `.env`: Archivo de configuración con las credenciales de la base de datos.
- `README.md`: Este archivo con la documentación del proyecto.

## Licencia

Este proyecto está bajo la Licencia MIT. Ver el archivo `LICENSE` para más detalles.

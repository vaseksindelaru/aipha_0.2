import pandas as pd
import pandas_ta as ta
import numpy as np

def get_enhanced_triple_barrier_labels(
    prices: pd.DataFrame,
    t_events: pd.Series,
    profit_factors: list,
    stop_loss_factor: float,
    time_limit: int,
    drawdown_threshold: float,
    atr_period: int = 14
) -> pd.Series:
    """
    Calcula etiquetas para eventos de trading usando el método de Triple Barrera Potenciado.

    Este método genera etiquetas ordinales basadas en múltiples niveles de toma de ganancias,
    un límite de pérdida, un límite de tiempo y un filtro de drawdown para la calidad de la señal.

    Args:
        prices (pd.DataFrame): DataFrame con precios OHLC y un índice de tipo Datetime.
                               Debe contener las columnas 'High', 'Low', 'Close'.
        t_events (pd.Series): Serie de pandas con los Timestamps de los eventos a etiquetar.
        profit_factors (list): Lista de multiplicadores de ATR para las barreras de toma de ganancias.
                               Ej: [2.0, 4.0, 6.0] crea 3 niveles de ganancia.
                               La lista DEBE estar ordenada de menor a mayor.
        stop_loss_factor (float): Multiplicador de ATR para la barrera de stop-loss.
        time_limit (int): Número máximo de velas a esperar antes de que la operación expire (barrera vertical).
        drawdown_threshold (float): Un valor entre 0 y 1. Si el drawdown supera este porcentaje
                                    del riesgo total (distancia al stop-loss), la operación se
                                    considera de baja calidad y se etiqueta como neutral (0).
        atr_period (int): Período para el cálculo del ATR (Average True Range).

    Returns:
        pd.Series: Una serie con las etiquetas ordinales para cada evento.
                   - Valor > 0: Nivel de toma de ganancias alcanzado (ej. 1, 2, 3).
                   - Valor = -1: Límite de pérdida alcanzado.
                   - Valor = 0: Límite de tiempo alcanzado o drawdown excesivo.
    """
    # 1. Calcular ATR para la volatilidad dinámica
    prices['atr'] = ta.atr(prices['High'], prices['Low'], prices['Close'], length=atr_period)
    
    # Asegurarse de que t_events esté en el índice de precios para un mapeo rápido
    t_events = t_events[t_events.isin(prices.index)]
    
    labels = pd.Series(0, index=t_events)
    
    # 2. Iterar sobre cada evento para definir y comprobar las barreras
    for event_idx in t_events:
        entry_price = prices.loc[event_idx, 'Close']
        atr_at_event = prices.loc[event_idx, 'atr']
        
        if pd.isna(atr_at_event):
            continue # Omitir si no hay ATR (al principio de la serie)

        # 3. Definir barreras dinámicas
        sl_level = entry_price - (atr_at_event * stop_loss_factor)
        
        # Crear barreras de TP ordenadas
        tp_levels = sorted([entry_price + (atr_at_event * pf) for pf in profit_factors])
        
        # 4. Extraer el camino del precio después del evento
        path_max_idx = prices.index.get_loc(event_idx) + time_limit
        price_path = prices.iloc[prices.index.get_loc(event_idx):path_max_idx]
        
        label = 0
        
        # 5. Comprobar el camino del precio contra las barreras
        for i, row in price_path.iterrows():
            # Comprobar Stop Loss
            if row['Low'] <= sl_level:
                # 5.1 Filtro de Drawdown
                # Antes de tocar SL, ¿tocó algún TP?
                path_before_sl = price_path.loc[event_idx:i]
                highest_before_sl = path_before_sl['High'].max()
                
                # Calcular el drawdown desde el punto más alto alcanzado
                drawdown = (highest_before_sl - sl_level) / (highest_before_sl - entry_price) if highest_before_sl > entry_price else 0
                
                if drawdown > drawdown_threshold:
                    label = 0 # Calidad de señal baja, se marca como neutral
                else:
                    label = -1 # Toca SL limpiamente
                break

            # Comprobar Take Profit (en orden)
            for j, tp_level in reversed(list(enumerate(tp_levels))):
                if row['High'] >= tp_level:
                    label = j + 1 # Etiqueta ordinal (1, 2, 3...)
                    # No rompemos el bucle aquí para permitir que se alcance un TP más alto
        
        labels.loc[event_idx] = label
        
    return labels

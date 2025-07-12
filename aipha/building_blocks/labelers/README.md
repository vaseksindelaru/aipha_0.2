# Potential Capture Engine

This module contains the implementation of the "Enhanced Triple Barrier" labeling method.

## `get_enhanced_triple_barrier_labels`

This function is used to generate sophisticated, ordinal labels for trading events. It goes beyond simple "up" or "down" predictions by considering multiple profit-taking levels, a dynamic stop-loss, a time limit, and a signal quality filter based on drawdown.

### Key Features:

- **Dynamic Barriers:** Profit-take and stop-loss levels are calculated using the Average True Range (ATR), making them adaptive to market volatility.
- **Multiple Profit Targets:** Allows defining several levels of profit-taking, resulting in more granular labels (e.g., +1, +2, +3) that can indicate the magnitude of a price move.
- **Drawdown Filter:** Penalizes signals that experience a significant price retracement before hitting the stop-loss, filtering out low-quality or risky entries.
- **Time Limit:** A vertical barrier that assigns a neutral label (0) if the price doesn't hit any of the horizontal barriers within a specified time.

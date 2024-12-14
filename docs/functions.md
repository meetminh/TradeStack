Here are all 9 financial calculation functions from database_functions.rs:
get_current_price
>
- Gets the latest price for a ticker at a specific date
Returns: time, ticker, and close price
get_sma (Simple Moving Average)
>
- Calculates arithmetic mean of prices over specified period
Example: 20-day SMA = average of last 20 closing prices
get_ema (Exponential Moving Average)
>
- Weighted moving average giving more importance to recent prices
Uses smoothing factor: 2/(period + 1)
4. get_cumulative_return
>
- Calculates total percentage return over period
Formula: ((end_price - start_price) / start_price) 100
get_ma_of_price (Moving Average of Prices)
>
- Simple moving average of raw price values
Used for trend identification
get_ma_of_returns (Moving Average of Returns)
>
- Moving average of daily return percentages
Helps identify momentum in returns
get_rsi (Relative Strength Index)
>
- Momentum oscillator measuring speed/magnitude of price changes
Range: 0-100, >70 overbought, <30 oversold
get_max_drawdown
>
- Measures largest peak-to-trough decline
Returns percentage and absolute value of decline, plus timing
get_price_std_dev (Price Standard Deviation)
>
- Measures price volatility over period
Higher values indicate more volatile price action
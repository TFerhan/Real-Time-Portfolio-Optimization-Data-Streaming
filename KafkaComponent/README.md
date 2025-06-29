## metrics to be computed
# 1. Descriptive Statistics:
Mean (Average): The central value of the dataset.
Standard Deviation (Volatility): Measures the spread of the data from the mean.
Variance: The square of the standard deviation.
Median: The middle value in the data, useful when data is skewed.
Mode: The most frequent value in the dataset.
# 2. Shape of the Distribution:
Skewness: Measures the asymmetry of the distribution (positive skew means the right tail is longer; negative skew means the left tail is longer).
Kurtosis: Measures the "tailedness" of the distribution (leptokurtic: heavy tails, platykurtic: light tails, mesokurtic: normal distribution).
Quantiles/Percentiles: Values at specific percentiles, such as 25th (Q1), 50th (median), and 75th (Q3).
# 3. Correlation and Relationships:
Pearson Correlation: Measures the linear correlation between two variables (ranges from -1 to 1).
Covariance: Measures how two assets move together.
Autocorrelation: Measures the correlation of a time series with a lagged version of itself.
Cross-Correlation: Measures the correlation between two time series at different lags.
# 4. Rolling Metrics:
Moving Averages (Simple and Exponential): Smoothens data over a window of time to identify trends.
Rolling Standard Deviation/Volatility: Measures the variability of returns over a rolling window.
Rolling Covariance: Measures the rolling relationship between two time series over a given period.
# 5. Risk Metrics:
VaR (Value at Risk): Measures the potential loss in value of a portfolio over a given time horizon for a given confidence interval.
CVaR (Conditional Value at Risk): Measures the expected loss in the worst-case scenario beyond VaR.
Beta: Measures the volatility of a stock or portfolio in relation to the market (systematic risk).
Alpha: Measures the excess return of a portfolio over the risk-adjusted expected return.
# 6. Performance Metrics:
Sharpe Ratio: Measures risk-adjusted return by dividing the mean excess return by the standard deviation.
Sortino Ratio: Similar to the Sharpe ratio, but only uses downside risk (negative returns).
Treynor Ratio: Risk-adjusted return based on systematic risk (beta).
Jensen’s Alpha: Measures the abnormal return of a portfolio above the expected return (based on the Capital Asset Pricing Model, CAPM).
# 7. Regression and Factor Models:
Beta Coefficients (CAPM): Measures sensitivity of a stock's return to the market return.
Factor Loadings (APT): Measures the sensitivity of a stock to various macroeconomic factors.
R-squared: The proportion of variance in the dependent variable that is explained by the independent variables.
# 8. Trend and Seasonality:
Trend (Linear or Exponential): Identifying underlying trends over time.
Seasonality: Identifying patterns that repeat over time (e.g., weekly, monthly).
Detrended Series: Series adjusted to remove trend components, often used in time-series modeling.
# 9. Financial Ratios (for Analysis):
P/E Ratio (Price-to-Earnings): Valuation ratio to assess if a stock is over or under-valued.
P/B Ratio (Price-to-Book): Compares the market value to the book value of a company.
Dividend Yield: Measures the annual dividends paid as a percentage of the stock price.
# 10. Cumulative Metrics:
Cumulative Return: The total return over a period of time, often plotted to visualize growth.
Cumulative Volatility: The cumulative risk over a period.
# 11. Time-Series Specific Metrics:
Lagged Values: Measures how past values influence future values in time series.
Rolling Window Metrics: Metrics calculated over a rolling window (e.g., 30-day moving average).
Seasonal Decomposition: Decompose the time series into trend, seasonal, and residual components.

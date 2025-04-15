# Stock Analyzer

A comprehensive Python tool for fetching, analyzing, and organizing financial data for stocks. This application retrieves a wide range of financial information and saves it to structured Excel files for analysis.

## Features

- **Historical Price Data**: Fetch 5-year historical stock price data with OHLC values
- **Company Information**: Get detailed company profiles and summaries
- **Financial Statements**: Extract income statements, balance sheets, and cash flow statements
- **Key Statistics**: Gather important financial metrics and ratios
- **Sustainability Metrics**: Retrieve ESG (Environmental, Social, Governance) scores when available
- **Peer Comparison**: Compare stocks with industry peers
- **Data Export**: Save all data to organized Excel spreadsheets

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/stock-analyzer.git
   cd stock-analyzer
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

## Dependencies

- yfinance
- pandas
- requests
- tqdm
- xlsxwriter
- beautifulsoup4
- lxml
- python-dateutil

## Usage

To analyze a stock, simply run the main script with the stock ticker symbol:

```python
# Example usage
import asyncio
from stocks import StockAnalyzer

# Analyze a single stock
async def analyze_stock():
    analyzer = StockAnalyzer("AAPL")  # Replace with your desired ticker
    await analyzer.fetch_all_data()

# Run the analysis
asyncio.run(analyze_stock())
```

All data will be saved to an Excel file in the `Datasets` directory, named after the ticker symbol (e.g., `AAPL.xlsx`).

## Data Sources

This tool uses several data sources to fetch comprehensive financial information:
- Yahoo Finance API
- Web scraping from Yahoo Finance pages
- Additional ESG data providers

## Notes

- The tool implements rate limiting and error handling to ensure reliable data collection
- For some metrics, if primary data sources are unavailable, the tool attempts to get data from alternative sources
- The Excel output is formatted for readability with proper data types and layout

## License

[Add your license information here]

## Disclaimer

This tool is for informational purposes only. The data provided should not be considered as financial advice. Always verify financial information from multiple sources before making investment decisions. 
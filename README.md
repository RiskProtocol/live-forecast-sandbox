# Debugging Live Forecast

This project contains a Python script for analyzing time gaps in coinmetrics websocket data.

## live_forecast.py

The `live_forecast.py` file includes code to fetch data from coinmetrics websocket writes this data immidiately to a log file in `logs` directory then parses the data and inserts it into a sqlite database. It also includes an SQL query that identifies and calculates time gaps between consecutive rows in a dataset.

The objective of this script is to identify and calculate time gaps between consecutive rows in a dataset. This information is useful for identifying discontinuities or missing data points in the time series data.

## Running the Service

### Prerequisites

- Python 3.7 or higher
- Required Python packages (install using `pip install -r requirements.txt`)

### Setup

1. Clone the repository:
   ```
   git clone https://github.com/RiskProtocol/live-forecast-sandbox.git
   cd live-forecast
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

### Running the Service

To start the Live Forecast service, run:

```
python live_forecast.py
```

The service will connect to the database, execute the query, and process the results. Check the console output for any messages or results

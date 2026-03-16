# Financial Data Engineering & Analytics Pipeline
## OVERVIEW
![Workflow-3](https://github.com/nvkhoa14/stock-data-engineering/blob/main/imgs/workflow-3.png)
## A. DATA SOURCE
### 1. sec-api.io
- This API allows us to retrieve the list of companies currently listed on the NYSE and NASDAQ exchanges.  
- **Details:** [sec-api.io/docs/mapping-api/list-companies-by-exchange](https://sec-api.io/docs/mapping-api/list-companies-by-exchange)
- **API Collecting:** At 00:00 on the first day of each month.
- **Data Volume:** ~28,000 rows updated monthly
### 2. Alpha Vantage
#### a. API for Market Status
- Provides information about stock markets worldwide.  
- **Details:** [alphavantage.co/documentation/#market-status](https://www.alphavantage.co/documentation/#market-status)
- **API Collecting:** At 00:00 on the first day of each month. 
#### b. API for News Sentiment
- Provides market news data along with sentiment analysis from leading news sources.  
- **Details:** [alphavantage.co/documentation/#news-sentiment](https://www.alphavantage.co/documentation/#news-sentiment)
- **API Collecting:** During trading hours, divided into time frames:
  - 00:00 – 09:30
  - 09:30 – 16:00
  - 16:00 – 00:00
- **Data Volume:** ~500 articles per day
### 3. Polygon
- Allows retrieval of daily OHLC (Open, High, Low, Close) data for all stocks in the U.S. stock market.  
- **Details:** [polygon.io/docs/stocks/get_v2_aggs_ticker__stocksticker__range__multiplier___timespan___from___to](https://polygon.io/docs/stocks/get_v2_aggs_ticker__stocksticker__range__multiplier___timespan___from___to)
- **API Collecting:** At 01:00 every day.
-  **Data Volume:** ~10,000 rows of data per day
## B. BUSINESS REQUIREMENT

## C. DATA PIPELINE
### 1. ETL-DATABASE
#### a.DATABASE
- We can see that the two APIs **sec-api.io** and **Alpha Vantage API for market status** do not need to be queried as frequently as the other two APIs because the data in these two APIs rarely changes and only needs to be updated at the beginning of each month.
- Therefore, we will store the data from these two APIs in the Database to simulate a Backend Database, making the Data Sources more diverse.
- The Database is normalized to **3NF** and consists of the following 5 tables:
![Database Schema](https://github.com/nvkhoa14/stock-data-engineering/blob/main/imgs/database.png)
#### b. DAGs
![DAG-ETL](https://github.com/nvkhoa14/stock-data-engineering/blob/main/imgs/DAG-ETL.png)
### 2. ELT-DATAWAREHOUSE
- Designing the Dimension tables and Fact tables based on the Business Requirements from the Data Sources (2 APIs and 1 Database), we obtain the following structure of the Data Warehouse:
![Datawarehouse Schema](https://github.com/nvkhoa14/stock-data-engineering/blob/main/imgs/datawarehouse.png)
- Workflow diagram of the ELT process
![Workflow-1](https://github.com/nvkhoa14/stock-data-engineering/blob/main/imgs/workflow-1.png)
![Workflow-2](https://github.com/nvkhoa14/stock-data-engineering/blob/main/imgs/workflow-2.png)
- Process model for ELT
![ELT](https://github.com/nvkhoa14/stock-data-engineering/blob/main/imgs/ELT.png)
- DAGs
![DAG-ELT](https://github.com/nvkhoa14/stock-data-engineering/blob/main/imgs/DAG-ELT.png)
### OUTPUT
- Check data available or not.
[Check](https://github.com/nvkhoa14/stock-data-engineering/tree/main/notebook/check.ipynb)

- Analysic
# Stock Market Data Warehouse (DBMS Project)

This repository contains the source code for a database management project designed to efficiently store, retrieve, and analyze large-scale financial time-series data using **MySQL**.

The project constructs a stock market data warehouse and benchmarks the performance of critical analytical queries on:
- A non-optimized baseline table,
- A table enhanced with **B-Tree indexing**, and
- A fully optimized table using **indexing + table partitioning**.

---

## Problem Statement

Traditional relational databases are commonly used for time-series data, but they are not inherently optimized for large-scale historical financial datasets. This results in slow query performance for essential financial operations such as:

> “Retrieve the closing price of AAPL on 2023-10-25.”

This project demonstrates that by applying foundational database optimization strategies, **standard MySQL can achieve performance comparable to specialized time-series databases**, delivering fast and scalable analytical query execution.

---

## Project Structure & Workflow

| Component | Description |
|---------|-------------|
| **data.py** | Downloads 20 years of historical stock data for 20 companies using the `yfinance` API and stores it in a CSV file. |
| **load_data.py** | Loads the CSV into MySQL across three tables: baseline, indexed, and partitioned. |
| **mine.py** | Performs analytical processing (20 & 50-day Simple Moving Average) and stores results in a `patterns_warehouse` table. |
| **mine_and_predict.py** | Fetches stock data, trains a Linear Regression model, predicts the next closing price, and benchmarks retrieval speeds. |

---

## Requirements

- Python 3.x
- MySQL Server (8.x recommended)
- MySQL Client (Workbench / DBeaver recommended)

Install Python dependencies:

```bash
pip install -r requirements.txt
```

## How to Run

### 1. Create the Database and Tables

Connect to MySQL and execute:

```sql
CREATE DATABASE stock_warehouse;
USE stock_warehouse;

-- Baseline Table
CREATE TABLE stock_data_baseline (
    `date` DATE,
    `open` DECIMAL(10, 2),
    `high` DECIMAL(10, 2),
    `low` DECIMAL(10, 2),
    `close` DECIMAL(10, 2),
    `adj_close` DECIMAL(10, 2),
    `volume` BIGINT,
    `ticker` VARCHAR(10)
);

-- Indexed Table
CREATE TABLE stock_data_indexed (
    `date` DATE,
    `open` DECIMAL(10, 2),
    `high` DECIMAL(10, 2),
    `low` DECIMAL(10, 2),
    `close` DECIMAL(10, 2),
    `adj_close` DECIMAL(10, 2),
    `volume` BIGINT,
    `ticker` VARCHAR(10),
    INDEX idx_ticker_date (ticker, `date`)
);

-- Partitioned + Indexed Table
CREATE TABLE stock_data_partitioned (
    `date` DATE,
    `open` DECIMAL(10, 2),
    `high` DECIMAL(10, 2),
    `low` DECIMAL(10, 2),
    `close` DECIMAL(10, 2),
    `adj_close` DECIMAL(10, 2),
    `volume` BIGINT,
    `ticker` VARCHAR(10),
    INDEX idx_ticker_date (ticker, `date`)
)
PARTITION BY RANGE ( YEAR(`date`) ) (
    PARTITION p2005 VALUES LESS THAN (2006),
    PARTITION p2006 VALUES LESS THAN (2007),
    PARTITION p2007 VALUES LESS THAN (2008),
    PARTITION p2008 VALUES LESS THAN (2009),
    PARTITION p2009 VALUES LESS THAN (2010),
    PARTITION p2010 VALUES LESS THAN (2011),
    PARTITION p2011 VALUES LESS THAN (2012),
    PARTITION p2012 VALUES LESS THAN (2013),
    PARTITION p2013 VALUES LESS THAN (2014),
    PARTITION p2014 VALUES LESS THAN (2015),
    PARTITION p2015 VALUES LESS THAN (2016),
    PARTITION p2016 VALUES LESS THAN (2017),
    PARTITION p2017 VALUES LESS THAN (2018),
    PARTITION p2018 VALUES LESS THAN (2019),
    PARTITION p2019 VALUES LESS THAN (2020),
    PARTITION p2020 VALUES LESS THAN (2021),
    PARTITION p2021 VALUES LESS THAN (2022),
    PARTITION p2022 VALUES LESS THAN (2023),
    PARTITION p2023 VALUES LESS THAN (2024),
    PARTITION p2024 VALUES LESS THAN (2025),
    PARTITION p_max VALUES LESS THAN MAXVALUE
);
```
### 2. Download Stock Data

```bash
python data.py
```

### 3. Load Data into All Tables

Edit the `DB_PASS` variable in `load_data.py`, then run:

```bash
python load_data.py
```

### 4. Generate Analytical Patterns (SMA)

```bash
python mine.py
```
### 4. Generate Analytical Patterns (SMA)

```bash
python mine.py
```

View the results in MySQL:

```sql
SELECT * FROM patterns_warehouse LIMIT 10;
```

---

### 5. Predict Next-Day Closing Price

```bash
python mine_and_predict.py
```

#### Example Output (Performance Comparison)

```
Fetching data for NFLX from 'stock_data_partitioned'...
5032 rows fetched in 0.1444 seconds.

Fetching data for NFLX from 'stock_data_baseline'...
5032 rows fetched in 0.3486 seconds.
```

---

## License

This project is licensed under the **MIT License**.

# Data Engineer – Home Assignment

## **1. Scenario**

Build a data pipeline that ingests **a month worth of precipitation data** from the **NOAA (National Oceanic & Atmoshperic Administration)** API writes it **directly into an Apache Iceberg table** and transforms the data.

Note: This dataset is stale, Choose a year with at least 3 monthes worth of data (for example 2010)

Getting started (obtain a token):
https://www.ncdc.noaa.gov/cdo-web/webservices/v2#gettingStarted

`www.ncei.noaa.gov/cdo-web/api/v2/data?datasetid=PRECIP_15`

---

# **2. Requirements**

## **Ingestion Pipeline**

* Apply these **3 required normalizations**:

  1. **Expand concatenated strings to arrays**
  2. **Convert all timestamps to ms epoch**
  3. **Add an ingestion timestamp column**

* Write the normalized data directly into an Iceberg table stored in MinIO (S3 API).
* Use clean, modular OOP Python.
* Apply strict validation for data based on the public schema.

## **Transformation Pipeline**
Implement the following transformation:
### Track Proportion of Missing Data per Station
The database must include a table that stores, for each station, the proportion of observations where the recorded value equals 99999.
The table must include:
* Station identifier
* Total number of observations
* Number of missing/invalid observations (value = 99999)
* Percentage of missing data

**Apply the transformation on new data only**

## **Maintenance Pipeline**
Inspect the Iceberg table and apply necessary maintenance actions such as snapshot management, file optimization, and metadata cleanup.

## **Datalake Intialization**

**Include all required DDLs for initializing your complete datalake schema**

## **Docker**

* Provide **one Dockerfile** that builds the pipeline image based on the `src/main.py`.
---

# **3. Architecture Diagram (the real deal)**
Consider this basic data flow as a prototype for production purposes. 
Add a simple diagram describing the data flow with the modifications needed to support production grade systems. 
Focus on relations and structures.
Any format acceptable.

---

# **4. SQL Sanity Checks**
Provide an SQL queries proving the data is ingested correctly.
Provide an SQL query proving the trasformed data is valid. 

---

# **5. Submission**

Submit a Git repository containing:

* Python code
* Dockerfile for the pipeline
* Architecture diagram
* SQL queries
* README with run instructions

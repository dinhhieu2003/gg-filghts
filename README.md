This project is still in development. Stay tuned for the end result!

# ✈️ Google Flights ETL Pipeline
In this project we build an ETL pipeline, that extracts data using the Google Flights API (serpapi).

It would be interesting to have a ready tool to analyze patterns in word frequency in different subreddits.

# 🗂️ Folder Structure
```
+ ---- config
    config.conf
+ ---- dags
    flights_dag.py
+ ---- plugins
+ ---- logs
+ ---- data
    + --- input
    + --- output
            + --- {DEPARTURE AIRPORT}:{ARRIVAL AIRPORT}
                + --- {TRIP START DATE}-{TRIP END DATE}
                    + --- {SEARCH DATE}
                        + --- bronze
                            raw.json
                        + --- silver
                            + --- best_trips
                                + --- flights
                                + --- layover
                                + --- trip_information
                            + --- other_trips
                                + --- flights
                                + --- layover
                                + --- trip_information
                            search_paramters.csv
                        + --- gold
            ...
+ ---- etls
        flights_etl.py
+ ---- pipelines
        flights_pipeline.py
+ ---- utils
        constants.py
```

`config`: Contains `config.conf`, which contains the SERP API Key used to send the API request. You will have to create the config file and add your credentials, as below:

```
[api_keys]
flights_api_key = *api key*
```

`data`:
- `input` any cached data that might be referred to. 
- `output` contains the output JSON and CSV data.
  - `bronze` contains the raw JSON data as output from the API requested.
  - `silver` contains **compiled** data, as personally found necessary to be compiled.
    - `best_trips` contains the information compiled with respect to the best prices & and shorter trip times (according to Google Flights).
      - `flights`
      - `layover`
      - `trip_information`
    - `other_trips` contains the other available trips found on Google Flights.
      - `flights`
      - `layover`
      - `trip_information`
  - `search_parameters` are logged in this folder, which are the parameters used for the search.

`etls`: The script used for data extraction, transformation, and loading.

`pipelines`: Orchestrates the ETL scripts and functions to form a pipeline-like flow.

`utils`: contains a `constants.py` script, which uses the `ConfigParser` library to parse the config file defined earlier.

`plugins`, `logs`, and `dags` are required to launch project on Apache Airflow, `dags` contains the Python script of the DAG involved in the pipeline tasks.

# 🏃🏽‍♂️ How To Run

1. First make sure to set the `config.conf` file as described above. 

2. Make sure to have `docker` and `docker compose` installed.
   
3. Run the following command to setup the postgres database:

```
docker-compose up airflow-init
```

4. Run the following command to spin up the containers in the background:
```
docker-compose up -d
```

5. Proceed to login to Apache Airflow webserver in `localhost` on port `8080`.

# ℹ️ Reference
1. SERP API documentation: https://serpapi.com/google-flights-api#api-parameters-search-query-departure-id

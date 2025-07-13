# Analysis of Flights Using PySpark

## Description
The project is a set of PySpark tasks for analyzing flight data. Each task focuses on a specific aspect of the analysis and creates a separate data mart with the results.

### Data schema
####  Airlines Data
| __Column Name__ | __Data Type__ | __Description__                      |
|-----------------|---------------|--------------------------------------|
| IATA_CODE       | String        | Unique identifier for the airline    |
| AIRLINE         | String        | Name of the airline                  |

#### Airports Data
| __Column Name__ | __Data Type__ | __Description__                      |
|-----------------|---------------|--------------------------------------|
| IATA_CODE       | String        | Unique identifier for the airport    |
| AIRPORT         | String        | Name of the airport                  |
| CITY            | String        | City where the airport is located    |
| STATE           | String        | State where the airport is located   |
| COUNTRY         | String        | Country where the airport is located |
| LATITUDE        | Float         | Latitude coordinate of the airport   |
| LONGITUDE       | Float         | Longitude coordinate of the airport  |

#### Flights Data
| __Column Name__     | __Data Type__ | __Description__                                                                                                                                                  |
|---------------------|---------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| YEAR                | Integer       | Year of the flight                                                                                                                                               |
| MONTH               | Integer       | Month of the flight                                                                                                                                              |
| DAY                 | Integer       | Day of the flight                                                                                                                                                |
| DAY_OF_WEEK         | Integer       | Day of the week  [1-7] = [monday-sunday]                                                                                                                         |
| AIRLINE             | String        | Airline code                                                                                                                                                     |
| FLIGHT_NUMBER       | String        | Flight number (id)                                                                                                                                               |
| TAIL_NUMBER         | String        | Tail number of the aircraft                                                                                                                                      |
| ORIGIN_AIRPORT      | String        | Origin airport code                                                                                                                                              |
| DESTINATION_AIRPORT | String        | Destination airport code                                                                                                                                         |
| SCHEDULED_DEPARTURE | Integer       | Scheduled departure time                                                                                                                                         |
| DEPARTURE_TIME      | Integer       | Fact departure time                                                                                                                                              |
| DEPARTURE_DELAY     | Integer       | Departure delay in minutes                                                                                                                                       |
| TAXI_OUT            | Integer       | The time elapsed between departure from the departure gate at the departure airport and departure                                                                |
| WHEELS_OFF          | Integer       | The time when the wheels leaves the ground at the departure airport                                                                                              |
| SCHEDULED_TIME      | Integer       | Scheduled arrival time at the destination airport                                                                                                                |
| ELAPSED_TIME        | Integer       | Total elapsed time of the flight    AIR_TIME+TAXI_IN+TAXI_OUT                                                                                                    |
| AIR_TIME            | Integer       | The time elapsed between the wheels leaving the ground at the departure airport (WHEELS_OFF) and the wheels touching down at the destination airport (WHEELS_ON) |
| DISTANCE            | Integer       | Distance between the origin and destination airports                                                                                                             |
| WHEELS_ON           | Integer       | The time when the wheels touch down at the destination airport                                                                                                   |
| TAXI_IN             | Integer       | The time elapsed between the wheels touching down at the destination airport and arrival at the arrival gate at the destination airport                          |
| SCHEDULED_ARRIVAL   | Integer       | Scheduled arrival time at the destination airport                                                                                                                |
| ARRIVAL_TIME        | Integer       | Fact arrival time at the destination airport (gate) WHEELS_ON+TAXI_IN                                                                                            |
| ARRIVAL_DELAY       | Integer       | Arrival delay in minutes    ARRIVAL_TIME-SCHEDULED_ARRIVAL                                                                                                       |
| DIVERTED            | Integer       | Indicates that the aircraft arrival did not arrive at the scheduled time                                                                                         |
| CANCELLED           | Integer       | Indicates that the flight was cancelled                                                                                                                          |
| CANCELLATION_REASON | String        | Reason for cancellation (A = Airline/Carrier, B = Weather, C = National Air System, D = Security)                                                                |
| AIR_SYSTEM_DELAY    | Integer       | Delay caused by the airline system                                                                                                                               |
| SECURITY_DELAY      | Integer       | Delay caused by security issues                                                                                                                                  |
| AIRLINE_DELAY       | Integer       | Delay caused by the airline                                                                                                                                      |
| LATE_AIRCRAFT_DELAY | Integer       | Delay caused by the late arrival of the aircraft                                                                                                                 |
| WEATHER_DELAY       | Integer       | Delay caused by weather conditions                                                                                                                               |



## Tasks

**PySparkJob1:** Generating a pivot table showing the top 10 aircraft (by TAIL_NUMBER) with the highest number of flights over all periods. Excluding records that do not have a specified TAIL_NUMBER. Data schema:

| __Column Name__ | __Data Type__ | __Description__                    |
|-----------------|---------------|------------------------------------|
| TAIL_NUMBER     | String        | Tail number of the aircraft        |
| count           | Integer       | Number of flights for the aircraft |

**PySparkJob2:** Generating a pivot table showing the top 10 aircraft (ORIGIN_AIRPORT, DESTINATION_AIRPORT) with the highest number of flights over all periods and average air time for the route. Data schema:

| __Column Name__     | __Data Type__ | __Description__                                |
|---------------------|---------------|------------------------------------------------|
| ORIGIN_AIRPORT      | String        | Origin airport code                            |
| DESTINATION_AIRPORT | String        | Destination airport code                       |
| tail_count          | Integer       | Number of flights for the route  (TAIL_NUMBER) |
| avg_air_time        | Float         | Average air time for the route                 |

**PySparkJob3:** Calculates the average, minimum, and maximum departure delay time (DEPARTURE_DELAY) and displays airports where the maximum delay time is 1000 seconds or more. Also shows the correlation between delay time and the day of the week (DAY_OF_WEEK). Data schema:

| __Column Name__        | __Data Type__ | __Description__                                         |
|------------------------|---------------|---------------------------------------------------------|
| ORIGIN_AIRPORT         | String        | Origin airport code                                     |
| avg_delay              | Float         | Average departure delay time                            |
| min_delay              | Integer       | Minimum departure delay time                            |
| max_delay              | Integer       | Maximum departure delay time                            |
| corr_delay2day_of_week | Float         | Correlation between departure delay and day of the week |

**PySparkJob4:** Generating a simple pivot table showing completed aircraft. Data schema:

| __Column Name__          | __Data Type__ | __Description__                                           |
|--------------------------|---------------|-----------------------------------------------------------|
| AIRLINE_NAME             | String        | Name of the airline (airlines.AIRLINE)                    |
| TAIL_NUMBER              | String        | Tail number of the aircraft (flights.TAIL_NUMBER)         |
| ORIGIN_COUNTRY           | String        | Country of the origin airport (airports.COUNTRY)          |
| ORIGIN_AIRPORT_NAME      | String        | Name of the origin airport (airports.AIRPORT)             |
| ORIGIN_LATITUDE          | Float         | Latitude of the origin airport (airports.LATITUDE)        |
| ORIGIN_LONGITUDE         | Float         | Longitude of the origin airport (airports.LONGITUDE)      |
| DESTINATION_COUNTRY      | String        | Country of the destination airport (airports.COUNTRY)     |
| DESTINATION_AIRPORT_NAME | String        | Name of the destination airport (airports.AIRPORT)        |
| DESTINATION_LATITUDE     | Float         | Latitude of the destination airport (airports.LATITUDE)   |
| DESTINATION_LONGITUDE    | Float         | Longitude of the destination airport (airports.LONGITUDE) |

**PySparkJob5:** Generating a pivot table showing info about all airlines. Data schema:

| __Column Name__      | __Data Type__ | __Description__                                                                |
|----------------------|---------------|--------------------------------------------------------------------------------|
| AIRLINE_NAME         | String        | Name of the airline (airlines.AIRLINE)                                         |
| correct_count        | Integer       | Number of flights without delays                                               |
| diverted_count       | Integer       | Number of diverted flights                                                     |
| cancelled_count      | Integer       | Number of cancelled flights                                                    |
| avg_distance         | Float         | Average distance of flights                                                    |
| avg_air_time         | Float         | Average air time of flights                                                    |
| airline_issue_count  | Integer       | Number of canceled flights by airline issues (CANCELLATION_REASON)             |
| weather_issue_count  | Integer       | Number of canceled flights by weather issues (CANCELLATION_REASON)             |
| nas_issue_count      | Integer       | Number of canceled flights by national air system issues (CANCELLATION_REASON) |
| security_issue_count | Integer       | Number of canceled flights by security issues (CANCELLATION_REASON)            |


## Requirements
- Python == 3.10
- PySpark == 3.1.3
- pandas
- pyarrow
- kafka-python

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/DarjaBalasheva/pySpark-Data-Mart.git
   ```
   

2. Install the required packages:
   ```bash
   pipenv sync
   ```

3. Run the PySpark jobs:
   ```bash
    python PySparkJob1.py --flights_path=data/flights.parquet --result_path=result/PySparkJob1
    python PySparkJob2.py --flights_path=data/flights.parquet --result_path=result/PySparkJob2
    python PySparkJob3.py --flights_path=data/flights.parquet --result_path=result/PySparkJob3
    python PySparkJob4.py --flights_path=data/flights.parquet --airports_path=data/airports.parquet --airlines_path=data/airlines.parquet --result_path=result/PySparkJob4
    python PySparkJob5.py --flights_path=data/flights.parquet --airlines_path=data/airlines.parquet --result_path=result/PySparkJob5
   ```


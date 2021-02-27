# Udacity Data Engineering Capstone Project

The purpose of the project is to combine what we've learned throughout the program. 

**Contents**

- [Description](#Description)
- [Datasets](#Datasets)
- [ETL Pipeline](#ETL-Pipeline)
- [Analysis](#Analysis)

## Description

Udacity provides four datasets for the project and also gives the student an option to use 
additional data. Four datasets include immigration to US, US city demographics, temperature data
and airport codes. I won't use airport codes in my analysis, thus I leave it out of the project. 
Instead, I will use some country based statistics to support my analysis.

## Datasets

**immigration**: Data includes immigrations to US in 2016. Contains international visitor arrival
statistics by world regions and countries, type of visa, mode of transportation, age groups, 
states visited (first intended address only), and the top ports of entry (for select countries). 

- source: [National Travel and Tourism Office](https://travel.trade.gov/research/reports/i94/historical/2016.html)

| column_name | description            | data_type |
|-------------|------------------------|-----------|
| cicid       | migrant_id             | int       |
| i94yr       | year                   | int       |
| i94mon      | month                  | int       |
| i94cit      | code of origin city    | int       |
| i94res      | code of origin country | int       |
| i94port     | code of departure city | string    |
| arrdate     | arrival date           | int       |
| depdate     | departure date         | int       |
| i94mode     | travel code            | int       |
| i94addr     | residence city         | string    |
| i94visa     | visa type              | int       |
| occup       | job                    | string    |
| biryear     | birth year             | int       |
| gender      | gender                 | string    |

**us_cities_demographics**: This dataset contains information about the demographics of all US 
cities and census-designated places with a population greater or equal to 65,000. Data comes from
the US Census Bureau's 2015 American Community Survey.

- source: [Open Data Soft](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)

| column_name            | description             | data_type |
|------------------------|-------------------------|-----------|
| City                   | city name               | string    |
| State                  | state name              | string    |
| Median Age             | median age              | int       |
| Male Population        | male population         | int       |
| Female Population      | male population         | int       |
| Total Population       | total population        | int       |
| Number of Veterans     | population              | int       |
| Foreign-born           | foreign-born population | int       |
| Average Household Size | average household size  | float     |
| Race                   | race                    | string    |
| Count                  | count                   | int       |

**global_temperatures**: The Berkeley Earth Surface Temperature Study combines 1.6 billion 
temperature reports from 16 pre-existing archives. Starts in 1750 for average land temperature,
1850 for max and min lan temperatures and global ocean land temperatures.

- source: [Kaggle](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data)

| column_name                               | description                               | data_type |
|-------------------------------------------|-------------------------------------------|-----------|
| dt                                        | Date                                      | date      |
| LandAverageTemperature                    | global average land temperature           | float     |
| LandAverageTemperatureUncertainty         | the 95% confidence interval               | float     |
| LandMaxTemperature                        | global average maximum land temperature   | float     |
| LandMaxTemperatureUncertainty             | the 95% confidence interval               | float     |
| LandMinTemperature                        | global average minimum land temperature   | float     |
| LandMinTemperatureUncertainty             | the 95% confidence interval               | float     |
| LandAndOceanAverageTemperature            | global average land and ocean temperature | float     |
| LandAndOceanAverageTemperatureUncertainty | the 95% confidence interval               | float     |

**temperatures_by_country**: From same source with global temperatures, this dataset includes 
temperature statistics by country.

- source: [Kaggle](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data)

| column_name                       | description                     | data_type |
|-----------------------------------|---------------------------------|-----------|
| dt                                | Date                            | date      |
| Country                           | Country name                    | string    |
| LandAverageTemperature            | global average land temperature | float     |
| LandAverageTemperatureUncertainty | the 95% confidence interval     | float     |

**gdp_per_capita**: GDP per capita is a country's economic output divided by its population. It's
a good representation of a country's standard of living. It also describes how much citizens 
benefit from their country's economy.

- source: [The World Bank](https://data.worldbank.org/indicator/NY.GDP.PCAP.CD)

| column_name     | description            | data_type |
|-----------------|------------------------|-----------|
| Country Name    | Country name           | string    |
| Year[1960-2020] | Gdp per capita of year | float     |

**human_capital_index**: Human capital is a central driver of sustainable growth and poverty 
reduction according to World Bank. It simply shows how much a country values its people. 

- source: [The World Bank](https://data.worldbank.org/indicator/HD.HCI.OVRL)

| column_name     | description                 | data_type |
|-----------------|-----------------------------|-----------|
| Country Name    | Country name                | string    |
| Year[2010-2020] | Human capital index of year | float     |

**press_freedom_index**: The data is collected through an online questionnaire sent to journalists,
media lawyers, researchers and other media specialists selected by Reporters without Borders (RSF)
in the 180 countries covered by the Index. It gives an idea about the freedom and tension level in 
a country.

- source: [The World Bank](https://tcdata360.worldbank.org/indicators/h3f86901f)

| column_name     | description                 | data_type |
|-----------------|-----------------------------|-----------|
| Country Name    | Country name                | string    |
| Year[2001-2019] | Press freedom index of year | float     |

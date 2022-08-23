# Data Engineering Practice Project with Surfline Data
## Overview
This repository contains my Python code for a data engineering project I undertook to practice building a data pipeline with [Surfline](https://www.surfline.com/) forecast data. The ETL process is shown in the below diagram and is further described below.


![Screen Shot 2022-08-22 at 6 41 28 PM](https://user-images.githubusercontent.com/79472629/186049942-a90ce3fe-ab63-49b3-a2f3-23371ae04964.png)


## Steps
1. Extract 17-day surf forecast data from Surfline's API via Requests library. Guidance on Surfline's API provided by [meta-surf-forecast's](https://github.com/swrobel/meta-surf-forecast) README.md file, and spot-specific mapping provided by meta-surf-forecasts's [seeds file](https://github.com/swrobel/meta-surf-forecast/blob/main/db/seeds.rb).
2. Transform JSON objects via Pandas and produce DataFrame objects to upload to a database.
3. Load data to a database in PostgreSQL. Though not necessary, this step was used to practice running PostgreSQL locally.         

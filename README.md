# Data Engineering Practice Project with Surfline Data
## Overview
I practiced building a data pipeline with [Surfline](https://www.surfline.com/) forecast data. The ETL process is shown in the below diagram and is described in the below steps.


![image](https://github.com/csim3/surf-spot-forecast/assets/79472629/b1974223-330c-4b15-a2bf-868b970ee740)


## Steps
1. Extracted 5-day surf forecast data from Surfline's API via Requests library. Guidance on Surfline's API provided by [meta-surf-forecast's](https://github.com/swrobel/meta-surf-forecast) README.md file, and spot-specific mapping provided by meta-surf-forecast's [seeds file](https://github.com/swrobel/meta-surf-forecast/blob/main/db/seeds.rb).
2. Transformed JSON objects via Pandas library and produced DataFrame objects to upload to a database.
3. Loaded data to a database in PostgreSQL. Though not necessary, this step was used to practice running PostgreSQL locally.
4. Loaded data from PostgreSQL to Google Sheets. Google Sheets was specifically used since it is a free, web-based program whose data is automatically refreshed on a daily basis in Tableau Public dashboards.
5. Configured the above four steps in an Airflow DAG that runs locally and refreshes data on a daily basis.
6. Constructed a Tableau dashboard that is viewable on [Tableau Public](https://public.tableau.com/views/Surfline_comSpotForecast/SpotForecast?:language=en-US&:display_count=n&:origin=viz_share_link) to visualize 5-day wave, weather, wind, and tides forecasts of different surf spots.

## Tableau Dashboard Screenshot

![5DayForecast](https://github.com/csim3/surf_spot_forecast/assets/79472629/988a2647-a756-4d86-9c58-779ce11e48aa)

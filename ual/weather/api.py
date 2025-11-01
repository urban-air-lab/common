from wetterdienst import Settings
from wetterdienst.provider.dwd.observation import DwdObservationRequest
import pandas as pd

from ual.weather.dwd_stations import DWDStations

# Links
#https://github.com/bundesAPI/dwd-api
#https://opendata.dwd.de/climate_environment/CDC/Readme_intro_CDC_ftp.pdf
#https://www.dwd.de/DE/leistungen/klimadatendeutschland/stationsliste.html
#https://wetterdienst.readthedocs.io/en/latest/usage/python-api.html

# Ids   = https://bookdown.org/brry/rdwd/interactive-map.html
#       = https://www.dwd.de/DE/leistungen/klimadatendeutschland/statliste/statlex_html.html;jsessionid=9D6B1464323F43DDDF9C3997DAB9F9D1.live21072?view=nasPublication&nn=16102

settings = Settings(
  ts_shape="long",
  ts_humanize=True,
  ts_convert_units=True
)


def DwdWeatherData(station_id: int, start_date: str, end_date: str) -> pd.DataFrame:
    request = DwdObservationRequest(
      parameters=("daily", "climate_summary"),
      start_date=start_date,
      end_date=end_date,
    )

    request = request.filter_by_station_id(station_id=station_id)

    values = request.values.all().df
    dataframe = values.to_pandas()

    dataframe['group'] = dataframe.groupby('parameter').cumcount()
    dataframe_pivot = dataframe.pivot(index='group', columns='parameter', values='value').reset_index(drop=True)
    return dataframe_pivot

result = DwdWeatherData(DWDStations.HEILBRONN_KLINGENBERG.value, "2025-10-01", "2025-10-21")
print(result)
import numpy as np
import pandas as pd
from dateutil.parser import parse
import urllib
from operational_analysis.types import PlantData
from examples.turbine_analysis.engie_project import TurbineEngieOpenData


class EngieQC(PlantData):
    """This class loads wind turbine data from the engie open data platform  https://opendata-renewables.engie.com"""

    def __init__(self, name, start_date ,end_date, turbs, engine="pandas"):
        """
        Create a turbine based on data loaded from the engie open data platform.

        Args:
            name(string): uniqiue name (wind_turbine_name) of the wind turbine in the engie open data platform
            start_date(string): start date of the data to be loaded into the object (%d.%m.%y %H:%M)
            end_date(string): end date of the data to be loaded into the object (%d.%m.%y %H:%M)
            turbs(): Array of TurbineEngieOpenData objects that represent the individual turbines 

        Returns:
            New object
        """
        path = ""
        self._start_date = start_date
        self._end_date = end_date
        self._turbs = turbs
        super(EngieQC, self).__init__(path, name, engine, toolkit=[])

    def prepare(self):
        scads= [] # array of scada dataframes
        for i in self._turbs:
            i.prepare() # This prepare function is for the TurbineEngieOpenData
            i.scada.df['ID'] = i._name
            i.scada.df.sort_index(inplace=True)
            scads.append(i.scada.df)

        self._scada.df = pd.concat(scads) # combining of separate dataframes

        self._scada.df = self._scada.df[['time', 'wtur_W_avg', 'wmet_wDir_avg', 'wmet_wdspd_avg', 'ID']]
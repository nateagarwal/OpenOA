######################################
#Data import script for Project Engie#
######################################
"""
This is the import script for project Engie. Below is a description of data quality for each data frame
and an overview of the steps taken to correct the raw data.

1. Turbine dataframe

2. Meter dataframe

3. Availability and curtailment dataframe

4. Reanalysis dataframe

"""
from operational_analysis.types import PlantData

import numpy as np
import pandas as pd
import operational_analysis.toolkits.timeseries as ts
import operational_analysis.toolkits.unit_conversion as un
from operational_analysis.toolkits import filters

from operational_analysis import logged_method_call
from operational_analysis import logging

logger = logging.getLogger(__name__)

class Project_Engie(PlantData):
    """This class loads data for the Engie site into a PlantData object"""

    def __init__(self, path="./",
                 name="engie", engine="pandas"):

        super(Project_Engie, self).__init__(path, name, engine)

    def prepare(self):
        """
        Do all loading and preparation of the data for this plant.
        """     
        # Set time frequencies of data in minutes
        self._scada_freq = '10T' # 10-min
        self._meter_freq = '1H' # 1 hour
        self._curtail_freq = '1H' # 1 hour
        
        # Load meta data
        self._lat_lon = (48.4461, 5.5925)
        self._plant_capacity = 8.2 # MW
        self._num_turbines = 4
        self._turbine_capacity = 2.05 # MW
        
        ###################
        # SCADA DATA #
        ###################
        logger.info("Loading SCADA data")
        self._scada.load(self._path, "engie_scada", "csv")  # Load Scada data
        logger.info("SCADA data loaded")
        
        logger.info("Timestamp QC and conversion to UTC")
        # Get 'time' field in datetime format
        self._scada.df['time']=pd.to_datetime(self._scada.df['time'])
        
        # Remove duplicated timestamps and turbine id
        self._scada.df.drop_duplicates(subset = ['time', 'ID'], inplace=True)

        # Convert local to UTC time
        #self._scada.df['time_utc'] = self.dst_shifter(dt= self._scada.df['time'], num_hours = -2, dst=True, dst_subset= 'French') # This function is defined below prepare
        #self._scada.df['time'] = self._scada.df['time_utc']

        # Remove duplicated timestamps and turbine id
        #self._scada.df.drop_duplicates(subset = ['time', 'ID'], inplace=True)

        # Set datetime as index
        self._scada.df.set_index('time',inplace=True,drop=False) 
        
        logger.info("Correcting for out of range of power, wind speed, and wind direction variables")
        #Handle extrema values
        self._scada.df = self._scada.df[(self._scada.df["wmet_wdspd_avg"]>=0.0) & (self._scada.df["wmet_wdspd_avg"]<=40.0)]
        self._scada.df = self._scada.df[(self._scada.df["wtur_W_avg"]>=-1000.0) & (self._scada.df["wtur_W_avg"]<=2200.0)]
        self._scada.df = self._scada.df[(self._scada.df["wmet_wDir_avg"]>=0.0) & (self._scada.df["wmet_wDir_avg"]<=360.0)]            

        logger.info("Flagging unresponsive sensors")
        #Flag repeated values from frozen sensors
        temp_flag = filters.unresponsive_flag(self._scada.df["wmet_wdspd_avg"], 3)
        self._scada.df.loc[temp_flag, 'wmet_wdspd_avg'] = np.nan
        temp_flag = filters.unresponsive_flag(self._scada.df["wmet_wDir_avg"], 3)
        self._scada.df.loc[temp_flag, 'wmet_wDir_avg'] = np.nan
        
        # Put power in watts; note although the field name suggests 'watts', it was really reporting in kw
        self._scada.df["wtur_W_avg"] = self._scada.df["wtur_W_avg"] * 1000
        
        # Calculate energy
        self._scada.df['energy_kwh'] = un.convert_power_to_energy(self._scada.df["wtur_W_avg"], self._scada_freq)/ 1000
        
        logger.info("Converting field names to IEC 61400-25 standard")
        #Map to -25 standards

        scada_map = {"time"                 : "time",
                     "ID"       : "id",
                     "wtur_W_avg"              : "wtur_W_avg",
                     "wmet_wdspd_avg"    : "wmet_wdspd_avg", 
                     "wmet_wDir_avg"    : "wmet_HorWd_Dir"
                     }

        self._scada.df.rename(scada_map, axis="columns", inplace=True)
        
        # Remove the fields we are not yet interested in
        self._scada.df.drop(['time.1'], axis=1, inplace=True)

        ###################
        # METER DATA #
        ###################

        self._meter.load(self._path, "engie_meter", "csv")  # Load Meter data
        self._meter.df['time'] = pd.to_datetime(self._meter.df['time'])
        
        #Remove duplications
        self._meter.df.drop_duplicates(subset=['time'], inplace=True, keep=False)

        # Convert local to UTC time:
        #self._meter.df['time_utc'] = self.dst_shifter(dt= self._meter.df['time'], num_hours = -2, dst=True, dst_subset= 'French')
        #self._meter.df['time'] = self._meter.df['time_utc'] 
        self._meter.df.set_index('time',inplace=True,drop=False) # Set datetime as index
        #self._meter.df.drop_duplicates(subset=['time'], inplace=True, keep=False) 

        #remove extrema values
        self._meter.df = self._meter.df[self._meter.df['energy_kwh']>0]

        #Drop fields we don't need
        #self._meter.df.drop(['time_utc'], axis=1, inplace=True)

        #####################################
        # Availability and Curtailment Data #
        #####################################
        
        logger.info("Loading Availability and Curtailment Data")
        self._curtail.load(self._path, "engie_avail_curt", "csv") # Load availability curtailment data
        self._curtail.df['time'] = pd.to_datetime(self._curtail.df['time']) #Convert time field to datetime object
        self._curtail.df.drop_duplicates(subset=['time'], inplace=True, keep=False)
        #self._curtail.df['time_utc'] = self.dst_shifter(dt= self._curtail.df['time'], num_hours = -2, dst=True, dst_subset= 'French') # convert from local time to UTC
        #self._curtail.df.drop_duplicates(subset=['time'], inplace=True, keep=False)
        self._curtail.df.set_index('time', inplace=True, drop=False)
        
        self._curtail.df.dropna(inplace=True)

        ###################
        # REANALYSIS DATA #
        ###################

        # merra2
        self._reanalysis._product['merra2'].load(self._path, "merra2_data", "csv")
        self._reanalysis._product['merra2'].rename_columns({"time": "datetime",
                                                            "windspeed_ms": "ws_50m",
                                                            "rho_kgm-3": "dens_50m",
                                                            "winddirection_deg": "wd_50m"})
        self._reanalysis._product['merra2'].normalize_time_to_datetime("%Y-%m-%d %H:%M:%S")
        self._reanalysis._product['merra2'].df.set_index('time', inplace=True, drop=False)

        # ncep2
        self._reanalysis._product['ncep2'].load(self._path, "ncep2_data", "csv")
        self._reanalysis._product['ncep2'].rename_columns({"time": "datetime",
                                                           "windspeed_ms": "ws_10m",
                                                           "rho_kgm-3": "dens_10m",
                                                           "winddirection_deg": "wd_10m"})
        self._reanalysis._product['ncep2'].normalize_time_to_datetime("%Y%m%d %H%M")
        self._reanalysis._product['ncep2'].df.set_index('time', inplace=True, drop=False)

        # erai
        self._reanalysis._product['erai'].load(self._path, "erai_data", "csv")
        self._reanalysis._product['erai'].rename_columns({"time": "datetime",
                                                          "windspeed_ms": "ws_58",
                                                          "rho_kgm-3": "dens_58",
                                                          "winddirection_deg": "wd_58"})
        self._reanalysis._product['erai'].normalize_time_to_datetime("%Y-%m-%d %H:%M:%S")
        self._reanalysis._product['erai'].df.set_index('time', inplace=True, drop=False)

    def dst_shifter(self, dt, num_hours, dst=False, dst_subset = 'American'):
        """
        This method takes a set of local timestamp data and converts it to utc.
        Args:
            dt(:obj:`pandas series`): series of datetime objects in local time
            num_hours(:obj:'int'): Time offset between local (no DST, e.g. on Jan 1) and UTC (positve value)
            dst(:obj:`boolean'): True if data requires a Daylight Savings Time correction and false otherwise
        Returns:
            dt_shift(:obj:`pandas series): dt(:obj:`pandas series`): series of datetime objects in UTC time
        """

        dt_shift = dt.copy()
    
        if dst == False: #If DST is not evident in the data, simply shift all data by the same number of hours
            dt_shift = dt + pd.DateOffset(hours = num_hours)
        else:
            if dst_subset == 'American':
                # American DST Transition Dates (Local Time)
                dst_dates = pd.DataFrame()
                dst_dates['year'] =  [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019]
                dst_dates['start'] = ['3/9/08 2:00', '3/8/09 2:00', '3/14/10 2:00', '3/13/11 2:00', '3/11/12 2:00', '3/10/13 2:00', '3/9/14 2:00', '3/8/15 2:00', '3/13/16 2:00', '3/12/17 2:00', '3/11/18 2:00' , '3/10/19 2:00']
                dst_dates['end'] = ['11/2/08 2:00', '11/1/09 2:00', '11/7/10 2:00', '11/6/11 2:00', '11/4/12 2:00', '11/3/13 2:00', '11/2/14 2:00', '11/1/15 2:00', '11/6/16 2:00', '11/5/17 2:00', '11/4/18 2:00', '11/3/19 2:00']
            else:
                # European DST Transition Dates (Local Time)
                dst_dates = pd.DataFrame()
                dst_dates['year'] = [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019]
                dst_dates['start'] = ['3/30/08 2:00', '3/29/09 2:00', '3/28/10 2:00', '3/27/11 2:00', '3/25/12 2:00', '3/31/13 2:00', '3/30/14 2:00', '3/29/15 2:00', '3/27/16 2:00', '3/26/17 2:00', '3/25/18 2:00' , '3/31/19 2:00']
                dst_dates['end'] = ['10/26/08 3:00', '10/25/09 3:00', '10/31/10 3:00', '10/30/11 3:00', '10/28/12 3:00', '10/27/13 3:00', '10/26/14 3:00', '10/25/15 3:00', '10/30/16 3:00', '10/29/17 3:00', '10/28/18 3:00', '10/27/19 3:00']
                
                years = dt.dt.year.unique() # Years in data record
                num_years = len(years)
        
                for y in np.arange(num_years): #We will apply the DST correction by year
                    dst_data = dst_dates.loc[dst_dates['year']== years[y]] 
            
                    # Set DST correction window
                    spring_start = pd.to_datetime(dst_data['start'])
                    spring_end = pd.to_datetime(dst_data['end']) - pd.DateOffset(hours=1)
            
                    # Set 'fall' window from November date to end of year (December 31)
                    fall_start_1 = pd.to_datetime(dst_data['end']) - pd.DateOffset(hours=1)
                    fall_end_1 = pd.to_datetime('%s-12-31 23:59:59' % years[y], format = '%Y-%m-%d %H:%M:%S')
            
                    # Set second 'fall' window from January 1st to DST boundary
                    fall_start_2 = pd.to_datetime('%s-01-01 00:00:00' % years[y], format = '%Y-%m-%d %H:%M:%S')
                    fall_end_2 = pd.to_datetime(dst_data['start'])
            
                    # Shift spring data
                    ind1 = (dt > spring_start.values[0]) & (dt < spring_end.values[0])
                    dt_shift[ind1] = dt[ind1] + pd.DateOffset(hours= (num_hours-1))

                    # Shift fall data
                    ind2 = (dt >= fall_start_1.values[0]) & (dt < fall_end_1)
                    dt_shift[ind2] = dt[ind2] + pd.DateOffset(hours = num_hours)
            
                    # Shift fall data stage 2
                    ind3 = (dt >= fall_start_2) & (dt < fall_end_2.values[0])
                    dt_shift[ind3] = dt[ind3] + pd.DateOffset(hours = num_hours)
            
            return dt_shift
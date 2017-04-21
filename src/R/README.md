# Data Exploration

Preliminary data exploration was done in `R`.

First, I load the joined data file and clean out the extra header rows left over from the Google BigQuery download and file concatenation.

Then, I tested the data processing (creating the outlier detection algorithm) in TestDataProcessing.ipynb.

Finally, the NDT_data_exploration.ipynb has the data exploration and plotting as well as the data processing in order to prepare the data for import into Hive.
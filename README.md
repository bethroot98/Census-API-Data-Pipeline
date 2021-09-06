# Census-API-Data-Pipeline

This file takes data from the American Community Survey Census API to look at the total population and population by sex and race by block groups and the average income and unemployment rate for counties in Kentucky. I chose these variables since they show basic demographic and economic data about counties and block groups within Kentucky. For the average income and unemployment rate data, I went with county-based data since the data is not available on a block-group level within the American Community Survey. The population data points are all available on the block-group level with the American Community Survey and they can show how Kentucky's population is distributed. 

Note, in order for the code to work, you would need to obtain your own census API key and database information to build your own table and create a data pipeline.

# Sources:
https://www.stratascratch.com/blog/importing-pandas-dataframe-to-database-in-python/
http://data.census.gpv

# Census-API-Data-Pipeline

This file takes data from the American Community Survey Census API to look at the total population and population by sex and race by block groups and the average income and unemployment rate for counties in Kentucky. I chose these variables since they show basic demographic and economic data about counties and block groups within Kentucky. I chose data from Kentucky since that is where I am from and I was interested in seeing how the population vaires by sex and race, income, and the unemployment rate varies throughout the state. 

For the average income and unemployment rate data, I went with county-based data from the subject tables in the American Community Survey since the data is not available on a block-group level within the American Community Survey detailed tables. The population data points are all available on the block-group level with the American Community Survey and they can show how Kentucky's population is distributed. I mainly restructured the data by providing what would be a composity primary key of state_id, county_id, tract_id, and block_group_id inside of a database. The other variable that is changed within the code is the name variable, Which is borken up to make it easier to group by these name values in a dataabse. Since the income and unemployment rate data is not available on the block level, I also created a new dataframe with only those two variables from the subject tables instead of the detailed tables and then merged both the demographic dataframe and the economic dataframe to create the full dataframe that is loaded within the database.

I created the database connector functions as user intputs, to make it more accessible for a larger audience and keep the database information private from GitHub. The needed package would be the psycopg2 module in python. You would also need to create the database prior to loading the dataframe into the database.

Note, in order for the code to work, you would need to obtain your own census API key and database information to build your own table and create a data pipeline.

# Sources:
https://www.stratascratch.com/blog/importing-pandas-dataframe-to-database-in-python/

http://data.census.gpv

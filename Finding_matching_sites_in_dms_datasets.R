# Time series at each of the TRPI sites

library(arrow)
#Data manipulation
library(tidyverse)
library(janitor)
#Managing spatial data
library(sf)

# import TRPI sites
sites <- read_csv("R_based_scripts/SST_DHW_TRPI_26_Sites_Jan2024.csv") 

#Establishing connection
data_bucket <- s3_bucket("s3://gbr-dms-data-public/aims-temp-loggers/data.parquet")

#Accessing dataset
data_df <- open_dataset(data_bucket)

#Getting unique sites
sites_data <- data_df |> 
  distinct(site, lat, lon, gbrmpa_reef_id) |> 
  collect()

#Site names and GBRMPA site IDs do NOT match in the two different datasets.
#Coordinates do not exactly match. This creates an issue because it is NOT
#possible to perform a match

## Possible solution 1 - Clean up sites information to facilitate finding
## matches
sites <- sites |> 
  #Cleaning up names
  clean_names() |> 
  #Removing column not needed
  select(!x7) |> 
  #Removing letters from GBRMPA IDs
  mutate(gbrmpa_reef_id = str_to_lower(gbrmpa_id), 
         gbrmpa_reef_id = str_remove_all(gbrmpa_reef_id, "[a-z]"))

#We are now ready to identify the sites of interest through a join
sites_merged <- sites |> 
  left_join(sites_data, by = join_by("gbrmpa_reef_id"))
#This will need to be checked and you can keep whichever sites are of your
#interest. You may notice that some sites are repeated multiple times and this
#is because their coordinates have changed over time.

#For this example, we will keep all names identified
sites_lookup <- sites_merged |> 
  #Removing NA rows
  tidyr::drop_na(site) |> 
  #Keeping unique values
  distinct(site) |> 
  #Turning to vector
  pull()
#This will give us a vector with the unique site names of our interest

#We do a query from the S3 bucket
temp_data <- data_df |> 
  #Select only sites of interest
  filter(site %in% sites_lookup) |>
  #For this example, I will choose just one year worth of data - the last one
  mutate(year = year(time)) |> 
  filter(year == 2021) |> 
  collect()

#That's it! You got the data you need.


## Possible solution 2 - Spatial approach
#We will turn the unique dataset sites and your sites to sf objects before
#identifying the nearest points between them

sites_shp <- sites |> 
  #Here I am assuming the CRS of your data is 4326, change if needed
  st_as_sf(coords = c("lon", "lat"), crs = 4326)
  
sites_data_shp <- sites_data |> 
  #CRS is correct for this dataset. Check STAC page for more details
  st_as_sf(coords = c("lon", "lat"), crs = 4326)

##NOTE: If the CRS is different between them, you will need to match the CRS
#You can use the st_transform() function for more details

#We can plot them
ggplot()+
  #Red for AIMS temperature data
  geom_sf(data = sites_data_shp, color = "red")+
  #Green for your data
  geom_sf(data = sites_shp, color = "green")
#As we saw at the beginning, sites do not exactly match, let's find the 
#closest pairs

#This will give us the index of features in the second input that are closest
#to the first input
matching_pts <- st_nearest_feature(sites_shp, sites_data_shp)

#We plot again
ggplot()+
  #Red for AIMS temperature data
  geom_sf(data = sites_data_shp, color = "red")+
  #Green for your data
  geom_sf(data = sites_shp[19,], color = "green")+
  #Purple for matching features
  geom_sf(data = sites_data_shp[matching_pts,], color = "purple")
#As we saw above some features could not be matched

#We can now use this information to extract data from the AIMS dataset
pts_interest <- sites_data_shp[matching_pts,] |> 
  #Convert to data frame
  st_drop_geometry() |> 
  #Keep GBRMPA ID as vector for search
  pull(site)

#Then we do a query as above
temp_data2 <- data_df |> 
  #Select only sites of interest
  filter(gbrmpa_reef_id %in% pts_interest) |>
  #For this example, I will choose just one year worth of data - the last one
  mutate(year = year(time)) |> 
  filter(year == 2021) |> 
  collect()
#This yields no results. If you check the temp_data, you can see that the only
#site returned was Hook Island, which was not identified by the second approach.
#The reason for this is that in the AIMS dataset, Hook Island has these coords:
#148.8871, -20.1681. While your sites has two coords: 148.9214, -20.06941, and 
#148.9029, -20.08348. The closest site identified to either of these was Hayman
#Island (148.8819 -20.0413).


#The approach you follow here will depend on your needs. The good thing is that
#you will only need to do this once for each dataset. Save the coordinate pairs
#or names for future reference.





#Home exam fall 2021, BAN400
#Candidate 21

#This code takes approx. 45 seconds to run.

#Packages needed to run this script is stated below.

library(httr)
library(jsonlite)
library(ggplot2)
library(DescTools)
library(tidyverse)
library(magrittr)
library(rlang)
library(lubridate)
library(anytime)
library(purrr)
library(mdsr)        
library(sf)          
library(ggmap)       
library(ggplot2)
library(rnaturalearth)
library(rnaturalearthdata)
library(rgeos)
library(sf)
library(ggspatial)
library(dplyr)
library(tidymodels)
library(knitr)
library(bannerCommenter)
library(modelr)
library(parallel)
library(furrr)
library(multidplyr)


###########################################################################
###########################################################################
###                                                                     ###
###                              PROBLEM 1                              ###
###                                                                     ###
###########################################################################
###########################################################################
#A)
#Working directory is assumed to contain a folder called data containing json files, and the one csv file in problem 1.

df_202101_csv <-
  read_csv("data/01.csv")

df_202101_json <-
  read_json("data/01.json",
            simplifyVector = TRUE) %>%
  as_tibble() %>%
  mutate(started_at = as.POSIXct(started_at, tz = "Europe/Berlin")) %>% #Change from character to date format. In the CSV file the dates have the POSIXct format.
  mutate(ended_at = as.POSIXct(ended_at, tz = "Europe/Berlin")) %>% #Change from character to date format
  mutate(
    start_station_id = as.numeric(start_station_id),
    end_station_id = as.numeric(end_station_id)
  )

summary(df_202101_json)
summary(df_202101_csv)

#The data frames are imported and modified in the same manner as the csv file. Some variables had to be converted
#to a certain date and numeric format.

#B)

#and then we make a function to test whether our to dataframes are equal. 
#The function is based on the anti_join function which returns all rows from a data frame x that  has no match in y. 
#If the number of columns and rows in the tmpdf is equal to the ones in the json data frame, it means that all the columns and rows are identical.

test_equal_df <- function(df1, df2) {
  tmpdf <-  anti_join(df1, df2)
  if (ncol(tmpdf) - ncol(df2) == 0) {
    print("The columns and the values in the two dataframes are the same")
  } else{
    print("The columns and the values are not the same in the two dataframes")
  }
}

test_equal_df(df_202101_csv, df_202101_json)

rm(df_202101_csv, df_202101_json, test_equal_df)

###########################################################################
###########################################################################
###                                                                     ###
###                              PROBLEM 2                              ###
###                                                                     ###
###########################################################################
###########################################################################

#The most time consuming process in this file is the loading of the json files.
#We therefore use the purr package to take advantage of multiple cores of the
#computer in this process.
#The effect can be seen in the task manager on windows, or activity monitor on a Mac.
#The processing time seems to be some seconds faster than without this code. The amount of
# time spent on the multiple core operation may be slowed down
# by overhead processes when nodes have to commmunicate with each other.

Cores <- detectCores() - 1

plan(multisession,
     workers = Cores)

require(dplyr)
require(jsonlite)
require(tidyverse)


citybike_raw <-
  list.files(path = "./data/",
             pattern = "*json",
             full.names = TRUE) %>%
  future_map_dfr( ~ read_json(.), simplifyVector = TRUE) %>%
  as_tibble()


#Working with the raw data also is a time consuming processs. Multidplyr can be used to work with dplyr data frames
# to speed up the process by assigning processing power to more cores. Note that overhead processes might slow
# the time here as well due to nodes working together. We assign one group to each node in this case.The groups are based on the 
# citybike data frame where we split this frame up into 1:Cores groups.

group <- rep(1:Cores,
             length.out = nrow(citybike_raw))
citybike_raw <- bind_cols(tibble(group),
                          citybike_raw)
citybike_raw

cluster <- new_cluster(Cores)
cluster_library(cluster,
                "dplyr")

#Cluster_library makes sure dplyr is loaded to each node.

citybike_raw <- citybike_raw %>%
  group_by(group) %>%
  partition(cluster) %>%
  mutate(
    floor_start_dh = as.POSIXct(started_at, tz = "Europe/Berlin"),
    ended_at = as.POSIXct(ended_at, tz = "Europe/Berlin"),
    #Change from character to date format
    start_station_id = as.numeric(start_station_id),
    end_station_id = as.numeric(end_station_id),
    start_date = as.Date(floor_start_dh),
    start_hour = lubridate::hour(floor_start_dh),
    weekday_start = lubridate::wday(floor_start_dh),
    floor_start_dh = lubridate::floor_date(floor_start_dh,
                                           unit = "hour")
  ) %>%
  collect() %>%
  ungroup() %>%
  select(-group) %>%
  distinct()

#rm(cluster, group)



citybike_raw %>%
  ggplot(aes(start_station_id)) +
  geom_histogram(bins = 1000) +
  xlab("Start Station Id") +
  ylab("Count of Start Station ID") +
  theme_classic()


#We create the df_agg data frame inside a function to save space by making the data frames and values used only temporary.
#It is important to note that functions only makes a copy of the data frames defined inside the function, and we must add a return()
#for it to go outside the function as well.
#Ideally the code should be made in one pipe, but the allocation of memory failed when trying it out.

create_df_agg <- function() {
  df_agg <- citybike_raw %>%
    group_by(start_station_id) %>%
    filter(n() > 15000) %>%
    ungroup() %>%
    select(start_station_id,
           floor_start_dh,
           start_hour,
           weekday_start) %>%
    arrange(start_station_id,
            floor_start_dh) %>%
    group_by(floor_start_dh,
             start_station_id) %>%
    mutate(n_rides = n()) %>%
    summarise(n_rides) %>%
    group_by(start_station_id)
  
  #We filter our set based on the count of observations linked to each station. Setting the filter to count > 15000
  # because of processing issues in the further problems. Crucial to this problem is also to group our set by the start stations.
  
  #The following lines will create a frame for each possible
  #combination of the hours and dates for the floor_start_dh column for our given set.
  pos.all <-
    seq(min(df_agg$floor_start_dh),
        max(df_agg$floor_start_dh),
        by = "1 hour")
  
  #The following is the key to solving the problem. We complete our data frame by filling in the missing positions for the
  #floor_start_dh such that we have one observation for each hour and date of the set inside the given time period.
  #At the same time we fill the missing n_rides values with NA for now. This will obviously be 0 in the result.
  df_comp <-
    complete(df_agg,
             floor_start_dh = pos.all,
             fill = list(n_rides = NA))
  
  
  df_agg <- df_comp %>%
    mutate(
      n_rides = (replace_na(n_rides, 0)),
      start_hour = lubridate::hour(floor_start_dh),
      weekday_start = (wday(floor_start_dh))
    ) %>%
    distinct() %>%
    filter(cumsum(n_rides) > 0) %>%
    arrange(desc(floor_start_dh)) %>%
    filter(cumsum(n_rides) > 0) %>%
    arrange(floor_start_dh, start_station_id) %>%
    ungroup() %>%
    mutate(start_hour = factor(start_hour),
           weekday_start = factor(weekday_start))
  
  return(df_agg)
}

df_agg <- create_df_agg()

#At the end of the code above it is filtered on whether the stations are operational.
#The cumsum of the n_rides has to be greater than 0 at the start of the set in order to be operational.(Assumption)
# If we arrange the dataframe in descending order when we group by start_station_id and arrange in descending order
# by the floor_start_dh, we can filter again on the cumsum of nrides > 0. As long as the cumsum of rides are = 0 in either end,
# we assume the stations are not operational



library(assertthat)
assert_that(
  df_agg %>%
    group_by(start_station_id,
             floor_start_dh) %>%
    summarise(n = n()) %>%
    ungroup() %>%
    summarise(max_n = max(n)) %$%
    max_n == 1,
  msg = "Duplicates on stations/hours/dates"
)

assert_that(
  df_agg %>%
    group_by(start_station_id) %>%
    mutate(timediff = floor_start_dh - lag(floor_start_dh,
                                           order_by = floor_start_dh)) %>%
    filter(as.numeric(timediff) != 1) %>%
    nrow(.) == 0,
  msg = "Time diffs. between obs are not always 1 hour"
)



#The code is not optimal due to the heavy processing power needed to do this task initially.
#Therefore the data frames are updated gradually so it is easier to work with. Optimally the frames would be made in one pipe.
#The most consuming function in this problem was the complete()-function.



###########################################################################
###########################################################################
###                                                                     ###
###                              PROBLEM 3                              ###
###                                                                     ###
###########################################################################
###########################################################################


#We nest the df_agg set by stations, such that we have one observation for each station.


by_station <- df_agg %>%
  group_by(start_station_id) %>%
  nest()


#We have a multiple regression where all our explanatory variables are categorical variables, which leads us to a parallel
# slopes model.

#Creating a function for the regression model to be used in the mapping below.

reg_model <- function(df) {
  lm(n_rides ~ weekday_start + start_hour, data = df)
}

#In the following we map through each station and apply the regression model to each station using the single stations operations.
#We then can add the predictions from this model.

by_station <- by_station %>%
  mutate(model = map(data,
                     reg_model),
         pred = map2(data,
                     model,
                     add_predictions))
by_station

#The by_station data frame contains lists of the models and the coefficients and residuals for each station, including our predictions.
#The regression model minimizes the squared residuals and fits the regression line in this manner based on our observations.

#By mapping through our result lists in the data and the model list using map2 function, we can use the add_predictions function in tidymodels to make predictions on the different combinations
# of weekday and hour of the day. We use map2 here because we map through two lists at the same time, the model and the data list for each station.

#Note about inference: The unnesting of the lists took quite some time. There are two options to get to coefficients and statistics of the test; Using the unnest function, or manually
# go into the by_station frame and go into the different lists. Due to time constraints and R crashing repeatedly when doing these operations, inference has not been done confidently.
# However, the p-values and test-statistics looks overall good. 

#If we want the mean characteristics of the tests, we can also do so by mapping through the list and applying the mean function to different data points, like test_statistics and coefficients. 

#We need a function to create a data frame for the stations in order to get to the station names.
create_stations <- function() {
  stations <- citybike_raw %>%
    select(start_station_id,
           start_station_name) %>%
    distinct() #These small dataframes are helpful because R has repeatedly crashed during this assignment.
  return(stations)
}

#We make a data frame pred over the predictions and data for each station. We also
# make a left_join to add the station names.

pred <- by_station %>%
  unnest(pred) %>%
  select(start_station_id,
         start_hour,
         weekday_start,
         pred) %>%
  distinct() %>%
  arrange(start_station_id,
          start_hour) %>%
  left_join(create_stations(),
            by = "start_station_id")

days <-
  c("Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday")

#In order to get to our final plot, we need to group the observations and facet_wrap the plot by weekday
# to split the plot into seven different plots.

pred %>%
  group_by(start_hour,
           weekday_start,
           start_station_id) %>%
  distinct()  %>%
  mutate(weekday = factor(days[weekday_start],
                          levels = days)) %>%
  ggplot(aes(
    start_hour,
    pred,
    group = as.factor(start_station_id),
    color = as.factor(start_station_name)
  )) +
  geom_point() +
  geom_line() +
  facet_wrap( ~ weekday, scales = 'free') +
  ylab("Mean Predicted Rides Per Hour Per Station") +
  xlab("Hour of day") +
  theme_classic() +
  scale_colour_brewer(palette = "Spectral") +
  labs(title = "Rides throughout day and week for each station\n",
       color = "Start Station Name") +
  theme(plot.title = element_text(size = 18,
                                  color = "black"))

#To get the best picture of the plot, please press zoom and expand the window horizontally.



###########################################################################
###########################################################################
###                                                                     ###
###                              PROBLEM 4                              ###
###                                                                     ###
###########################################################################
###########################################################################

#To save space we make a couple of functions to retrieve the coordinates for each station and observation.

create_station_coordinates <- function() {
  station_coordinates <- citybike_raw %>%
    select(
      start_station_id,
      start_station_latitude,
      start_station_longitude,
      start_date,
      start_station_name
    ) %>%
    distinct() %>%
    mutate(lat = start_station_latitude,
           lon = start_station_longitude)
  return(station_coordinates)
}


# In order to account for changing of location of stations, we have to include the date column in the coordinates for the stations
# to makes sure that when we choose a station for a given date, we are sure to be getting the right coordinates.

#To save space we make a function to make the data frame with the coordinates for each station.

create_df_agg_coord <- function() {
  df_agg_coord <- df_agg %>%
    mutate(start_date = as.Date(floor_start_dh)) %>%
    left_join(create_station_coordinates(),
              by = c("start_station_id", "start_date")) %>%
    select(start_station_id,
           n_rides,
           start_hour,
           start_date,
           lat,
           lon,
           start_station_name) %>%
    mutate(start_date = format(as.Date(start_date),
                               '%d-%m-%Y'))
  return(df_agg_coord)
}


#The function takes the arguments date and hour, and based on this it makes a map for a given day and hour. The tmpdf data frame 
# is necessary to give us the data frame where we filter on the date and hour. Based on this data frame, a map can be made for the
# given input.

map_datehour <- function(date, hour) {
  tmpdf <- create_df_agg_coord() %>%
    select(start_date,
           start_hour,
           n_rides,
           start_station_id,
           start_station_name,
           lat,
           lon) %>%
    filter(start_date == date,
           start_hour == hour)
  bergen <-
    make_bbox(tmpdf$lon, #Være nøye med lat lon rekkefølgen!
              tmpdf$lat,
              f = .05) #Expand by 5 %
  m <- get_map(bergen, zoom = 15, source = "osm")
  ggmap(m) +
    geom_point(aes(
      x = lon,
      y = lat,
      size = n_rides,
      color = as.factor(start_station_name)
    ), data = tmpdf) +
    scale_colour_brewer(palette = "Spectral") +
    labs(title = "Activity of the stations for given dates and hours\n", color = "Start Station Name") +
    theme(plot.title = element_text(size = 18, color = "black")) +
    ylab("Latitude") +
    xlab("Longitude") +
    labs(size = "Number \nOf Rides")
  
}

map_datehour("01-10-2021", 15) #Make sure to use the right date format when using the function. "" must be wrapped around the date input for the function to work.

#More comments also in the .Rmd file. However, the most important intuition is described in the R-script.
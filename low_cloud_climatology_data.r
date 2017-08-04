# low_cloud_climatology.r

options(scipen=3)
require(doParallel)
registerDoParallel(cores = 3)

setwd("/home/jim/Dropbox/Projects/low_cloud_climatology")

# reads in metar data
setwd("/media/jim/FAT323/Projects/low_cloud_climatology")
require(data.table)
metar <- fread("HM01X_Data_040842_999999999445572.txt"
  # , nrow = 0
  # , header = FALSE
  , sep = ","
  # , strip.white = FALSE
  , stringsAsFactors = FALSE
  # , data.table = FALSE
  )
# metar <- metar[1:10000, ]
# metar < - as.data.frame(metar, optional = TRUE)
head(metar)

# sets column names to be shorter
setnames(metar, "hm", "hm")
setnames(metar, "Station Number", "stnnumb")
setnames(metar, "Day/Month/Year in DD/MM/YYYY format", "dateloc")
setnames(metar, "Hour24:Minutes  in HH24:MI format in Local standard time", "timeloc")
setnames(metar, "Precipitation in last 10 minutes in mm", "preci10")
setnames(metar, "Quality of precipitation in last 10 minutes", "prec10q")
setnames(metar, "Precipitation since 9am local time in mm", "prec9am")
setnames(metar, "Quality of precipitation since 9am local time", "prec9aq")
setnames(metar, "Air Temperature in degrees C", "tempera")
setnames(metar, "Quality of air temperature", "tempqua")
setnames(metar, "Wet bulb temperature in degrees C", "wetbulb")
setnames(metar, "Quality of Wet bulb temperature", "wetbulq")
setnames(metar, "Dew point temperature in degrees C", "dewpntt")
setnames(metar, "Quality of dew point temperature", "dewpntq")
setnames(metar, "Relative humidity in percentage %", "relhumi")
setnames(metar, "Quality of relative humidity", "relhumq")
setnames(metar, "Vapour pressure in hPa", "vappres")
setnames(metar, "Quality of vapour pressure", "vappreq")
setnames(metar, "Saturated vapour pressure in hPa", "satvapp")
setnames(metar, "Quality of saturated vapour pressure", "satvapq")
setnames(metar, "Wind speed in km/h", "windkmh")
setnames(metar, "Wind speed quality", "windkmq")
setnames(metar, "Wind direction in degrees true", "winddir")
setnames(metar, "Wind direction quality", "winddrq")
setnames(metar, "Speed of maximum windgust in last 10 minutes in  km/h", "windgkm")
setnames(metar, "Quality of speed of maximum windgust in last 10 minutes", "wingkmq")
setnames(metar, "Cloud amount(of first group) in eighths", "cldoct1")
setnames(metar, "Quality of first group of cloud amount", "cldamq1")
setnames(metar, "Cloud type(of first group) in in_words", "cldtyp1")
setnames(metar, "Quality of first group of cloud type", "cldtyq1")
setnames(metar, "Cloud height (of first group) in feet", "cldhgt1")
setnames(metar, "Quality of first group of cloud height", "cldhtq1")
setnames(metar, "Cloud amount(of second group) in eighths", "cldoct2")
setnames(metar, "Quality of second group of cloud amount", "cldamq2")
setnames(metar, "Cloud type(of second group) in in_words", "cldtyp2")
setnames(metar, "Quality of second group of cloud type", "cldtyq2")
setnames(metar, "Cloud height (of second group) in feet", "cldhgt2")
setnames(metar, "Quality of second group of cloud height", "cldhtq2")
setnames(metar, "Cloud amount(of third group) in eighths", "cldoct3")
setnames(metar, "Quality of third group of cloud amount", "cldamq3")
setnames(metar, "Cloud type(of third group) in in_words", "cldtyp3")
setnames(metar, "Quality of third group of cloud type", "cldtyq3")
setnames(metar, "Cloud height (of third group) in feet", "cldhgt3")
setnames(metar, "Quality of third group of cloud height", "cldhtq3")
setnames(metar, "Cloud amount(of fourth group) in eighths", "cldoct4")
setnames(metar, "Quality of fourth group of cloud amount", "cldamq4")
setnames(metar, "Cloud type(of fourth group) in in_words", "cldtyp4")
setnames(metar, "Quality of fourth group of cloud type", "cldtyq4")
setnames(metar, "Cloud height (of fourth group) in feet", "cldhgt4")
setnames(metar, "Quality of fourth group of cloud height", "cldhtq4")
setnames(metar, "Ceilometer cloud amount(of first group)", "cldocc1")
setnames(metar, "Quality of first group of ceilometer cloud amount", "cldacq1")
setnames(metar, "Ceilometer cloud height (of first group) in feet", "cldhtc1")
setnames(metar, "Quality of first group of ceilometer cloud height", "cldhcq1")
setnames(metar, "Ceilometer cloud amount(of second group)", "cldocc2")
setnames(metar, "Quality of second group of ceilometer cloud amount", "cldacq2")
setnames(metar, "Ceilometer cloud height (of second group) in feet", "cldhtc2")
setnames(metar, "Quality of second group of ceilometer cloud height", "cldhcq2")
setnames(metar, "Ceilometer cloud amount(of third group)", "cldocc3")
setnames(metar, "Quality of third group of ceilometer cloud amount", "cldacq3")
setnames(metar, "Ceilometer cloud height (of third group) in feet", "cldhtc3")
setnames(metar, "Quality of third group of ceilometer cloud height", "cldhcq3")
setnames(metar, "Horizontal visibility in km", "visobsk")
setnames(metar, "Quality of horizontal visibility", "visobsq")
setnames(metar, "Direction of minimum visibility in degrees", "visobmn")
setnames(metar, "Quality of direction of minimum visibility", "visobmq")
setnames(metar, "AWS visibility in km", "visawsk")
setnames(metar, "Quality of AWS(Automatic Weather Station) visibility", "visawsq")
setnames(metar, "Present weather in text", "presewx")
setnames(metar, "Quality of present weather", "preswxq")
setnames(metar, "Intensity of first present weather in text", "prwxin1")
setnames(metar, "Quality of intensity of first present weather", "prwxiq1")
setnames(metar, "Descriptor of first present weather in text", "prwxde1")
setnames(metar, "Quality of descriptor of first present weather", "prwxdq1")
setnames(metar, "Type of first present weather in text", "prwxty1")
setnames(metar, "Quality of type of first present weather", "prwxtq1")
setnames(metar, "Intensity of second present weather in text", "prwxin2")
setnames(metar, "Quality of intensity of second present weather", "prwxiq2")
setnames(metar, "Descriptor of second present weather in text", "prwxde2")
setnames(metar, "Quality of descriptor of second present weather", "prwxdq2")
setnames(metar, "Type of second present weather in text", "prwxty2")
setnames(metar, "Quality of type of second present weather", "prwxtq2")
setnames(metar, "Intensity of third present weather in text", "prwxin3")
setnames(metar, "Quality of intensity of third present weather", "prwxiq3")
setnames(metar, "Descriptor of third present weather in text", "prwxde3")
setnames(metar, "Quality of descriptor of third present weather", "prwxdq3")
setnames(metar, "Type of third present weather in text", "prwxty3")
setnames(metar, "Quality of type of third present weather", "prwxtq3")
setnames(metar, "Descriptor of first recent weather in text", "rewxde1")
setnames(metar, "Quality of descriptor of first recent weather", "rewxdq1")
setnames(metar, "Type of first recent weather in text", "rewxty1")
setnames(metar, "Quality of type of first recent weather", "rewxtq1")
setnames(metar, "Descriptor of second recent weather in text", "rewxde2")
setnames(metar, "Quality of descriptor of second recent weather", "rewxdq2")
setnames(metar, "Type of second recent weather in text", "rewxty2")
setnames(metar, "Quality of type of second recent weather", "rewxtq2")
setnames(metar, "Descriptor of third recent weather in text", "rewxde3")
setnames(metar, "Quality of descriptor of third recent weather", "rewxdq3")
setnames(metar, "Type of third recent weather in text", "rewxty3")
setnames(metar, "Quality of type of third recent weather", "rewxtq3")
setnames(metar, "AWS present weather in text", "prwxaws")
setnames(metar, "Quality of AWS present weather", "prwxawq")
setnames(metar, "AWS weather for last 15 minutes in text", "awwx15m")
setnames(metar, "Quality of AWS weather for last 15 minutes", "awwx15q")
setnames(metar, "AWS weather for last 60 minutes in text", "awwx60m")
setnames(metar, "Quality of AWS weather for last 60 minutes", "awwx60q")
setnames(metar, "Mean sea level pressure in hPa", "mslphPa")
setnames(metar, "Quality of mean sea level pressure", "mslphPq")
setnames(metar, "Station level pressure in hPa", "stprehP")
setnames(metar, "Quality of station level pressure", "stprehq")
setnames(metar, "QNH pressure in hPa", "QNHphPa")
setnames(metar, "Quality of QNH pressure", "QNHhPaq")
setnames(metar, "AWS Flag", "awsflag")
setnames(metar, "#", "x")

head(metar)

metar$visobsm <- metar$visobsk*1000
metar$windkno <- as.numeric(metar$windkmh)*0.539957

# creates year, month, day columns from date columns
require(stringr)
metar$year_loc <- str_match(as.character(metar$dateloc), pattern = "(\\d*)/(\\d*)/(\\d*)")[, 4]
metar$month_loc <- str_match(as.character(metar$dateloc), pattern = "(\\d*)/(\\d*)/(\\d*)")[, 3]
metar$day_loc <- str_match(as.character(metar$dateloc), pattern = "(\\d*)/(\\d*)/(\\d*)")[, 2]

# creates new date column dateloc as date type 
metar$dateloc <- as.Date(paste(metar$year_loc, "-", metar$month_loc, "-", metar$day_loc, sep = ""), "%Y-%m-%d") 
head(metar$dateloc)

# creates new datetime column dateloc as datetime type 
head(paste(metar$dateloc, " ", metar$timeloc, sep = ""))
metar$datetime_loc <- as.POSIXct(paste(metar$dateloc, " ", metar$timeloc, sep = ""), tz = "Australia/Brisbane", format = "%Y-%m-%d %H:%M")
head(metar$datetime_loc)

require(lubridate)
metar$year_loc <- year(metar$datetime_loc)
metar$month_loc <- month(metar$datetime_loc)
metar$day_loc <- day(metar$datetime_loc)
metar$hour_loc <- hour(metar$datetime_loc)
metar$minute_loc <- minute(metar$datetime_loc)

# creates datetime utc by converting from datetime loc
metar$datetime_utc <- format(metar$datetime_loc, usetz = TRUE, tz = "UTC", format = "%Y-%m-%d %H:%M")
metar$datetime_utc <- as.POSIXct(metar$datetime_utc, tz = "UTC", format = "%Y-%m-%d %H:%M")
head(metar$datetime_utc)

# pulls out year, month, day etc from utc datetime
require(lubridate)
metar$year_utc <- year(metar$datetime_utc)
metar$month_utc <- month(metar$datetime_utc)
metar$day_utc <- day(metar$datetime_utc)
metar$hour_utc <- hour(metar$datetime_utc)
metar$minute_utc <- minute(metar$datetime_utc)

# -------------------------------------------------------------

require(parallel)
metar$datemid <- unlist(mclapply(metar$datetime_loc
  , FUN = function(datetime_loc) datemid_fun(datetime_loc)
  , mc.cores = 3
  )
)

metar$date9am <- unlist(mclapply(metar$datetime_loc
  , FUN = function(datetime_loc) date9am_fun(datetime_loc)
  , mc.cores = 3
  )
)

require(plyr)
rainday_df <- ddply(.data = metar
  , .variables = c("datemid")
  , .fun = summarize
  , rainday = ifelse(sum(preci10) > 0, 1, 0)
  )
head(rainday_df)

head(metar)
head(rainday_df)
metar_norain <- merge(x = metar, y = rainday_df, by = "datemid")
metar_norain <- metar_norain[metar_norain$rainday == 0, ]
head(metar_norain)

# require(plyr)
# lcldday_df <- ddply(.data = metar_norain
#   , .variables = c("datemid")
#   , .fun = summarize

#   , lcldday = ifelse(
#         any(
#           lwcloud_fun(
#             # preci10
#             cldoct1
#             , cldoct2
#             , cldoct3
#             , cldoct4
#             , cldocc1
#             , cldocc2
#             , cldocc3
#             , cldhgt1
#             , cldhgt2
#             , cldhgt3
#             , cldhgt4
#             , cldhtc1
#             , cldhtc2
#             , cldhtc3
#             )
#           )
#         , 1, 0)
#   )
# head(lcldday_df)  

# require(parallel)
# require(foreach)
# metar_norain$lwcloud <- unlist(

#   foreach(i = seq(1, nrow(metar_norain), 1)) %dopar% {

#           ifelse(
#             lwcloud_fun(
#               , cldoct1 = metar_norain[i, ]$cldoct1
#               , cldoct2 = metar_norain[i, ]$cldoct2
#               , cldoct3 = metar_norain[i, ]$cldoct3
#               , cldoct4 = metar_norain[i, ]$cldoct4
#               , cldocc1 = metar_norain[i, ]$cldocc1
#               , cldocc2 = metar_norain[i, ]$cldocc2
#               , cldocc3 = metar_norain[i, ]$cldocc3
#               , cldhgt1 = metar_norain[i, ]$cldhgt1
#               , cldhgt2 = metar_norain[i, ]$cldhgt2
#               , cldhgt3 = metar_norain[i, ]$cldhgt3
#               , cldhgt4 = metar_norain[i, ]$cldhgt4
#               , cldhtc1 = metar_norain[i, ]$cldhtc1
#               , cldhtc2 = metar_norain[i, ]$cldhtc2
#               , cldhtc3 = metar_norain[i, ]$cldhtc3
#               )
#           , 1, 0)
#         }
#         )

require(parallel)
require(foreach)
bknhght <- unlist(

  # foreach(i = seq(1, 5, 1)) %dopar% {
  foreach(i = seq(1, nrow(metar_norain), 1)) %dopar% {

          # ifelse(
            # metar_norain[i, ]$lwcloud == 0, NA, 
            bknhght_fun(
              cldoct1 = metar_norain[i, ]$cldoct1
              , cldoct2 = metar_norain[i, ]$cldoct2
              , cldoct3 = metar_norain[i, ]$cldoct3
              , cldoct4 = metar_norain[i, ]$cldoct4
              , cldocc1 = metar_norain[i, ]$cldocc1
              , cldocc2 = metar_norain[i, ]$cldocc2
              , cldocc3 = metar_norain[i, ]$cldocc3
              , cldhgt1 = metar_norain[i, ]$cldhgt1
              , cldhgt2 = metar_norain[i, ]$cldhgt2
              , cldhgt3 = metar_norain[i, ]$cldhgt3
              , cldhgt4 = metar_norain[i, ]$cldhgt4
              , cldhtc1 = metar_norain[i, ]$cldhtc1
              , cldhtc2 = metar_norain[i, ]$cldhtc2
              , cldhtc3 = metar_norain[i, ]$cldhtc3
              )
            # )
        }
        )
head(bknhght)

setwd("/home/jim/Dropbox/Projects/low_cloud_climatology")
write.csv(metar_norain, "metar_norain.csv")

# -------------------------------------------------------------

setwd("/home/jim/Dropbox/Projects/low_cloud_climatology")
metar_norain <- read.csv("metar_norain.csv")

require(parallel)
metar_norain$lwcloud <- unlist(mclapply(metar_norain$bknhght
  , FUN = function(bknhght) ifelse(bknhght <= 1500, TRUE, FALSE)
  , mc.cores = 3
  )
)
head(metar_norain)

require(plyr)
lcldday_df <- ddply(.data = metar_norain
  , .variables = c("datemid")
  , .fun = summarize
  , lcldday = ifelse(any(lwcloud), TRUE, FALSE)
  )
head(lcldday_df)  

metar_cld <- merge(x = metar_norain, y = lcldday_df, by = "datemid")

metar_cld <- metar_cld[metar_cld$lcldday == TRUE, ]
write.csv(metar_cld, "metar_cld.csv")

# -------------------------------------------------------------

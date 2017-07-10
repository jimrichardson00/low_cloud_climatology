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

# require(parallel)
# require(foreach)
# MiddaytoMiddayday <- foreach(dateloc = metar$dateloc) %dopar% {

#   datetime_lwr = as.POSIXct(paste(dateloc, " ", "12:00:00", sep = ""), format = "%Y-%m-%d %H:%M:%S" , tz = "Australia/Brisbane") 
#   # datetime_lwr 

#   datetime_upr = datetime_lwr + 24*60*60
#   # datetime_upr

#   # as.POSIXct(metar$datetime_loc) >= datetime_lwr
#   # as.POSIXct(metar$datetime_loc) < datetime_upr
#   trues <- (as.POSIXct(metar$datetime_loc) >= datetime_lwr) & (as.POSIXct(metar$datetime_loc) < datetime_upr)
#   # sum(trues)

#   metar_datetime <- metar[trues, ]
#   # metar_datetime[is.na(metar_datetime)] <- NA
#   # metar_datetime <- na.omit(metar_datetime)
#   # head(metar_datetime)
#   metar_datetime

# }

date9am_fun <- function(datetime_loc) {

  dateloc <- as.Date(datetime_loc, tz = "Australia/Brisbane")
  dateloc

  datetime_lwr = as.POSIXct(paste(dateloc, " ", "09:00:00", sep = ""), format = "%Y-%m-%d %H:%M:%S" , tz = "Australia/Brisbane") 
  datetime_lwr 

  datetime_upr = datetime_lwr + 24*60*60
  datetime_upr

  datetime_loc
  dateloc

  datetime_loc >= datetime_lwr & datetime_loc < datetime_upr
  if(datetime_loc >= datetime_lwr & datetime_loc < datetime_upr) {

    return(as.character(dateloc))

    } else {

      return(as.character(dateloc - 1))

    }

  }


datemid_fun <- function(datetime_loc) {

  dateloc <- as.Date(datetime_loc, tz = "Australia/Brisbane")
  dateloc

  datetime_lwr = as.POSIXct(paste(dateloc, " ", "12:00:00", sep = ""), format = "%Y-%m-%d %H:%M:%S" , tz = "Australia/Brisbane") 
  datetime_lwr 

  datetime_upr = datetime_lwr + 24*60*60
  datetime_upr

  datetime_loc
  dateloc

  datetime_loc >= datetime_lwr & datetime_loc < datetime_upr
  if(datetime_loc >= datetime_lwr & datetime_loc < datetime_upr) {

    return(as.character(dateloc))

    } else {

      return(as.character(dateloc - 1))

    }

  }

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
head(metar)
head(metar$datemid)

require(plyr)
rainday_df <- ddply(.data = metar
  , .variables = c("date9am")
  , .fun = summarize
  , rainday = ifelse(sum(prec9am) > 0, 1, 0)
  )
head(rainday_df)

lwcloud_fun <- function(
  preci10
  , cldoct1
  , cldoct2
  , cldoct3
  , cldoct4
  , cldocc1
  , cldocc2
  , cldocc3
  , cldhgt1
  , cldhgt2
  , cldhgt3
  , cldhgt4
  , cldhtc1
  , cldhtc2
  , cldhtc3
  ) {

  ifelse(
    (preci10 == 0) & ((is.na(cldoct1) == FALSE & is.na(cldhgt1) == FALSE) & (cldoct1 >= 5 & cldhgt1 <= 1500)) |
    (preci10 == 0) & ((is.na(cldoct2) == FALSE & is.na(cldhgt2) == FALSE) & (cldoct2 >= 5 & cldhgt2 <= 1500)) |
    (preci10 == 0) & ((is.na(cldoct3) == FALSE & is.na(cldhgt3) == FALSE) & (cldoct3 >= 5 & cldhgt3 <= 1500)) |
    (preci10 == 0) & ((is.na(cldoct4) == FALSE & is.na(cldhgt4) == FALSE) & (cldoct4 >= 5 & cldhgt4 <= 1500)) |
    (preci10 == 0) & ((is.na(cldocc1) == FALSE & is.na(cldhtc1) == FALSE) & (cldocc1 >= 5 & cldhtc1 <= 1500)) |
    (preci10 == 0) & ((is.na(cldocc2) == FALSE & is.na(cldhtc2) == FALSE) & (cldocc2 >= 5 & cldhtc2 <= 1500)) |
    (preci10 == 0) & ((is.na(cldocc3) == FALSE & is.na(cldhtc3) == FALSE) & (cldocc3 >= 5 & cldhtc3 <= 1500))
    , TRUE, FALSE)

}

bknhght_fun <- function(
  cldoct1
  , cldoct2
  , cldoct3
  , cldoct4
  , cldocc1
  , cldocc2
  , cldocc3
  , cldhgt1
  , cldhgt2
  , cldhgt3
  , cldhgt4
  , cldhtc1
  , cldhtc2
  , cldhtc3
  ) {

  min(as.numeric(na.omit(c(
                            cldhgt1[cldoct1 >= 5 & cldhgt1 <= 1500]
                            , cldhgt2[cldoct2 >= 5 & cldhgt2 <= 1500]
                            , cldhgt3[cldoct3 >= 5 & cldhgt3 <= 1500]
                            , cldhgt4[cldoct4 >= 5 & cldhgt4 <= 1500]
                            , cldhtc1[cldocc1 >= 5 & cldhtc1 <= 1500]
                            , cldhtc2[cldocc2 >= 5 & cldhtc2 <= 1500]
                            , cldhtc3[cldocc3 >= 5 & cldhtc3 <= 1500]
                            )
  )
  )
  )
}

require(plyr)
lcldday_df <- ddply(.data = metar
  , .variables = c("datemid")
  , .fun = summarize
  , lcldday = ifelse(
        any(
          lwcloud_fun(
            preci10
            , cldoct1
            , cldoct2
            , cldoct3
            , cldoct4
            , cldocc1
            , cldocc2
            , cldocc3
            , cldhgt1
            , cldhgt2
            , cldhgt3
            , cldhgt4
            , cldhtc1
            , cldhtc2
            , cldhtc3
            )
          )
        , 1, 0)
  )
head(lcldday_df)

# require(parallel)
# require(foreach)
# metar$lwcloud <- unlist(

#   foreach(i = seq(1, nrow(metar), 1)) %dopar% {

#           ifelse(
#             lwcloud_fun(
#               , cldoct1 = metar[i, ]$cldoct1
#               , cldoct2 = metar[i, ]$cldoct2
#               , cldoct3 = metar[i, ]$cldoct3
#               , cldoct4 = metar[i, ]$cldoct4
#               , cldocc1 = metar[i, ]$cldocc1
#               , cldocc2 = metar[i, ]$cldocc2
#               , cldocc3 = metar[i, ]$cldocc3
#               , cldhgt1 = metar[i, ]$cldhgt1
#               , cldhgt2 = metar[i, ]$cldhgt2
#               , cldhgt3 = metar[i, ]$cldhgt3
#               , cldhgt4 = metar[i, ]$cldhgt4
#               , cldhtc1 = metar[i, ]$cldhtc1
#               , cldhtc2 = metar[i, ]$cldhtc2
#               , cldhtc3 = metar[i, ]$cldhtc3
#               )
#           , 1, 0)
#         }
#         )

require(parallel)
require(foreach)
metar$bknhght <- unlist(

  foreach(i = seq(1, nrow(metar), 1)) %dopar% {

          ifelse(
            metar$lwcloud == 0, NA, 
            bknhght_fun(
              cldoct1 = metar[i, ]$cldoct1
              , cldoct2 = metar[i, ]$cldoct2
              , cldoct3 = metar[i, ]$cldoct3
              , cldoct4 = metar[i, ]$cldoct4
              , cldocc1 = metar[i, ]$cldocc1
              , cldocc2 = metar[i, ]$cldocc2
              , cldocc3 = metar[i, ]$cldocc3
              , cldhgt1 = metar[i, ]$cldhgt1
              , cldhgt2 = metar[i, ]$cldhgt2
              , cldhgt3 = metar[i, ]$cldhgt3
              , cldhgt4 = metar[i, ]$cldhgt4
              , cldhtc1 = metar[i, ]$cldhtc1
              , cldhtc2 = metar[i, ]$cldhtc2
              , cldhtc3 = metar[i, ]$cldhtc3
              )
            )
        }
        )


metar_cld <- merge(x = metar, y = lcldday_df)
head(metar_cld)
head(metar)

require(plyr)
dat <- ddply(.data = metar_cld[metar_cld$lcldday == 1, ]
  , .variables = c("datemid") 
  , .fun = summarize
  , cldblw1500_sta_utc = min(datetime_utc[lwcloud == 1])
  , cldblw1500_end_utc = max(datetime_utc[lwcloud == 1])
  , Temp_03pm = tempera[which(hour_loc == 15 & minute_loc == 0)][1]
  , Temp_06pm = tempera[which(hour_loc == 18 & minute_loc == 0)][1]
  , Temp_12am = tempera[which(hour_loc == 0 & minute_loc == 0)][1]
  , Temp_03am = tempera[which(hour_loc == 3 & minute_loc == 0)][1]
  , Temp_06am = tempera[which(hour_loc == 6 & minute_loc == 0)][1]
  , Dwpt_03pm = dewpntt[which(hour_loc == 15 & minute_loc == 0)][1]
  , Dwpt_06pm = dewpntt[which(hour_loc == 18 & minute_loc == 0)][1]
  , Dwpt_12am = dewpntt[which(hour_loc == 0 & minute_loc == 0)][1]
  , Dwpt_03am = dewpntt[which(hour_loc == 3 & minute_loc == 0)][1]
  , Dwpt_06am = dewpntt[which(hour_loc == 6 & minute_loc == 0)][1]
  , cldminutc = datetime_utc[lwcloud == 1][lwcloud == 1 ]
  , vismin_time_utc = datetime_utc[presewx %in% c("Fog patches", "Fog", "Mist")][which.min(as.numeric(na.omit((visobsm[presewx %in% c("Fog patches", "Fog", "Mist")]))))][1]
  , vismin_dwpt = tempera[presewx %in% c("Fog patches", "Fog", "Mist")][which.min(as.numeric(na.omit((visobsm[presewx %in% c("Fog patches", "Fog", "Mist")]))))][1]
  , vismin_temp = dewpntt[presewx %in% c("Fog patches", "Fog", "Mist")][which.min(as.numeric(na.omit((visobsm[presewx %in% c("Fog patches", "Fog", "Mist")]))))][1]
  , fogprwx = mean(fogprwx)
  )
head(dat)

head(data)
dat2 <- merge(x = dat, y = rainday_df, by.x = "datemid", by.y = "date9am", all.x = TRUE)
head(dat2)

head(dat2[dat2$fogprwx == 1, ])
dat2 <- as.data.frame(dat2)
head(dat2)

setwd("/home/jim/Dropbox/Projects/low_cloud_climatology")
write.csv(dat2, "dat2.csv")

dat2$vislessthan1000_sta_utc_time <- substr(x = dat2$vislessthan1000_sta_utc, start = 12, stop = 16)
dat2$vislessthan1000_end_utc_time <- substr(x = dat2$vislessthan1000_end_utc, start = 12, stop = 16)
dat2$vislessthan1000_sta_utc_fog_time <- substr(x = dat2$vislessthan1000_sta_utc_fog, start = 12, stop = 16)
dat2$vislessthan1000_end_utc_fog_time <- substr(x = dat2$vislessthan1000_end_utc_fog, start = 12, stop = 16)
dat2$vislessthan2500_sta_utc_time <- substr(x = dat2$vislessthan2500_sta_utc, start = 12, stop = 16)
dat2$vislessthan2500_end_utc_time <- substr(x = dat2$vislessthan2500_end_utc, start = 12, stop = 16)
dat2$vislessthan2500_sta_utc_fog_time <- substr(x = dat2$vislessthan2500_sta_utc_fog, start = 12, stop = 16)
dat2$vislessthan2500_end_utc_fog_time <- substr(x = dat2$vislessthan2500_end_utc_fog, start = 12, stop = 16)

head(dat2)

setwd("/home/jim/Dropbox/Projects/low_cloud_climatology")
write.csv(dat2, "dat2.csv")

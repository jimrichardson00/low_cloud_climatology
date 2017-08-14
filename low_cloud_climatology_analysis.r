# low_cloud_climatology.r

options(scipen=3)
require(doParallel)
registerDoParallel(cores = 3)

setwd("/home/jim/Dropbox/Projects/low_cloud_climatology")
metar_cld <- read.csv("metar_cld.csv")
metar_cld$datetime_utc <- as.POSIXct(metar_cld$datetime_utc, tz = "UTC", format = "%Y-%m-%d %H:%M")
metar_cld$datetime_loc <- as.POSIXct(metar_cld$datetime_loc, tz = "Australia/Brisbane", format = "%Y-%m-%d %H:%M")

head(metar_cld)
names(metar_cld)
metar_cld[1:100, ]
class(metar_cld$datetime_utc)

# -------------------------------------------------------------

require(plyr)
dat <- ddply(.data = metar_cld
  , .variables = c("datemid") 
  , .fun = summarize
  
  , preci10 = sum(preci10)
  
  , cldblw1500_sta_loc = min(datetime_loc[lwcloud == TRUE])
  , cldblw1500_end_loc = max(datetime_loc[lwcloud == TRUE])

  , bknhght = min(bknhght)
  , bknhght_utc = hour_utc[which.min(bknhght)]

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

  # , cldminutc = datetime_utc[lwcloud == 1][lwcloud == 1 ]
  , vismin_time_utc = datetime_utc[presewx %in% c("Fog patches", "Fog", "Mist")][which.min(as.numeric(na.omit((visobsm[presewx %in% c("Fog patches", "Fog", "Mist")]))))][1]
  , vismin_dwpt = tempera[presewx %in% c("Fog patches", "Fog", "Mist")][which.min(as.numeric(na.omit((visobsm[presewx %in% c("Fog patches", "Fog", "Mist")]))))][1]
  , vismin_temp = dewpntt[presewx %in% c("Fog patches", "Fog", "Mist")][which.min(as.numeric(na.omit((visobsm[presewx %in% c("Fog patches", "Fog", "Mist")]))))][1]
  # , fogprwx = mean(fogprwx)
  )
head(dat)

dat$cldblw1500_sta_loc <- as.POSIXct(dat$cldblw1500_sta_loc , tz = "Australia/Brisbane", format = "%Y-%m-%d %H:%M")
dat$cldblw1500_end_loc <- as.POSIXct(dat$cldblw1500_end_loc , tz = "Australia/Brisbane", format = "%Y-%m-%d %H:%M")

# sta_aft <- dat$cldblw1500_sta_loc > as.POSIXct(paste(dat$datemid, "12:00") , tz = "Australia/Brisbane", format = "%Y-%m-%d %H:%M") + 2*60*60
# end_bef <- dat$cldblw1500_end_loc < as.POSIXct(paste(dat$datemid, "12:00") , tz = "Australia/Brisbane", format = "%Y-%m-%d %H:%M") + 24*60*60 - 2*60*60
# dat <- dat[sta_aft & end_bef, ]

require(lubridate)
dat$month_loc <- month(dat$datemid)

dat$cldblw1500_sta_hr_utc <- as.numeric(substr(x = dat$cldblw1500_sta_utc, start = 12, stop = 13))
dat$cldblw1500_end_hr_utc <- as.numeric(substr(x = dat$cldblw1500_end_utc, start = 12, stop = 13))
head(dat)

setwd("/home/jim/Dropbox/Projects/low_cloud_climatology")
write.csv(dat, "dat.csv")

# ----------------------------------------------------------

setwd("/home/jim/Dropbox/Projects/low_cloud_climatology")
dat <- read.csv("dat.csv")
head(dat)

boxplot(cldblw1500_sta_hr_utc ~ month_loc
  , data = dat
  , main="Car Milage Data"
  , xlab="Number of Cylinders"
  , ylab="Miles Per Gallon")




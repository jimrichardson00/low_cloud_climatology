# low_cloud_climatology.r

options(scipen=3)
require(doParallel)
registerDoParallel(cores = 3)

setwd("/home/jim/Dropbox/Projects/low_cloud_climatology")
metar_cld <- read.csv("metar_cld.csv")
metar_cld$datetime_utc <- as.POSIXct(metar_cld$datetime_utc, tz = "UTC", format = "%Y-%m-%d %H:%M")
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
  , cldblw1500_sta_utc = min(datetime_utc[lwcloud == TRUE])
  , cldblw1500_end_utc = max(datetime_utc[lwcloud == TRUE])
  , bknhght = min(bknhght)
  # , bknhght_utc = hour_utc[which.min(bknhght)]
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
    # , 
  # , cldminutc = datetime_utc[lwcloud == 1][lwcloud == 1 ]
  , vismin_time_utc = datetime_utc[presewx %in% c("Fog patches", "Fog", "Mist")][which.min(as.numeric(na.omit((visobsm[presewx %in% c("Fog patches", "Fog", "Mist")]))))][1]
  , vismin_dwpt = tempera[presewx %in% c("Fog patches", "Fog", "Mist")][which.min(as.numeric(na.omit((visobsm[presewx %in% c("Fog patches", "Fog", "Mist")]))))][1]
  , vismin_temp = dewpntt[presewx %in% c("Fog patches", "Fog", "Mist")][which.min(as.numeric(na.omit((visobsm[presewx %in% c("Fog patches", "Fog", "Mist")]))))][1]
  # , fogprwx = mean(fogprwx)
  )
head(dat)


setwd("/home/jim/Dropbox/Projects/low_cloud_climatology")
write.csv(dat, "dat.csv")

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

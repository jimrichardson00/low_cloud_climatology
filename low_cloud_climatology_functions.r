# low_cloud_climatology_functions.r

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
  
lwcloud_fun <- function(
  # preci10
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

  ifelse(
    ((is.na(cldoct1) == FALSE & is.na(cldhgt1) == FALSE) & (cldoct1 >= 5 & cldhgt1 <= 1500)) |
    ((is.na(cldoct2) == FALSE & is.na(cldhgt2) == FALSE) & (cldoct2 >= 5 & cldhgt2 <= 1500)) |
    ((is.na(cldoct3) == FALSE & is.na(cldhgt3) == FALSE) & (cldoct3 >= 5 & cldhgt3 <= 1500)) |
    ((is.na(cldoct4) == FALSE & is.na(cldhgt4) == FALSE) & (cldoct4 >= 5 & cldhgt4 <= 1500)) |
    ((is.na(cldocc1) == FALSE & is.na(cldhtc1) == FALSE) & (cldocc1 >= 5 & cldhtc1 <= 1500)) |
    ((is.na(cldocc2) == FALSE & is.na(cldhtc2) == FALSE) & (cldocc2 >= 5 & cldhtc2 <= 1500)) |
    ((is.na(cldocc3) == FALSE & is.na(cldhtc3) == FALSE) & (cldocc3 >= 5 & cldhtc3 <= 1500))
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

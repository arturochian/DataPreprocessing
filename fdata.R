library(plyr)
library(dplyr)
library(data.table)
library(pipeR)
library(doParallel)
library(bit64)
library(stringr)
.to.sec <- function(ts) {
  floor(ts/1e4) * 3600 + floor((ts %% 1e4)/1e2) * 60 + ts %% 1e2
}
.total.sec <- .to.sec(235959)
.to.sec.night <- function(ts) {
  .to.sec(ts) - .total.sec - 1
}
secs <- list(
  .morning1.start = .to.sec(090000),
  .morning1.end = .to.sec(101500),
  .morning2.start = .to.sec(103000),
  .morning2.end = .to.sec(113000),
  .afternoon.start = .to.sec(133000),
  .afternoon.end = .to.sec(150000),
  .cfx.morning.start = .to.sec(091500),
  .cfx.morning.end = .to.sec(113000),
  .cfx.afternoon.start = .to.sec(130000),
  .cfx.afternoon.end = .to.sec(151500),
  .night.start = .to.sec.night(210000),
  .night1.end = .to.sec(023000),
  .night2.end = .to.sec(010000))

common_ts_filter <- function(ts) {
  (ts >= secs$.morning1.start & ts <= secs$.morning1.end) |
    (ts >= secs$.morning2.start & ts <= secs$.morning2.end) |
    (ts >= secs$.afternoon.start & ts <= secs$.afternoon.end)
}

cfx_ts_filter <- function(ts) {
  (ts >= secs$.cfx.morning.start & ts <= secs$.cfx.morning.end) |
    (ts >= secs$.cfx.afternoon.start & ts <= secs$.cfx.afternoon.end)
}

night1_ts_filter  <- function(ts) {
  (ts >= secs$.night.start & ts <= secs$.night1.end) |
    common_ts_filter(ts)
}

night2_ts_filter  <- function(ts) {
  (ts >= secs$.night.start & ts <= secs$.night2.end) |
    common_ts_filter(ts)
}

args <- commandArgs(trailingOnly = TRUE)
path <- args[1]
ts_filter <- get(paste0(args[2],"_ts_filter"))
#path <- "D:/Data/20140213/"

files <- list.files(path = path,pattern = "[A-Za-z]+\\d{2}_\\d{8}\\.csv",full.names = T,recursive = T,ignore.case = T)
#file <- list.files(path = path,pattern = "ag06_20140213\\.csv",full.names = T,recursive = T,ignore.case = T)
system.time(result <- files %>>%
    ldply(ts_filter = ts_filter,
      .fun = function(file,ts_filter){
        instrument <- tolower(str_split(rev(str_split(file,pattern = "[/\\.]")[[1]])[[2]],pattern = "_")[[1]][[1]])
        col_class = c("character","character","numeric","numeric","numeric",
          "numeric","numeric","numeric","numeric","numeric","numeric","numeric")
        try_default(quiet = T,
          default = data.frame(file = file, done = F),
          expr = {
            dt <- fread(file,header = F,stringsAsFactors = F,skip = 1,showProgress = F,colClasses = col_class)
            if(!empty(dt)) {
              dt <- dt %>>% 
                mutate(
                  InstrumentID = instrument,
                  V1 = as.integer(str_replace_all(V1,pattern = "(\\d{4})-(\\d{2})-(\\d{2})",replacement = "\\1\\2\\3")),
                  TS = as.integer(str_replace_all(V2,pattern = "(\\d{2}):(\\d{2}):(\\d{2})",replacement = "\\1\\2\\3"))) %>>%
                mutate(TS = ifelse(TS >= 200000,.to.sec.night(TS),.to.sec(TS))) %>>%
                mutate(TS_temp = TS) %>>%
                group_by(TS) %>>%
                mutate(TS_temp = TS_temp + c(0,0.5)[1:length(TS_temp)]) %>>%
                ungroup() %>>%
                mutate(TS = TS_temp,TS_temp = NULL) %>>%
                arrange(TS) %>>%
                filter(ts_filter(TS)) %>>%
                select(InstrumentID,V1,V2,TS,V3:V12)
              #             setnames(dt,c("InstrumentID","YMD","HMS","TS",
              #               "LastPrice","TradeDiff","Volume","OpenInterestDiff","OpenInterest",
              #               "Buy1Price","Buy1Size","Sell1Price","Sell1Size","AvgPrice"))
              write.table(dt,file=file,sep=",",row.names = F,col.names=F)
              message(file)
              data.frame(file = file, done = T)
            }  
          })
      }))

if(all(result$done)) {
  message("All done.")
} else {
  message("Some failed.")
  write.csv(result %>>% filter(!done),
    file=paste0("d:/data/fdata/log-",as.character(Sys.time(),format="%Y%m%d-%H%M%S"),".csv"),
    row.names=F)
}
  
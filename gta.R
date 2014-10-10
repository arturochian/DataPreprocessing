library(plyr)
library(dplyr)
library(pipeR)
library(data.table)
library(doParallel)
library(stringr)

.to.sec = function(x,baseline = 9e7) {
  x <- x - baseline
  floor(x/1e7)*3600 + 
    floor((x %% 1e7)/1e5)*60 + (x %% 1e5)/1e3
}

cl <- makeCluster(4)
registerDoParallel(cl)

data0 <- list.files("D:/Data/FTdata/GTA/AU/",pattern = ".+\\.txt",full.names = T) %>>%
  ldply(.parallel = T,function(file) {
    read.table(file,header = T,sep = "\t",stringsAsFactors = F)
  }) %>>%
  mutate(ContractID = str_trim(ContractID),
    YMD = as.integer(str_replace_all(Tdatetime,
    pattern = "(\\d{4})-(\\d{2})-(\\d{2}) (\\d{2}):(\\d{2}):(\\d{2})\\.(\\d{3})",
    replacement = "\\1\\2\\3")),
    HMS = as.integer(str_replace_all(Tdatetime,
      pattern = "(\\d{4})-(\\d{2})-(\\d{2}) (\\d{2}):(\\d{2}):(\\d{2})\\.(\\d{3})",
      replacement = "\\4\\5\\6\\7"))) %>>%
  mutate(TS = as.integer(.to.sec(HMS) / 60)) %>>%
  select(InstrumentID = ContractID,
    YMD,HMS,TS,
    Open=OpenPx,High=HighPx,Low=LowPx,Close=LastPx,
    Turnover,OpenInterest=OpenInts,MinQty) %>>%
  as.data.table
stopCluster(cl)

setkey(data0,InstrumentID,YMD,HMS)

daily <- data0[,list(
  Open = first(Open),
  High = max(High),
  Low = min(Low),
  Close = last(Close),
  Turnover = sum(Turnover),
  OpenInterest = last(OpenInterest),
  MinQty = sum(MinQty)
  ),by=c("InstrumentID","YMD")] %>>%
  filter(Turnover > 0) %>>%
  group_by(YMD) %>>%
  mutate(rank=row_number(desc(OpenInterest))) %>>%
  ungroup()

daily_zl <- daily %>>%
  filter(rank == 1) %>>%
  select(InstrumentID, YMD)
setkey(daily_zl,InstrumentID, YMD)

daily_czl <- daily %>>%
  filter(rank == 2) %>>%
  select(InstrumentID, YMD)
setkey(daily_czl,InstrumentID, YMD)

tick_zl <- data0[daily_zl]
setkey(tick_zl,YMD,HMS)
tick_czl <- data0[daily_czl]
setkey(tick_czl,YMD,HMS)

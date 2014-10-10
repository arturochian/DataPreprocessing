library(plyr)
library(dplyr)
library(data.table)
library(pipeR)
library(doParallel)
library(bit64)
library(stringr)
library(rlist)
library(reshape2)

args <- commandArgs(trailingOnly = TRUE)
# rm(list = ls())
# dev.off()
# args <- c("i","d:/data/fdata/i/","r",10,FALSE)
cls <- args[1]
path <- args[2]
style <- tolower(args[3]) # f for fixed, r for relative
dd <- as.integer(args[4]) # e.g. f 15, r 10
xyid <- as.logical(args[5]) # true of false
if(!style %in% c("f","r")) {
  failwith("Invalid style argument.")
}

#path <- "d:/data/fdata/m/"

cl <- makeCluster(6)
registerDoParallel(cl)
invisible(clusterEvalQ(cl, {
  library(data.table)
}))

system.time(data0 <- list.files(path = path,pattern = "[A-Za-z]+\\d+_\\d{8}\\.csv",
  full.names = T,recursive = T,ignore.case = T) %>>%
    llply(.parallel = T,function(file) {
      try_default(default = NULL,quiet = T,
        expr = {
          col_class = c("character","integer","character","numeric",
            "numeric","numeric","numeric","numeric","numeric",
            "numeric","numeric","numeric","numeric","numeric")
          if(file.info(file)$size > 0) {
            fread(file,header = F,stringsAsFactors = F,colClasses = col_class,showProgress = F)
          } else {
            NULL
          }  
        })
    }) %>>%
    list.exclude(is.null(.)) %>>%
    rbindlist)
stopCluster(cl)

setnames(data0,c("InstrumentID","YMD","HMS","SEC",
  "LastPrice","Diff","Volume","OIDiff","OI",
  "Bid1Price","Bid1Size","Ask1Price","Ask1Size","AvgPrice"))
setkey(data0,InstrumentID,YMD,SEC)

indx <- fread("d:/data/fdata/indx.csv",header=T)
indx[,c("Y_indx","M_indx","D_indx") :=
    list(floor(YMD/1e4),floor(YMD %% 1e4/1e2),floor(YMD %% 1e2))]
setkey(indx,YMD)
# daily <- data0[,list(
#   Open = first(LastPrice),
#   High = max(LastPrice),
#   Low = min(LastPrice),
#   Close = last(LastPrice),
#   Volume = last(Volume),
#   OI = last(OI),
#   AvgPrice = last(AvgPrice)
# ),by=c("InstrumentID","YMD")] %>>%
#   filter(Volume > 0) %>>%
#   group_by(YMD) %>>%
#   mutate(rank=row_number(desc(OI))) %>>%
#   ungroup()

daily <- data0[,list(
  Open = first(LastPrice),
  High = max(LastPrice),
  Low = min(LastPrice),
  Close = last(LastPrice),
  Volume = last(Volume),
  OI = last(OI),
  AvgPrice = last(AvgPrice)
),by=c("InstrumentID","YMD")]

indx_pruned <- indx %>>%
  filter(YMD >= min(daily$YMD), YMD <= max(daily$YMD))

###
vol <- dcast.data.table(daily,formula = YMD~InstrumentID,
  value.var = "Volume",drop = F,fill = 0)[indx_pruned[,list(YMD)]]
oi <- dcast.data.table(daily,formula = YMD~InstrumentID,
  value.var = "OI",drop = F,fill = NA_real_)[indx_pruned[,list(YMD)]]
oi <- zoo::na.locf(oi,na.rm = F)
vol <- melt(vol,id.vars="YMD",value.name = "Volume",variable.name="InstrumentID")
oi <- melt(oi,id.vars="YMD",value.name = "OI",variable.name="InstrumentID")

daily <- select(daily,-Volume,-OI)[merge(vol,oi,by = c("InstrumentID","YMD"))]
rm(vol,oi)
###

daily[,c("Y_","M_","D_") :=
    list(floor(YMD/1e4),floor(YMD %% 1e4/1e2),floor(YMD %% 1e2))]
daily[,Remote := (xyid) & (tolower(str_replace_all(InstrumentID,
  pattern = "([^XxYy]+)([XxYy])(\\d{2})",replacement = "\\2"))=="y")]
daily[,MatMonth := as.integer(str_replace_all(InstrumentID,
  pattern = "([A-Za-z]+)(\\d{2})",replacement = "\\2"))]

invisible(if(style == "f") { # expiry on fixed day
  daily[,LastTradingDay := c(0,diff(!Remote & M_ == MatMonth & D_ >= dd))==1,by = "InstrumentID"]
  daily[,InitOffset := YMD[1] > filter(indx,Y_indx == Y_[1], M_indx == MatMonth[1], D_indx >= dd)$YMD[1],by = "InstrumentID"]
  
} else if(style == "r") { # expiry on k-th trading day
  daily[,LastTradingDay := !Remote & M_ == MatMonth & (1:.N == dd),by=c("InstrumentID","Y_","M_")]  
  daily[,InitOffset := YMD[1] > filter(indx,Y_indx == Y_[1], M_indx == MatMonth[1])$YMD[dd],by = "InstrumentID"]
})

daily[,MatYear := as.integer(Y_[1] + c(0,head(cumsum(LastTradingDay),-1)) + InitOffset),by = "InstrumentID"]
daily[,ContractID := paste0(cls,sprintf("%.2d",MatYear%%1e2),sprintf("%.2d",MatMonth))]

daily <- daily[,list(
  InstrumentID = InstrumentID,
  ContractID = ContractID,
  VolumeRatio = Volume/max(Volume,na.rm = T),
  OIRatio = OI/max(OI,na.rm = T),
  Inferior = is.na(Volume) | is.na(OI) | (Volume/max(Volume,na.rm = T) < 0.5 & OI/max(OI,na.rm = T) < 0.5),
  LastTradingDay = LastTradingDay
),by="YMD"]
setkey(daily,YMD)

.empty <- function(dt) {
  empty(dt) || all(is.na(dt))
}

daily_zl <- data.table(YMD = unique(daily$YMD),
  InstrumentID = NA_character_)
nn <- nrow(daily_zl)
current_zl <- daily[J(daily_zl$YMD[1])][OIRatio==1.0]$InstrumentID[1]
message(daily_zl$YMD[1]," ",current_zl)
set(daily_zl,1L,2L,current_zl)
for(i in 1:(nn-1)) {
  #message(i)
  dt <- daily[J(daily_zl$YMD[i])]
  dt_day <- daily[J(daily_zl$YMD[i])][InstrumentID == current_zl]
  dt2_day <- daily[J(daily_zl$YMD[i+2])][InstrumentID == current_zl]
  if(.empty(dt_day) ||
      (!.empty(dt_day) && dt_day$Inferior) ||
      (!.empty(dt2_day) && dt2_day$LastTradingDay)) {
    current_zl <- dt[OIRatio==1.0]$InstrumentID[1]
    message(daily_zl$YMD[i+1]," ",current_zl)
  }  
  set(daily_zl,as.integer(i+1),2L,current_zl)
}

inst_factor <- factor(daily_zl$InstrumentID)
png(filename = paste0("d:/data/fdata/RData_new/",cls,"-zl.png"),width = 800,height = 600)
plot(as.numeric(inst_factor),type="s",main=cls)
dev.off()
#set(daily_zl,230L,2L,"al02")
setkey(daily_zl,InstrumentID, YMD)
setkey(daily,InstrumentID,YMD)
daily <- daily[daily_zl,list(InstrumentID,ContractID,YMD)]

tick_zl <- data0[daily][complete.cases(LastPrice) & Volume > 0]
setkey(tick_zl,YMD,SEC)

write.csv(arrange(daily,YMD),file=paste0("d:/data/fdata/RData_new/",cls,"-zl.csv"),row.names=F)
save(tick_zl,daily_zl,file = paste0("d:/data/fdata/RData_new/",cls,"-zl.RData"),compress = T)

library(plyr)
library(dplyr)
library(data.table)
library(pipeR)
library(doParallel)
library(bit64)
library(stringr)
library(R.matlab)

rdata_files <- list.files("d:/data/fdata/rdata_new/",pattern = ".+\\.RData$",full.names = T)

l_ply(rdata_files,function(file) {
  #file <- rdata_files[9]
  filename <- first(str_split(last(str_split(file,"/")[[1]]),"\\.")[[1]])
  load(file = file)
  daily_zl[,InstrumentID := as.integer(str_replace_all(InstrumentID,"([A-Za-z]+)(\\d+)","\\2"))]
  tick_zl[,InstrumentID := as.integer(str_replace_all(InstrumentID,"([A-Za-z]+)(\\d+)","\\2"))]
  tick_zl[,ContractID := as.integer(str_replace_all(ContractID,"([A-Za-z]+)(\\d+)","\\2"))]
  tick_zl[,HMS := as.integer(str_replace_all(HMS,"(\\d{2}):(\\d{2}):(\\d{2})","\\1\\2\\3"))]
  
  con_mat <- gzfile(paste0("d:/data/fdata/mat/",filename,".mat.gz"),"wb")
  writeMat(con_mat,daily_zl=daily_zl,tick_zl=tick_zl)
  close(con_mat)
  
  con_csv <- gzfile(paste0("d:/data/fdata/csv/",filename,".csv.gz"),"wb")
  write.csv(tick_zl,file=con_csv,row.names=F)
  close(con_csv)
})
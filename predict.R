#library(readr)
#redfin <- read_csv("data/redfin_2020-06-07-06-20-21.csv")
mods <- readRDS("results/mods.rds")
county.mods <- readRDS("results/county_mods.rds")
MODEL <- readRDS("results/final_model.rds")
sdat_predict <- function(df=read.csv("data/redfin_2020-06-07-06-20-21.csv"),
                         addr=c("ADDRESS"), 
                         list.price = c("PRICE"),
                         get_meta=T){
  df <- as.data.frame(df)
  if(get_meta){
    df.sdat <- sdat_get_meta(data.frame(address = df[,addr], List.Price = df[,list.price])) %>%
    sdat_add_features()
  }else{
    df.sdat <- df
    df.sdat$List.Price <- df[,list.price]
  }
  
  try({
    df.sdat$price.05.est <- predict(county.mods$lm.05, df.sdat)
    df.sdat$price.50.est <- predict(county.mods$lm.50, df.sdat)
    df.sdat$price.95.est <- exp(predict(county.mods$lm.95.log, df.sdat))
  })
  
  df.sdat$Estimate.RF <- sapply(1:nrow(df.sdat), function(i){
    tryCatch(
      predict(MODEL,df.sdat[i,]) %>% as.numeric(),
      error=function(cond){return(NA)},
      silent=T
        )
  })
  df.sdat$Estimate.ENET <- sapply(1:nrow(df.sdat), function(i){
    tryCatch(
      predict(mods$enet,df.sdat[i,]) %>% as.numeric(),
      error=function(cond){return(NA)},
      silent=T
    )
  })
  
  df.sdat <- df.sdat %>% dplyr::select(address,List.Price, Estimate.RF, Estimate.ENET,price.50.est,price.05.est, price.95.est, everything())
  return(df.sdat) 
  
}

sdat_prepare_predict <- function(df.sdat){

  df.sdat$isBank <- F
  df.sdat$isCompany <-F
  df.sdat$flip <- F
  df.sdat$isNewConstr <- F
  df.sdat$yearSold <- format(Sys.Date(), "%Y") %>% as.numeric()
  df.sdat$monthSold <- format(Sys.Date(), "%m") %>% as.numeric()
  df.sdat$monthSold <- as.factor(df.sdat$monthSold)
  df.sdat$age <- df.sdat$yearSold  - df.sdat$year_built 
  
  return(df.sdat)
}

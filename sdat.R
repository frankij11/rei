library(httr, quietly = TRUE, verbose=F)
#library(RSocrata)
suppressMessages( library(tidyverse, quietly = TRUE, verbose=F))
library(lubridate, quietly = TRUE, verbose=F)
#slibrary(stringr, quietly = TRUE)
library(caret, quietly = TRUE, verbose=F) 

library(geosphere)

api_endpoint = "https://opendata.maryland.gov/resource/ed4q-f8tm.json?"
api_csv = "https://opendata.maryland.gov/resource/ed4q-f8tm.csv?"

url_md = "https://opendata.maryland.gov/"
app_id = "fuixaI8OmedLW38WcI00DnoCn"

# get zipcode
zips <- read.csv("data/superzip.csv") %>%
  filter(state=="MD") %>%
  mutate(college =  gsub("%", "", college) %>% as.numeric()) %>%
  select(zipcode, centile, superzip, rank,adultpop, households,college,income)



select_vars <- function(vars=read.csv("data/vars.csv")){
  vars <- vars %>% filter(short_name!="") %>% mutate(sel = paste0(api_field, " as ", short_name)) %>% select(sel)
  statement <- paste("SELECT", vars$sel[1])
  for(i in 2:nrow(vars)){
    statement <- paste0(statement, ", ", vars$sel[i], " " )    
  }
  return(statement)
}

where_comps = function(lat=38.8159, lon=-76.7497, miles = 1, year = 2019){ 
  miles = miles/0.000621371 #convert to meters
  w = sprintf(" WHERE 
        
        sales_segment_1_transfer_date_yyyy_mm_dd_mdp_field_tradate_sdat_field_89 >= %s  AND 
        sales_segment_1_consideration_mdp_field_considr1_sdat_field_90 > 10000 AND 
        within_circle(mappable_latitude_and_longitude, %f, %f, %f)", paste0("'", year, "%'"),lat, lon, miles)
  w = gsub("[\r\n]", "",w)
  w = gsub("  ", "",w)
  return(w)
  # (sales_segment_1_consideration_mdp_field_considr1_sdat_field_90 - sales_segment_2_consideration_sdat_field_110) > 50000 AND 
  # sales_segment_2_consideration_sdat_field_110 > 0 AND 

}

clean_addresses <- function(addresses){
  props_str = toupper(addresses)
  props_str = str_replace_all(props_str,"STREET", "ST")
  props_str = str_replace_all(props_str,"AVENUE", "AVE")
  props_str = str_replace_all(props_str,"DRIVE", "DR")
  props_str = str_replace_all(props_str,"LN", "LANE")
  props_str = str_replace_all(props_str,"TERRACE", "TER")
  props_str = str_replace_all(props_str,"COURT", "CT")
  props_str = str_replace_all(props_str," RD", " ROAD")
  props_str = str_replace_all(props_str,"TRAIL", "TRL")
  props_str = str_replace_all(props_str,"PLACE", "PL")
  props_str = str_replace_all(props_str,"BOULEVARD", "BLVD")
  
  return(props_str)
}

where_meta = function(props){
  props = paste0("'", sapply(props, trimws), "'")
  props_str = paste0(props, collapse = ",")
  props_str = clean_addresses(props_str)
  
  
  
  
  w = sprintf(" WHERE mdp_street_address_mdp_field_address in (%s) ", props_str)
  w = gsub("[\r\n]", "",w)
  w = gsub("  ", "",w)
  return(w)
}

sdat_query = function(api=api_csv, select=select_vars(), where,limit = 50000,return_page=0){
  query = paste0("$query= ", select, where, " LIMIT ", limit)
  full_url = paste0(api, query)
  full_url = URLencode(full_url)
  page = GET(full_url)
  if(return_page>0){return(page)}
  if(status_code(page)==200){
    frame = suppressMessages(content(page))
    
    #clean data
    try({

    })
    return(frame)
  }else{
    return(NULL)
  }
  
}

sdat_q=function(api=api_csv, q){
  query= paste0("$q=", q)
  full_url = paste0(api, query)
  full_url = URLencode(full_url)
  page = GET(full_url)
  if(status_code(page)==200){
    frame = content(page)
    return(frame)
  }else{
    return(NULL)
  }
  
}
sdat_get_meta <- function(df, col = "address"){
  results <- NULL
  for(i in 1:nrow(df)){
    tmp_result <- NULL
    tmp_result <- sdat_query(where=where_meta(df[,col][i]))
    if(!is.null(tmp_result)){
      #tmp_result$search_address <- df[,col][i] 
      if(is.null(results)){
        results <- tmp_result
      }else{
        results <- rbind(results, tmp_result)
    }
  }
  }
  df$search_address <- clean_addresses(df[,col])
  df <- left_join(df, results, by=c("search_address" = "address"))
  return(df)
}


# remove rows with missing address / zipcode
sdat_filter <- function(df, land =c("Residential (R)","Town House (TH)" ),
                        low_sqft=500, 
                        high_sqft=6000, 
                        excl.style =" TH |CONDO"){
  df <- df %>% filter(!is.na(address),
                      land_use %in% land,  
                      #!grepl(excl.style, style, ignore.case = T) ,
                      sqft >low_sqft, sqft < high_sqft,
                      price < 5000000)
  return(df)
}

sdat_add_features <- function(df, dc =list(lat=38.9072,lon= -77.036)){
  #Add zipcode information
  try({
    df <- df %>% left_join(zips, on="zipcode", how="left")
    
    df$zipcode <- factor(df$zipcode)
    
    df$basement <- grepl("with basement", tolower(df$style))
    df$stories <- sub(".*STRY *(.*?) *Story.*", "\\1", df$style)
    
    df$year_built <- df$year_built %>% as.numeric() 
    df$age <- year(ymd(df$sale_date)) - df$year_built 
    
    df$acre <- df$land_area_unit
    df$acre <- gsub("A", 1, df$acre)
    df$acre <- gsub("S", 43560, df$acre)
    df$acre <- df$land_area / as.numeric(df$acre)
    
        
    df$price_delta <- (df$price - df$price2)
    df$date_delta <- (ymd(df$sale_date) - ymd(df$sale_date2))/ 365
    df$date_delta <- as.numeric(df$date_delta)
    df$flip <- df$price_delta > 50000 & df$date_delta <=1 & df$price2 >10000
    
    
    df$factor_commercial <- ifelse(grepl("Commercial", df$factor_commercial, ignore.case = T), T,F)

    df$isMaterialLow <- F
    df$isMaterialLow[grep("block|Asbestos Shingle", df$material, ignore.case = T)] <- T
    
    df$isMaterialBrick <- F
    df$isMaterialBrick[grep("brick", df$material, ignore.case = T)] <- T
    
    df$isMaterialFrame <- F
    df$isMaterialFrame[grep("frame", df$material, ignore.case = T)] <- T
    
    df$isMaterialSiding <- F
    df$isMaterialSiding[grep("siding", df$material, ignore.case = T)] <- T
    
    df$isMaterialHigh <- F
    df$isMaterialHigh[grep("stone|Wood Shingle", df$material, ignore.case = T)] <- T
    
    df$isMaterialOther <- F
    df$isMaterialOther[-grep("Brick|frame|stone|block|Asbestos Shingle", df$material, ignore.case = T)] <- T
    
    # Add Features
    df$yearSold <- substr(df$sale_date, 1, 4) %>% as.numeric()
    df$monthSold <- substr(df$sale_date, 6, 7) %>% as.numeric()
    df$monthSold <- as.factor(df$monthSold)
    
    
    df$isCompany <- F
    df$isCompany[grep("INC|LLC|BUILDER", df$seller, ignore.case = T)] <- T
    
    df$isBank <- F
    df$isBank[grep("BANK|MORTGAGE|MORTG|SAVING|SECRETARY", df$seller, ignore.case = T)] <- T
    
    df$isNewConstr <- F
    df$isNewConstr[df$yearSold == df$year_built-1 |df$yearSold == df$year_built | df$yearSold == df$year_built +1 ] <-T
    
    
    df$dist_DC <- as.vector(distm(x=df[,c("lon", "lat")] %>% as.matrix(), y = c(dc$lon, dc$lat)) ) * 0.000621371
    
    #df$quality_county <- .8
    df$quality_zipcode <- .8
    
  })  
  return(df)
}

sdat_drop_cols <- function(df){
  cols.char <- df[,sapply(df, is.character)] %>% names()
  cols.char.unique <- sapply(df[,cols.char], unique) %>% sapply( length)
  drop.cols <- cols.char.unique[cols.char.unique ==1 | cols.char.unique >15] %>% names()
  #keep.cols <- names(df)[!names(df) %in% drop.cols]
  print(drop.cols)
  drop.cols2 <- c("lat", "lon", "price2","material", "tax_land_value","tax_improvement",
                  "tax_preferential_land_value","tax_assessment", "style", "adultpop",
                  "price_delta", "date_delta", "land_area_unit", "land_area")
  drop.cols <- c(drop.cols, drop.cols2) %>% unique()
  #print(drop.cols)
  #df <- df %>% select(.dots = -drop.cols, -price_delta,-date_delta, -land_area_unit)
  for(i in 1:length(drop.cols)){
    try({
      #print(paste("Dropping:", drop.cols[i]))
      df <- df %>% select(.dots = -drop.cols[i])
      
      })
  }
  return(df)
}


sdat_get_many_properties = function(addresses){
  n = length(addresses)
  result = NULL
  beg = 0
  end = 0
  while(end<n){
    beg= beg+1
    end = min(beg +9 , n)
    print(paste("beginning", beg))
    print(paste("END", end))
    tmp_result = sdat_query(where=where_meta(addresses[beg:end]))
    if(is.null(result)){
      result = tmp_result
    }else{
      result = rbind(result, tmp_result)
    }
    tmp_result = NULL   
    beg = end
  }
  return(result)
}

model.lm <- function(df, reno = T, dist = 1){
  if(reno){df <- filter(df, renovated==T)}
  
  lm(price~living_area + stories + basement , data = df)
}

model.rf <- function(df){
  #control <- trainControl(method='boot', 
  #                        number=10, 
  #                       repeats=3)
  train(price~living_area + tax_assessment + owner_type + stories + renovated + year_built, data =df, method="rf")
}

#   if(values()$all_comps %>% !is.null()){
sdat_models <- function(meta = NULL, comps = NULL, address = NULL){

if(is.null(meta)){
  meta = sdat_query(where=where_meta(address))
  if(nrow(meta<1)){return(NULL)}
}
comps <- filter(comps, land_use =="Residential (R)" | land_use =="Town House (TH)" , price <1500000 )
  
comps_reno <- filter(comps, renovated ==T)
mods <- list()
try({
mods$avg <- lm(price~basement, data = filter(comps, living_area > meta$living_area[[1]] -250,living_area > meta$living_area[[1]] +250) )
mods$all <- lm(price~living_area:basement:renovated, data=comps)
#mods$slr <- lm(price~living_area, data = comps_reno)
mods$lm <- lm(price~living_area:basement, data = comps_reno)
mods$lm2 <- lm(price~living_area:basement:stories, data = comps_reno)
mods$lm3 <- lm(price~living_area:basement:stories:land_use:renovated, data = comps)
mods$lm4 <- lm(price~living_area:basement:stories:neighborhood:land_use:renovated, data = comps)
#mods$rf <- train(price~living_area + basement + stories + renovated +land_use + tax_assessment, 
#                 data = comps,
#                 method="rf",
#                 ntree=50, 
#                 control = trainControl(number=1))

})
meta$renovated == T
#mods$predict <- predict(mods, meta) %>% data.frame()
return(mods)
}

sdat_predict <- function(models, meta, reno=T){
  meta$renovated <- reno
  df <- data.frame(Model = sapply(1:length(models), function(i) models[i] %>% names() %>% toupper())
                   #,Equation = sapply(1:length(models), function(i) models[[i]]$call)
                   )
  df$Estimate <- sapply(1:length(models), 
                        function(i) {
                          r <- NULL
                          try({r <- predict(models[[i]], meta)[[1]]})
                          return(r)
                        })
  
                  
  return(df)
}

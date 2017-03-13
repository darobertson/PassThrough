#####################################################################################################
#
# Estimate the Nested Logit Demand Model with Market Defined as Stores
# Xiliang Lin
# Febuary, 2016
#
#####################################################################################################
# Settings
rm(list = ls())

# Testing status
trial_run = FALSE
if (trial_run){
  module_list = 1393  # For testing
} else{
  module_list = c(1393, 1340) # For running
}

# Top brands to be included
top_cut = 5

# Cumulative store share to be included
store_cum_cut = 0.85

# Reference Price Setting
delta = 0.1
ref0 = 0.1

# Market Level
# Elascity is homogeneous within a market
mkt_level = c("dma_code", "parent_code")
mkt_s_level = c("store_code_uc") # level to compute market share
t_level = c("week_end") # time level to compute market share

# Market Size Definition as Penetration rate at peak demand 
prate = 0.75

# Number of stores to keep
nstore_cut = 3

# Observation cut per market
nobs_cut = 500

# Regression formula
# form = formula(lshare ~ factor(brand_descr_corrected) + labove_ref + lbelow_ref + promotion + lprice + Christmas
#                  +Thanksgiving+Easter+lcpi+lunemployment+poly(tm, 5)+factor(store_code_uc)+lcond_share)
form = formula(lshare ~ factor(brand_descr_corrected) + promotion + lprice + Christmas
                 +Thanksgiving+Easter+lcpi+lunemployment+poly(tm, 5)+factor(store_code_uc)+lcond_share)

# IV Regression formula
#IVform = formula(lshare ~ factor(brand_descr_corrected) + labove_ref + lbelow_ref + promotion + lprice + Christmas+
#                   Thanksgiving+Easter+lcpi+lunemployment+poly(tm, 5)+factor(store_code_uc)+lcond_share|
#                   factor(brand_descr_corrected) + IV_above_ref + IV_below_ref + promotion + lhausman + Christmas+
#                   Thanksgiving+Easter+lcpi+lunemployment+poly(tm, 5)+factor(store_code_uc)+lcond_share)
IVform = formula(lshare ~ factor(brand_descr_corrected) + promotion + lprice + Christmas+
                  Thanksgiving+Easter+lcpi+lunemployment+poly(tm, 5)+factor(store_code_uc)+lcond_share|
                  factor(brand_descr_corrected) + promotion + lhausman + Christmas+
                  Thanksgiving+Easter+lcpi+lunemployment+poly(tm, 5)+factor(store_code_uc)+lcond_share)

# Load Necessary Packages
library(parallel)
library(timeDate)
library(chron)
library(data.table)
setNumericRounding(0)
library(bit64)
library(AER)

# Set Working Folder Path Here
setwd("~/PassThrough")
meta_dir  = "Data/RMS-Build-2016/Meta-Data/"
RMS_input_dir = "Data/RMS-Build-2016/RMS-Processed/Modules/"
macro_dir = "Data/Macro-Data/"
output_dir = "Data/Demand-Estimates/Nested-Logit-FE/"
iv_output_dir = "Data/Demand-Estimates/Nested-Logit-Hausman/"
fun_dir = "Scripts/bin/"

# Source Functions to be used
source(paste0(fun_dir, "AggregateBrandPrice.R"))
source(paste0(fun_dir, "HausmanIV.R"))
source(paste0(fun_dir, "LogDemandReg.R"))
source(paste0(fun_dir, "LogHausmanIVReg.R"))
source(paste0(fun_dir, "shareGen.R"))
myHoliday = function(inYear, holidayName, daysBefore, daysAfter){
  dates(as.character(seq((as.Date(holiday(inYear, holidayName))-daysBefore),
                         as.Date(holiday(inYear, holidayName))-daysAfter ,by='day')),format="Y-M-D")
}

# Initialize Parallel Environment
cores = detectCores(logical=TRUE)
cl = makeCluster(cores)

# Put relevant packages and functions on cluster
invisible(clusterEvalQ(cl, library(data.table)))
invisible(clusterEvalQ(cl, setNumericRounding(0)))
invisible(clusterEvalQ(cl, library(bit64)))
invisible(clusterEvalQ(cl, library(timeDate)))
invisible(clusterEvalQ(cl, library(chron)))
invisible(clusterEvalQ(cl, library(AER)))
invisible(clusterEvalQ(cl, setwd("~/PassThrough")))
clusterExport(cl, c('AggregateBrandPrice', 'LogDemandReg', 'LogHausmanIVReg'))

# Export directories and regression forms to cluster
clusterExport(cl, c('meta_dir', 'RMS_input_dir', 'macro_dir', 'output_dir', 'fun_dir', 'form', 'IVform'))

#---------------------------------------------------------------------------------------------------#

# Load Meta Data - Product Meta Data
load(paste0(meta_dir, "Meta-Data-Corrected.RData"))

# Load Product Characteristics Data
load(paste0(meta_dir, "Products-Corrected.RData"))

# Load the store data 
load(paste0(meta_dir, "Stores.RData"))

# Load Macro Data 
load("~/PassThrough/Data/Macro-Data/CPI-SA.RData")
load("~/PassThrough/Data/Macro-Data/employment.RData")

# Aggregate meta data to product level 
meta_data = meta_data[, .(productRev = sum(revenue_RMS), productWeek = sum(N_weeks_RMS)),
                      by = c("upc", "upc_ver_uc_corrected")]
setkey(meta_data, upc, upc_ver_uc_corrected)

# Product Cleaning to make units conversion and upc, upc_ver_uc_corrected unique.
# Convert PO to OZ
products[size1_units == "PO", `:=`(size1_amount = size1_amount*16, size1_units = "OZ" )]
products = products[, .(brand_descr_corrected = brand_descr_corrected[1], multi=multi[1],
                        size1_units=size1_units[1], size1_amount = size1_amount[1]),
                    by = c("upc", "upc_ver_uc_corrected", "product_module_code")]
products = products[product_module_code%in%module_list, ]

# Merge data; find the top brand in each category
setkey(products, upc, upc_ver_uc_corrected)
products = meta_data[products]
revenue = products[, .(brand_revenue = sum(productRev, na.rm=TRUE)), 
                   by = .(product_module_code, brand_descr_corrected)]
revenue = revenue[order(product_module_code, -brand_revenue),]
setkey(revenue, product_module_code)
clusterExport(cl, c('products'))

# Filter Stores -- only keep certain stores
stores = setDT(stores)[,.(store_code_uc, year, dma_code, dma_descr, parent_code)]
# Use first dma for each store
stores[, `:=`(dma_code = dma_code[1], dma_descr = dma_descr[1]), by = c("store_code_uc")]
stores[, `:=`(nchain = length(unique(parent_code))), by = "store_code_uc"]
stores = stores[nchain==1, ] # No stores which have switched chains
stores[, nchain:=NULL]

# Merge macro trend 
cpi[, yearmonth := as.integer(gsub("-","", year_month))]
month_emply[, yearmonth := as.integer(gsub("-","", month))]
macro_ind = month_emply[, .(yearmonth, dma_code, unemploy_rate)]
setkey(cpi, yearmonth)
setkey(macro_ind, yearmonth)
macro_ind = macro_ind[cpi[, .(yearmonth, inflation=value)], nomatch=0L]

#---------------------------------------------------------------------------------------------------#

# Aggregate Prices to Brand Level.
for (module in module_list){
  upc_files = list.files(paste0(RMS_input_dir, module))
  top_brands = revenue[.(module), brand_descr_corrected][1:top_cut]
  top_brands = top_brands[!grepl("CTL BR", top_brands)]
  clusterExport(cl, c('top_brands', 'module'))
  move_agg = parLapply(cl, upc_files, AggregateBrandPrice)
  gc()
  invisible(clusterEvalQ(cl, gc()))
  move_agg = rbindlist(move_agg)[, .(base_cum= sum(base_cum, na.rm=T),
                                     imputed_cum = sum(imputed_cum, na.rm=T),
                                     wts_cum = sum(wts_cum, na.rm=T),
                                     units = sum(units, na.rm=T)), 
                                 by = .(brand_descr_corrected, store_code_uc, week_end)]
  gc()
  
  # Define top stores
  # Extract year; Merge movement and store data
  move_agg[, `:=`(year = as.integer(format(week_end, format = "%Y")))]
  setkeyv(move_agg, c("store_code_uc", "year"))
  move_agg = move_agg[stores, nomatch=0L]
  
  # Compute the revenue for each store; share; culmulated share ranked by store revenues
  store_revenue = move_agg[, .(revenue = sum(wts_cum, na.rm = T)), by = .(store_code_uc)]
  store_revenue[, share := revenue/sum(revenue, na.rm = T)]
  setorder(store_revenue, -share)
  store_revenue[, cumshare := cumsum(share) - share]
  top_store = store_revenue[cumshare<=store_cum_cut, store_code_uc]
  move_agg = move_agg[(store_code_uc %in% top_store),] # Only keep sales from top stores
  
  # Filter markets with at least certain number of stores 
  move_agg[, nstore:=length(unique(store_code_uc)), by = c(mkt_level)]
  move_agg = move_agg[nstore>=nstore_cut, ]
  
  # Need to have at least certain number of Observations
  move_agg[, nobs:=.N, by = c(mkt_level, "brand_descr_corrected")]
  move_agg = move_agg[nobs>=nobs_cut, ]
  
  # Calculate price indexes based on stores and dmas; 
  move_agg[, `:=`(imputed_price = imputed_cum/wts_cum,
                  base_price = base_cum/wts_cum)]

  # Generate additional variables for promotion, seasonality and reference price
  move_agg[, `:=`(promotion = as.integer(imputed_price<=(0.95*base_price)))]
  
  # Extract month; See if it's holiday
  move_agg[, `:=`(Christmas=1*is.holiday(week_end, myHoliday(year,"USChristmasDay",7,0)),
                  Thanksgiving=1*is.holiday(week_end, myHoliday(year,"USThanksgivingDay",7,0)),
                  Easter=1*is.holiday(week_end, myHoliday(year,"Easter",7,0))), by = .(week_end, year)]
  
  # Calculate reference price
  setkey(move_agg, store_code_uc, brand_descr_corrected, week_end)
  move_agg[, `:=`(ref_price = shift(imputed_price)), by = .(brand_descr_corrected, store_code_uc)]
  
  # Generate Hausman IV
  dma_hausman = HausmanIV(move_agg)
  
  # Merge Hausman with Macro Trend
  setkey(dma_hausman, dma_code, yearmonth)
  setkey(macro_ind, dma_code, yearmonth)
  dma_data = dma_hausman[macro_ind, nomatch=0L]
  dma_data[, dma_descr:=NULL]
  setkey(dma_data, week_end)
  dma_data[, tm:=.GRP, by = "week_end"]
  
  # Merge Hauseman + Macro Trend with Sales Data
  setkey(move_agg, brand_descr_corrected, dma_code, week_end)
  setkey(dma_data, brand_descr_corrected, dma_code, week_end)
  move_agg = move_agg[dma_data, nomatch=0L]
  
  # Generate market share 
  shareGen(move_agg, mkt_level = mkt_s_level, timevar = t_level, pen_rate = prate)
  
  # Drop 0 market share products and markets -- not ideal but a very small percentage
  # Or no reference price
  # Drop other brand from the equation -- it's a composite
  move_agg = move_agg[s>0 & !is.na(ref_price) & !(brand_descr_corrected=="OTHER")]
  
  # Variable modification to prepare for regression
  move_agg[, `:=`(lshare = log(s)-log(s0), lcond_share = log(sg), lprice = log(imputed_price), lref = log(ref_price), 
                  lhausman = log(hausman), lcpi = log(inflation), lunemployment = log(unemploy_rate))]
  
  # Separate reference price effects (above and below)
  move_agg[, `:=`(labove_ref = ifelse(lref<lprice, lprice-lref, 0),
                  lbelow_ref = ifelse(lref>lprice, lref-lprice, 0),
                  IV_above_ref = ifelse(lref<lhausman, lhausman-lref, 0),
                  IV_below_ref = ifelse(lref>lhausman, lref-lhausman, 0))]
  move_agg[, grp_id := .GRP, by = c(mkt_level)]
  grp_info = move_agg[, .(grp_id = grp_id[1]), by = c(mkt_level)]
  setkey(move_agg, grp_id)
  
  # Split move_agg and put data into each worker 
  nworker = length(cl)
  max_grp_id = move_agg[, max(grp_id)]
  grp_group_list = split(1:max_grp_id, sort((1:max_grp_id)%%nworker))
  i = 0
  for (grp_group in grp_group_list){
    i = i + 1
    move_chunk = move_agg[.(grp_group), ]
    setkey(move_chunk, grp_id)
    clusterExport(cl[i], c("move_chunk", "grp_group"))
  }
  gc()
  
  # Cluster Evaluation
  estimates = invisible(clusterEvalQ(cl, LogDemandReg()))
  estimates = rbindlist(estimates)
  estimates[, `:=`(var_name = ifelse(var_name=="(Intercept)", "intercept", var_name))]
  estimates[, `:=`(var_name = ifelse(grepl("store_code_uc", var_name), 
                                     substr(var_name,22,nchar(var_name)), var_name))]
  estimates[, `:=`(var_name = ifelse(grepl("brand_descr_corrected", var_name), 
                                     substr(var_name,30,nchar(var_name)), var_name))]
  
  # Merge estimates with market identifiers
  setkey(estimates, grp_id)
  setkey(grp_info, grp_id)
  estimates = grp_info[estimates]
  setnames(estimates, c("Estimate", "Std. Error", "t value", "Pr(>|t|)"), c("bhat", "se", "tval", "pval"))

  # Estimate elasticity
  setkey(estimates, grp_id)
  setkey(move_agg, grp_id)
  move_agg = move_agg[estimates[var_name=="lprice", .(grp_id, bhat)], nomatch=0L]
  setnames(move_agg, "bhat", "pcoef")
  move_agg = move_agg[estimates[var_name=="lcond_share", .(grp_id, bhat)], nomatch=0L]
  setnames(move_agg, "bhat", "rho")
  move_agg[, own_elast := pcoef * (1/(1-rho) - sg * rho/(1-rho) - s)]
  move_agg[, cross_elast := pcoef * (- sg * rho/(1-rho) - s)]
  elastcities = move_agg[, .(own_elast = mean(own_elast),
                             cross_elast = mean(cross_elast)), 
                         by = c("grp_id", "dma_code", "parent_code", "brand_descr_corrected")]
  save(estimates, elastcities, file = paste0(output_dir, module, ".RData"))
  
  # IV Cluster Evaluation
  estimates = invisible(clusterEvalQ(cl, LogHausmanIVReg()))
  estimates = rbindlist(estimates)
  estimates[, `:=`(var_name = ifelse(var_name=="(Intercept)", "intercept", var_name))]
  estimates[, `:=`(var_name = ifelse(grepl("store_code_uc", var_name), 
                                     substr(var_name,22,nchar(var_name)), var_name))]
  estimates[, `:=`(var_name = ifelse(grepl("brand_descr_corrected", var_name), 
                                     substr(var_name,30,nchar(var_name)), var_name))]
  
  # Merge estimates with market identifiers
  setkey(estimates, grp_id)
  setkey(grp_info, grp_id)
  estimates = grp_info[estimates]
  setnames(estimates, c("Estimate", "Std. Error", "t value", "Pr(>|t|)"), c("bhat", "se", "tval", "pval"))

  # Estimate elasticity
  move_agg[, `:=`(pcoef=NULL, rho=NULL, own_elast=NULL, cross_elast=NULL)]
  setkey(estimates, grp_id)
  setkey(move_agg, grp_id)
  move_agg = move_agg[estimates[var_name=="lprice", .(grp_id, bhat)], nomatch=0L]
  setnames(move_agg, "bhat", "pcoef")
  move_agg = move_agg[estimates[var_name=="lcond_share", .(grp_id, bhat)], nomatch=0L]
  setnames(move_agg, "bhat", "rho")
  move_agg[, own_elast := pcoef * (1/(1-rho) - sg * rho/(1-rho) - s)]
  move_agg[, cross_elast := pcoef * (- sg * rho/(1-rho) - s)]
  elastcities = move_agg[, .(own_elast = mean(own_elast),
                             cross_elast = mean(cross_elast)), 
                         by = c("grp_id", "dma_code", "parent_code", "brand_descr_corrected")]
  save(estimates, elastcities, file = paste0(iv_output_dir, module, ".RData"))
  
  # Clean up worker workspace
  invisible(clusterEvalQ(cl, rm(move_chunk)))
  invisible(clusterEvalQ(cl, gc()))
}

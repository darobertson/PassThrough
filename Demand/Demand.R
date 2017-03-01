#####################################################################################################
#
# Compute the Fixed Effect Demand Model with Cross Sectional Units
# Xiliang Lin
# Febuary, 2016
#
#####################################################################################################
# Settings
rm(list = ls())

# Testing status
trial_run = TRUE
if (trial_run){
  module_list = 1393  # For testing
} else{
  module_list = "all" # For running
}

# Top brands to be included
top_cut = 5

# Load Necessary Packages
library(parallel)
library(data.table)
setNumericRounding(0)
library(nloptr)

# Set Working Folder Path Here
setwd("~/PassThrough")
meta_dir  = "Data/RMS-Build-2016/Meta-Data/"
RMS_input_dir = "Data/RMS-Build-2016/RMS-Processed/Modules/"
macro_dir = "Data/Macro-Data/"
output_dir = "Data/Demand-Data"

#---------------------------------------------------------------------------------------------------#

# Load Meta Data - Product Meta Data
load(paste0(meta_dir, "Meta-Data-Corrected.RData"))

# Load Product Characteristics Data
load(paste0(meta_dir, "Products-Corrected.RData"))

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
#---------------------------------------------------------------------------------------------------#

# Aggregate Prices to Brand Level.
for (module in module_list){
  all_move_file = list.files(paste0(RMS_input_dir, module))
  all_move = as.list(1:length(all_move_file))
  k = 0
  top_brands = revenue[.(module), brand_descr_corrected][1:top_cut]
  for (i in all_move_file){
    k = k+1
    load(paste0(RMS_input_dir, module, "/", i))
    move = move[!(is.na(base_price)|is.na(imputed_price))]
    move[, `:=`(store_rev = sum(units*imputed_price, na.rm=T)), by = .(upc, upc_ver_uc_corrected, store_code_uc)]
    
    setkey(move, upc, upc_ver_uc_corrected)
    # Half of the data is gone!
    move = move[products, nomatch=0L]
    
    # Obtain per unit price and units
    move[, `:=`(base_price = base_price/size1_amount, imputed_price = imputed_price/size1_amount,
                units = units*size1_amount*multi)]
    
    # Aggregate price to brand level (only consider top brands)
    move[!(brand_descr_corrected %in% top_brands), brand_descr_corrected := "OTHER"]
    
    all_move[[k]] = move[, .(base_cum= sum(store_rev*base_price, na.rm=T),
                             imputed_cum = sum(store_rev*imputed_price, na.rm=T),
                             wts_cum = sum(store_rev*(!is.na(imputed_price)), na.rm=T),
                             units = sum(units, na.rm=T)), 
                         by = .(brand_descr_corrected, store_code_uc, week_end)]
  }
  all_move = rbindlist(all_move)[, .(base_cum= sum(base_cum, na.rm=T),
                                     imputed_cum = sum(imputed_cum, na.rm=T),
                                     wts_cum = sum(wts_cum, na.rm=T),
                                     units = sum(units, na.rm=T)), 
                                 by = .(brand_descr_corrected, store_code_uc, week_end)]
  gc()
}

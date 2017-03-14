# Convert this to a function
AggregateBrandPrice = function(filename){
  if (!exists("products")){
    stop("Product database needs to be loaded first")
  }
  if (!exists("top_brands")){
    stop("The top brands in the category needs to be loaded first")
  }
  if (!exists("module")){
    stop("The module code need to be loaded first")
  }
  
  load(paste0(RMS_input_dir, module, "/", filename))
  move = move[!(is.na(base_price)|is.na(imputed_price))]
  if (nrow(move) == 0) return(data.table(NULL))
  move[, `:=`(store_rev = sum(units*imputed_price, na.rm=T)), by = .(upc, upc_ver_uc_corrected, store_code_uc)]
  
  setkey(move, upc, upc_ver_uc_corrected)
  # Half of the data is gone!
  move = move[products, nomatch=0L]
  
  # Obtain per unit price and units
  move[, `:=`(base_price = base_price/size1_amount, imputed_price = imputed_price/size1_amount,
              units = units*size1_amount*multi)]
  
  # Aggregate price to brand level (only consider top brands)
  move[!(brand_descr_corrected %in% top_brands), brand_descr_corrected := "OTHER"]
  
  move_agg = move[, .(base_cum= sum(store_rev*base_price, na.rm=T),
                           imputed_cum = sum(store_rev*imputed_price, na.rm=T),
                           wts_cum = sum(store_rev*(!is.na(imputed_price)), na.rm=T),
                           units = sum(units, na.rm=T)), 
                       by = .(brand_descr_corrected, store_code_uc, week_end)]
  rm(move)
  gc()
  return(move_agg)
}

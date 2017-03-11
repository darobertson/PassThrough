Elasticity = function(mv_dt, coefv = estimates){
  # Merge in price coefficient
  setkey(mv_dt)
  
  # Merge in correlation coefficient
  
  mv_dt[, tot_units := max(units), by = c(mkt_level, timevar)]
  mv_dt[, mkt_size := max(tot_units)/pen_rate, by = c(mkt_level)]
  mv_dt[, `:=`(s = units/mkt_size, sg = units/tot_units)] # To make sure no zero shares. 
  return(NULL)
}
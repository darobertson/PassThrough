shareGen = function(mv_dt, mkt_level = "store_code_uc", timevar = "week_end", pen_rate = 0.75){
  mv_dt[, tot_units := max(units), by = c(mkt_level, timevar)]
  mv_dt[, mkt_size := max(tot_units)/pen_rate, by = c(mkt_level)]
  mv_dt[, `:=`(s = units/mkt_size, sg = units/tot_units, s0 = 1 - tot_units/mkt_size)] # To make sure no zero shares. 
  return(NULL)
}
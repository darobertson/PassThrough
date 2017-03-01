# Generate Hauseman Instruments for Each DMA
HausmanIV = function(dat){
  dma_price = dat[, .(dma_price = sum(imputed_cum)/sum(wts_cum)), 
                      by = .(brand_descr_corrected, dma_code, dma_descr, week_end)]
  dma_price[, hausman := (sum(dma_price) - dma_price) / (.N-1), by = .(brand_descr_corrected, week_end)]
  dma_price[, `:=`(yearmonth = as.integer(format(week_end, format = "%Y%m")))]
  return(dma_price)
}

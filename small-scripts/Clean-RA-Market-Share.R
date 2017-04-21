#####################################################################################################
#
# Clean the market share data collected by RA
# Xiliang Lin
# April, 2017
#
#####################################################################################################
# Settings
rm(list = ls())

# Load dataset
load("/Users/xlin0/Dropbox/Commodity prices/xiaohui/Data collection/organized/national/dataset.RData")
mshare = copy(dataset1)
rm(dataset1)

# Hand code data set information for conversion purposes
mshare[, .(rho = cor(data_planet_per_capita_availability, Statista_avg_volumn_per_capita, use = "pairwise.complete.obs")),
       by = "product_module_code"]
mshare[, .(nobs = length(na.omit(data_planet_per_capita_availability + Statista_avg_volumn_per_capita))),
       by = "product_module_code"]

mshare[, .(rho = cor(passport_market_size, Statista_avg_volumn_per_capita, use = "pairwise.complete.obs")),
           by = "product_module_code"]
mshare[, .(nobs = length(na.omit(passport_market_size + Statista_avg_volumn_per_capita))),
       by = "product_module_code"]

mshare[, .(rho = cor(USDA_per_capita_availability, Statista_avg_volumn_per_capita, use = "pairwise.complete.obs")),
       by = "product_module_code"]
mshare[, .(nobs = length(na.omit(USDA_per_capita_availability + Statista_avg_volumn_per_capita))),
       by = "product_module_code"]

# Us population by year
# census data in millions
uspop = c(323.42, 321.93, 319.70, 317.34, 314.96, 312.76, 310.45, 308.11, 306.77, 304.09, 
          301.23, 298.38, 295.52, 292.81, 290.11, 287.63, 284.97)
uspop = data.table(year = c(2017:2001), pop = uspop)

# Merge us pop
setkey(uspop, year)
setkey(mshare, year)
mshare = mshare[uspop, nomatch=0L]

# setkey for mshare
setkeyv(mshare, c("product_module_code", "year"))

# mshare for paper towel is duplicated with erroreous toilet paper sales
mshare = mshare[!(product_module_code==7734&passport_market_size>=2000)]

# Convert all per-capita to overall
mshare[, `:=`(data_planet = data_planet_per_capita_availability * pop, 
              USDA = USDA_per_capita_availability * pop, 
              passport = passport_market_size, 
              Statista_rev_agg = Statista_avg_revenue_per_capita * pop,
              Statista_rev = Statista_revenue,
              Statista_vol_agg = Statista_avg_volumn_per_capita * pop,
              Statista_volume = Statista_volumn)]
mshare[, c("data_planet_per_capita_availability", "USDA_per_capita_availability", 
          "passport_market_size", "Statista_avg_revenue_per_capita", 
          "Statista_avg_volumn_per_capita", "Statista_volumn", "Statista_revenue"):=NULL]

# Convert all variables to the same units
# Revenue to millions of dollars
# volume to millions of OZ (either weight or fluid OZ)
# Unfinished! Pick up here!
mshare[product_module_code==1040, `:=`(data_planet = data_planet*14.63)]

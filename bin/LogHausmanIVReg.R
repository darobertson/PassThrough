LogHausmanIVReg = function(){
  if (!exists("IVform")){
    stop("IV regression form needs to be loaded first")
  }
  if (!exists("move_chunk")){
    stop("Data move_chunk needs to be loaded first")
  }
  if (!exists("grp_group")){
    stop("Data grp_group needs to be loaded first")
  }
  regfun = function(g){
    if (move_chunk[.(g), length(unique(brand_descr_corrected))]<=1) return(data.table(NULL))
    regout = coef(summary(ivreg(IVform, data = move_chunk[.(g), ])))
    regout = data.table(var_name = row.names(regout), regout)
    regout[, grp_id:=g]
    return(regout)
  }
  return(rbindlist(lapply(grp_group, regfun)))
}
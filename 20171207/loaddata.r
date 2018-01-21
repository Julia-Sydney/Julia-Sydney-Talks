library(data.table)
library(magrittr)
memory.limit(160000)
pt <- proc.time()
# 70 seconds first run
dpath = "D:/data/lending_club/csv/consolidated/notlogin/"
system.time(
  a <- lapply(dir(dpath, full.name = T), fread, showProgress = F) %>%
    rbindlist)
timetaken(pt)

system.time(feather::write_feather(a, "D:/git/Julia-Sydney-Talks/20171207/lc.feather")) #10 seconds

pt = proc.time()
system.time(fst::write_fst(a, "D:/git/Julia-Sydney-Talks/20171207/lc.fst"), 100)
timetaken(pt)


system.time(fa <- feather::read_feather("D:/git/Julia-Sydney-Talks/20171207/lc.feather"))


system.time(fsta <- fst::read_fst("D:/git/Julia-Sydney-Talks/20171207/lc.fst"))

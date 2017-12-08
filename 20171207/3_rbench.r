require(data.table)
N=2e8; K=100
set.seed(1)
DT <- data.table(
  id3 = sample(N/K, N, TRUE),
  id4 = sample(K, N, TRUE),                          # large groups (int)
  v1 =  sample(5, N, TRUE)                          # int in range [1,5
)
cat("GB =", round(sum(gc()[,2])/1024, 3), "\n")
proc.time()
system.time( DT[, sum(v1), keyby=id3] )
system.time( DT[, sum(v1), keyby=id3] )
2+2
system.time( DT[, sum(v1), keyby=id4] )
system.time( DT[, sum(v1), keyby=id4] )
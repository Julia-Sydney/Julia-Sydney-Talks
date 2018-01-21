using FastGroupBy, RCall

N = 10_000_000
K = 100

################################################################################
# Sorting 10m strings with 100k unique values
################################################################################
str_samplespace = [randstring(8) for i=1:N÷K];

a = rand(str_samplespace, N);
@time radixsort(a);

@rput a; # copy variable a into R

R"""
memory.limit(2^31-1)
st = system.time(sort(a, method="radix"))
rm(a); gc()
st
"""

################################################################################
# Sorting 1m strings with 1m unique values
# this should yield similar speeds as R's advantage is no longer!
################################################################################
str_1m = [randstring(8) for i=1:1_000_000];
@time radixsort(str_1m);

@rput str_1m;
using RCall
R"""
memory.limit(2^31-1)
st = system.time(sort(str_1m, method="radix"))
rm(str_1m); gc()
st
"""

################################################################################
# Categorical Arrays
################################################################################
using CategoricalArrays
pools = unique([randstring(8) for i = 1:N÷K]);
byvec = CategoricalArray{String, 1}(rand(UInt32(1):UInt32(length(pools)), N), CategoricalPool(pools, false));
valvec = rand(N);
@time fnrs = fastby!(sum, byvec, valvec); #
@time FastGroupBy.cate_sum_by(byvec, valvec); # fast path

using RCall
R"""
memory.limit(2^31-1)
library(data.table)
df = data.table(a = $byvec, val = $valvec)
system.time(df[,sum(val),a])
"""




################################################################################
# R grouping
################################################################################

using RCall
R"""
memory.limit(2^31-1)
library(data.table)
df = data.table(a = $a, val = $val)
system.time(df[,sum(val),a])
"""

val = rand(N);

@time res = fastby(sum, a, val);


using RCall
R"""
library(data.table)
df = data.table(a = $a, val = $val)
system.time(df[,sum(val),a])
"""

x_grouped = repeat(str_samplespace, inner = 100)
@time res = fastby(sum, x_grouped, val)

using RCall
R"""
library(data.table)
df = data.table(a = $x_grouped, val = $val)
system.time(df[,sum(val),a])
"""

using StaticArrays

function Base.isless(a::SVector, b::SVector)
#may segfault on bad input; too lazy to do right now
       @inbounds for i in 1:length(a)
       a[i] < b[i] && return true
       a[i] > b[i] && return false
       end
       return false
       end

A= rand(SVector{8,Float32},10_000_000);
A2 = rand(Float32, 8, 10_000_000);

#post warm-up
@time sortcols(A2);
 26.563095 seconds (10.12 M allocations: 921.714 MiB, 3.65% gc time)
@time sort(A; alg=QuickSort);
  3.976193 seconds (13 allocations: 305.176 MiB, 1.73% gc time)
@time a2 =sort(A; alg=MergeSort);
  5.784950 seconds (15 allocations: 457.764 MiB, 0.07% gc time)
  
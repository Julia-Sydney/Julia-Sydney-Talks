dpath = "D:/data/lending_club/csv/consolidated/notlogin/"
using DataFrames
using FastGroupBy

const N = Int(1e8)
testdf = DataFrame(large_n_grps = rand(1:Int32(N/100), N), small_n_grps = rand(1:100, N), v1 = rand(1:5, N))
@time testdf[sortperm(testdf[:small_n_grps]),:]

function fsort(df::DataFrame, cols)
    x = df[cols]
    df[sortperm(x),:]
end

@time fsort(testdf, :small_n_grps)
#@time sort(testdf, cols = :small_n_grps) # 40 second

# @time sumby(testdf, :small_n_grps, :v1)
# @time sumby(testdf, :large_n_grps, :v1)
# using SortingAlgorithms
#
x = copy(testdf[:small_n_grps])
# @time sort!(x,alg=RadixSort)
#
# x = copy(testdf[:small_n_grps])
# @time new_order = fsortandperm_radix!(x)
#
# copy(testdf[:small_n_grps])[new_order[2]] |> issorted
#
# @code_warntype sorttwo!(x, collect(1:length(x)))

function fsort2(df,cols)
    bycol = copy(df[cols])
    (bycol_sorted, order_col) = fsortandperm_radix!(bycol)
    df[order_col,:]
end

@code_warntype fsortandperm_radix!(copy(testdf[:small_n_grps]))

x = copy(testdf[:small_n_grps])
@time sort!(x)
x = copy(testdf[:small_n_grps])
@time fsortandperm_radix!(x)

@code_warntype fsortandperm_radix!(x)
y = collect(1:length(x))



@code_warntype sorttwo!(x, y)

x = copy(testdf[:small_n_grps])
y = collect(1:length(x))
@time sorttwo!(x,y)

x = copy(testdf[:small_n_grps])
@time sort!(x)
x = copy(testdf[:small_n_grps])
@time sort!(x, alg=RadixSort)

@which sort!(x, alg=RadixSort)

x = copy(testdf[:small_n_grps])
@time sortperm(x)
@time fsortandperm_radix!(x)

# data.table 2.5 seconds
@time testdf_sorted = fsort(testdf,:small_n_grps) # 12-15
@time testdf_sorted_old_method = sort(testdf, cols = [:small_n_grps]) # 35-40 seconds
@time sort!(testdf, cols =[:small_n_grps]) # 116 seconds

# data.table 12.8seconds
@time testdf_sortedl = fsort(testdf,:large_n_grps) # 25 seconds
@time testdf_sorted_old_methodl = sort(testdf, cols = [:large_n_grps]) #80 seconds

@time testdf[new_order[2],:]
@time sort!(testdf, cols=[:small_n_grps])

@time sumby(testdf_sorted, :small_n_grps, :v1)

@time testdf_sorted2 = sort(testdf, cols=[:small_n_grps])
@time sumby(testdf_sorted, :large_n_grps, :v1)



@time sumby_multi_rs(x1, x2)
@time sumby_multi_rs(x1, x2)

@time sumby_radixsort(x1, x2)
@time sumby_radixgroup(x1, x2)

using uCSV
@time ucsvread = uCSV.read(dpath*readdir(dpath)[end],quotes='"', header=1)
@time dt = DataFrame(ucsvread)

using JuliaDB
# addprocs()
# @everywhere dpath = "D:/data/lending_club/csv/in one place/"
@time data = JuliaDB.loadndsparse(dpath, escapechar='"',usecache=false);# 2 mins
eltype(data)
colnames(data)


# ;cd "/D/data/lending_club/csv/consolidated/"
# ;sed -i -n -e '/^\"/p' *.csv
using CSV
@time acsv = CSV.read(dpath*readdir(dpath)[end])
acsv[:id]

using TextParse
@time tcsv = TextParse.csvread(dpath*readdir(dpath)[end])
dftcsv = DataFrame(tcsv)

using RCall
function read_all_using_r(dir)
    R"""
    library(data.table)
    library(magrittr)
    memory.limit(160000)
    pt <- proc.time()
    # 40 seconds first run
    system.time(
        a <- lapply(dir($dir, full.name = T), fread, showProgress = F) %>%
            rbindlist)
    timetaken(pt)
    """
    @rget a
    a
end

@time a = read_all_using_r(dpath)
@time a = read_all_using_r(dpath)

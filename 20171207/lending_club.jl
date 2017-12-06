dpath = "c:/temp/LoanStats_2016Q3.csv"

using uCSV
@time a = uCSV.read(dpath, escapequote='"')

using CSV
@time a = CSV.read(dpath)

using RCall
R"""
a = data.table::fread($dpath)
feather::write_feather(a, "loan3.feather")
"""
@time @rget a


using Feather, NamedTuples

a = Feather.read("loan3.feather")

names(a)
parse.(Int, a[:loan_amnt])

using DataFrames
colwise(a) do col1
    x = rand(1:length(col1),1)[1]
    println(col1[x])
end

names(a)

using StatsBase

function counttbl(v)
    ca = countmap(a[:loan_status])
    vca = [ca[k] for k in keys(ca)]
    DataFrames.DataFrame(category = keys(ca) |> collect, cnt = vca)
end

counttbl(a[:loan_status])

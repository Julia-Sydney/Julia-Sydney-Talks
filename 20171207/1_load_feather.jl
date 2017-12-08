@time using Feather
fpath = "D:/git/Julia-Sydney-Talks/20171207/lc.feather"
# loads in 67 seconds if in feather format
# R can read from cold in 20 seconds
@time dataf_backup = Feather.read(fpath)

# make a backup just in case
dataf = copy(dataf_backup)

# should say DataFrames.DataFrame
typeof(dataf)

# I want to make all numerics into Float or Integer
# use regex pattern matching
#
# Julia computing's JuliaDB has a way to do it automatically on loading of data
# https://juliacomputing.com/blog/2017/08/22/lendingclub-demo-blog.html

float_regex = r"^[0-9]+\.[0-9]+$"
integer_regex = r"^[0-9]+$"

using DataFrames
for (symbol, val) in eachcol(dataf[1,:])
    if ismatch(float_regex, val[1])
        println(symbol)
    end
end

bool_numeric = [(symbol, ismatch(float_regex, val[1])) for (symbol, val) in eachcol(dataf[1,:])]
bool_int = [(symbol, ismatch(integer_regex, val[1])) for (symbol, val) in eachcol(dataf[1,:])]

# nothing is coded as float and integer
[a & b for ((s, a), (t, b)) in zip(bool_numeric, bool_int)] |> any

# convert string to integer
parse(Int, "888")

# apply the same function to an array?
# put a . as function suffix
parse.(Int, ["888","888888"])

# it will fail
# as parse(Float64, "") is not defined
cdataf = copy(dataf)
for (symbol, is_col_numeric) in bool_numeric
    if is_col_numeric
        cdataf[symbol] = parse.(Float64, cdataf[symbol])
    end
end

# I want to use missing
# create a function to convert "" to missing
function emptystrtoi(str)
    if str == ""
        return missing
    else
        return str
    end
end

emptystrtoi("")

# it will fail again
# because parse(Float64, missing) is not defined
cdataf = copy(dataf)
for (symbol, is_col_numeric) in bool_numeric
    if is_col_numeric
        cdataf[symbol] = parse.(Float64, emptystrtoi.(cdataf[symbol]))
    end
end

# redefine parse
# to extend/overload a function that exists
# need to explicitly import
import Base.parse
parse(::Type, ::Missings.Missing) = missing

# convert all numeric strings to float
# conver all ineger strings to integers
@time for (symbol, is_col_numeric) in [bool_numeric...,bool_int...]
    if is_col_numeric
        dataf[symbol] = parse.(Float64, emptystrtoi.(dataf[symbol]))
    end
end


# @time for (symbol, is_col_int) in bool_int
#     if is_col_int
#         println(symbol)
#         dataf[symbol] = parse.(Int, emptystrtoi.(dataf[symbol]))
#     end
# end

names(dataf)

@time by(dataf[[:grade, :loan_amnt]],  :grade, df -> sum(df[:loan_amnt])) |> sort!
@time by(dataf[[:sub_grade, :loan_amnt]],  :sub_grade, df -> sum(df[:loan_amnt]))

using StatsBase
countmap(dataf[:loan_status])

# filtering
charged_off_df = dataf[dataf[:loan_status] .== "Charged Off",:]

@time by(dataf[[:grade, :loan_amnt]],  :grade, df -> sum(df[:loan_amnt]))
@time aggregate(dataf[[:grade, :loan_amnt]], :grade, sum) |> sort!

using FastGroupBy
@time sumby(dataf[:loan_status], ones(Int, size(dataf)[1]))

dataf[:loan_status_cate] = CategoricalArray(dataf[:loan_status])
@time sumby(dataf[:loan_status_cate], ones(Int, size(dataf)[1]))

cd = countmap(dataf[:issue_d])
dates = collect(keys(cd))
"01-" .* dates
dd = Dates.Date.("01-" .* dates,"dd-u-yyyy")

iddf = DataFrame(issue_date = dd, val = [v for (k,v) in cd])
sort!(iddf)

function datetocount(df)
    cd = countmap(df[:issue_d])
    dates = collect(keys(cd))
    "01-" .* dates
    dd = Dates.Date.("01-" .* dates,"dd-u-yyyy")

    iddf = DataFrame(issue_date = dd, val = [cd[k] for k in keys(cd)])
    sort!(iddf)
end

a = datetocount(dataf)
b = datetocount(charged_off_df)

c = join(a, b, on =:issue_date)

rename!(c, :val, :n_origination)
rename!(c, :val_1, :n_chargeoff)

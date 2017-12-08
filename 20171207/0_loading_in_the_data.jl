dpath = "D:/data/lending_club/csv/consolidated/notlogin/"
dpath2 = "D:/data/lending_club/csv/consolidated/notlogin2/"
dpathout = "D:/data/lending_club/csv/consolidated"
# ;cd "/D/data/lending_club/csv/consolidated/"
# ;sed -i -n -e '/^/"/p' /D/data/lending_club/csv/consolidated/.csv

using DataFrames
using FastGroupBy


using uCSV
# @time ucsvread = uCSV.read(dpath*readdir(dpath)[end])
# exemplary bug report inspired by Elm
# Parsed 147 fields on row 78. Expected 145.
# line:
# "","","21000","21000","21000"," 36 months"," 12.62%","703.74","C","C1","SVP, Chief Information Office, NA Claims","3 years","MORTGAGE","450000","Verified","Sep-2017","Current","n","","","credit_card","Credit card refinancing","079xx","NJ","18.94","0","Mar-2001","1","","","16","0","130690","84%","29","w","20517.11","20517.11","674.29","674.29","482.89","191.40","0.0","0.0","0.0","Nov-2017","703.74","Dec-2017","Nov-2017","0","","1","Individual","","","","0","0","573693","1","7","2","5","7","113774","56","1","3","28958","84","155550","1","4","3","8","35855","15079","77","0","0","106","198","2","2","6","2","","0","","0","7","8","11","11","10","8","15","8","16","","0","0","3","100","85.7","0","0","723948","194596","105550","204398","","","","","","","","","","","","N","","","","","","","","","","","","","","","Cash","N","","","","","",""
# Possible fixes may include:
#   1. including 78 in the `skiprows` argument
#   2. setting `skipmalformed=true`
#   3. if this line is a comment, setting the `comment` argument
#   4. if fields are quoted, setting the `quotes` argument
#   5. if special characters are escaped, setting the `escape` argument
#   6. fixing the malformed line in the source or file before invoking `uCSV.read`

@time ucsvread = uCSV.read(dpath*readdir(dpath)[end],quotes='"', header=1) #38 seconds
@time dt = DataFrame(ucsvread)

# everything is loaded as a string
dt[:id]
dt[:loan_amnt]
# What do I miss from Rstudio? Autocompletion of colum names

# ways to solve this
# 1 manually go through and figure which ones are which
# 2 use regex

using JuliaDB
# addprocs() # start a number of julia process in the background
@time data = JuliaDB.loadtable(dpath2, escapechar='"', usecache=false) # 2 mins
@time JuliaDB.save(data, dpathout)
@time data = JuliaDB.load(dpathout)

# second time you load in 1.17 seconds if started
@time data = JuliaDB.loadtable(dpath2, escapechar='"', usecache=true);
fields = fieldnames(eltype(data))
ftypes = eltype(data).parameters

numeric_cols = [fields[i] for i = 1:length(fields) if (ftypes[i] <: Number ||
            ftypes[i] <: Nullable{<:Number}) && !(fields[i] in [:id, :member_id, :dti_joint])];
#
# const bad_statuses = ("Late (16-30 days)","Late (31-120 days)","Default","Charged Off")
# good_loans = filter(x->!(x.loan_status in bad_statuses), data)
# bad_loans = filter(x->x.loan_status in bad_statuses, data);

@time using Feather
fpath = "D:/git/Julia-Sydney-Talks/20171207/lc.feather"
# loads in 67 seconds if in feather format
# can load in 8 seconds
# data.table can read from cold in 20 seconds
@time dataf = Feather.read(fpath)
# @time data = JuliaDB.loadndsparse(dpath, escapechar='"',usecache=false); # 2 mins
#
# # second time you load in 1.17 seconds
# @time data = JuliaDB.loadndsparse(dpath, escapechar='"');
# pretty ugly
# segue into type-stability
eltype(data)
colnames(data)

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

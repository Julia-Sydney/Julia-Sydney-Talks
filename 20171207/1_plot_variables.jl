dpath = "D:/data/lending_club/csv/consolidated/notlogin/"
dpath2 = "D:/data/lending_club/csv/consolidated/notlogin2/"
dpathout = "D:/data/lending_club/csv/consolidated"
# ;cd "/D/data/lending_club/csv/consolidated/"
# ;sed -i -n -e '/^/"/p' /D/data/lending_club/csv/consolidated/.csv

using JuliaDB
# second time you load in 1.17 seconds if started
@time data = JuliaDB.loadtable(dpath2, escapechar='"', usecache=true);
fields = fieldnames(eltype(data));
ftypes = eltype(data).parameters;

numeric_cols = [fields[i] for i = 1:length(fields) if (ftypes[i] <: Number ||
            ftypes[i] <: Nullable{<:Number}) && !(fields[i] in [:id, :member_id, :dti_joint])];

const bad_statuses = ("Late (16-30 days)","Late (31-120 days)","Default","Charged Off")
good_loans = filter(x->!(x.loan_status in bad_statuses), data);
bad_loans = filter(x->x.loan_status in bad_statuses, data);

using Gadfly
import NullableArrays: dropnull
# Density plot for bad and good loans
plots = Gadfly.Plot[]

for (name, g, b) in zip(numeric_cols, columns(good_loans), columns(bad_loans))
    g = dropnull(g)
    b = dropnull(b)
    p = plot(layer(x=g, Geom.density, Theme(default_color=colorant"green")),
             layer(x=b, Geom.density, Theme(default_color=colorant"red")),
              Guide.title(string(name)), Guide.ylabel("density"))
    push!(plots, p)
end

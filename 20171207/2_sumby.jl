#fast sumby
using DataFrames
using FastGroupBy

const N = Int(2e8) # 200 million rows
const K = 100
testdf = DataFrame(
            large_n_grps = rand(1:Int(N/K), N),
            small_n_grps = rand(1:K, N),
            v1 = rand(1:5, N))

# @time by(testdf, :large_n_grps, df -> sum(df[:v1]))
# @time by(testdf[[:large_n_grps,:v1]], :large_n_grps, df -> sum(df[:v1]))


@time sumby(testdf, :large_n_grps, :v1)

# convert to CategoricalArray
@time sumby(testdf, :small_n_grps, :v1)

testdf[:sc] = CategoricalArray(testdf[:small_n_grps])

@time sumby(testdf, :sc, :v1)
@time sumby(testdf, :sc, :v1)

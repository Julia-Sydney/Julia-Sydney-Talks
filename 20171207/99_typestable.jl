testmul(a,b) = a*b;

testmul(10, 100)
testmul("b","c")

@code_warntype testmul(10, 100)
# Variables:
#   #self# <optimized out>
#   a::Int64
#   b::Int64
#
# Body:
#   begin
#       return (Base.mul_int)(a::Int64, b::Int64)::Int64
#   end::Int64

@code_warntype testmul("b","c")
# Variables:
#   #self# <optimized out>
#   a::String
#   b::String
#
# Body:
#   begin
#       return $(Expr(:invoke, MethodInstance for string(::String, ::Vararg{String,N} where N), :(Base.string), :(a), :(b)))
#   end::String

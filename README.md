# IndexedDataFrames
A small package to allow Julia `DataFrames` to have meaningful row indices.  

Create an `IndexedDataFrame` by feeding a `DataFrames.DataFrame` into `IndexedDataFrames.IndexedDataFrame` and specifying the index column. For example,
```julia
import DataFrames
import IndexedDataFrames
df = DataFrames.DataFrame(id=[100, 200, 300], a=[1, 2, 3], b=[4, 5, 6])
idf = IndexedDataFrames.IndexedDataFrame(df, "id")
```
The `IndexedDataFrame` then behaves much like a regular `DataFrames.DataFrame`, although I haven't made an exhaustive effort to cover all the overloads. For example, the user could build on `idf` with
```julia
idf[400] = (a=7, b=8)
idf[500] = Dict("a" => 9, "b" => 10)
idf[:, "c"] = [11, 12, 13, 14, 15]
push!(idf, (id=600, a=16, b=17, c=18))
deleteat!(idf, 200)
println(idf)
println(idf[300])
```
etcetera.
```
# Output
IndexedDataFrame with index column id:
5×4 DataFrame
 Row │ id     a      b      c     
     │ Int64  Int64  Int64  Int64 
─────┼────────────────────────────
   1 │   100      1      4     11
   2 │   300      3      6     13
   3 │   400      7      8     14
   4 │   500      9     10     15
   5 │   600     16     17     18
DataFrameRow
 Row │ id     a      b      c     
     │ Int64  Int64  Int64  Int64 
─────┼────────────────────────────
   2 │   300      3      6     13
```

The `IndexedDataFrame` has the underlying `DataFrames.DataFrame` as an attribute called `df`. However, be cautions about manually modifying that. Things like `IndexedDataFrames.check_uniqueness` don't run when you do that until the next time you access the `IndexedDataFrame`. In some cases, I might not have built out the specific overload you're looking for, so you'll have to access the underlying `DataFrame`. Just be careful with what you do. Don't make a new row with the same index value as one that already exists.

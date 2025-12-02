import Test
import DataFrames
import IndexedDataFrames 

df = DataFrames.DataFrame(id=[100, 200, 300], a=[1, 2, 3], b=[4, 5, 6])
idf = IndexedDataFrames.IndexedDataFrame(df, "id")

Test.@testset "Adding a row via push!" begin
    println("Starting pushing")
    push!(idf, (id=400, a=7, b=8))
    push!(idf, Dict("id" => 600, "a" => 9, "b" => 10))

    testdf = DataFrames.DataFrame(
        id=[100, 200, 300, 400, 600],
        a=[1, 2, 3, 7, 9],
        b=[4, 5, 6, 8, 10]
    )
    test_idf = IndexedDataFrames.IndexedDataFrame(testdf, "id")
    Test.@test test_idf.df == idf.df
end

Test.@testset "Adding a row via setindex!" begin
    idf[700] = (a=11, b=12)
    idf[800] = Dict("a" => 13, "b" => 14)

    testdf = DataFrames.DataFrame(
        id=[100, 200, 300, 400, 600, 700, 800],
        a=[1, 2, 3, 7, 9, 11, 13],
        b=[4, 5, 6, 8, 10, 12, 14]
    )
    testidf = IndexedDataFrames.IndexedDataFrame(testdf, "id")
    Test.@test testidf.df == idf.df
end

Test.@testset "Changing the index value" begin
   idf[700, "id"] = 900 
   Test.@test idf[900] == (id=900, a=11, b=12)
   # Ensure the old index value is no longer there.
   Test.@test !(700 in idf.df[!, idf.index_col])
end

Test.@testset "Replace a column" begin
    idf[:, "b"] = [14, 15, 16, 18, 100, 112, 114]

    test_df = DataFrames.DataFrame(
        id=[100, 200, 300, 400, 600, 900, 800],
        a=[1, 2, 3, 7, 9, 11, 13],
        b=[14, 15, 16, 18, 100, 112, 114]
    )
    test_idf = IndexedDataFrames.IndexedDataFrame(test_df, "id")
    Test.@test test_idf.df == idf.df
end

Test.@testset "Add a column" begin
    idf[:, "c"] = [14, 15, 16, 18, 100, 112, 114] 

    test_df = DataFrames.DataFrame(
        id=[100, 200, 300, 400, 600, 900, 800],
        a=[1, 2, 3, 7, 9, 11, 13],
        b=[14, 15, 16, 18, 100, 112, 114],
        c=[14, 15, 16, 18, 100, 112, 114]
    )
    test_idf = IndexedDataFrames.IndexedDataFrame(test_df, "id")
    Test.@test test_idf.df == idf.df
end

Test.@testset "Not allowing duplicate indices" begin
    # Cannot create an IndexedDataFrame from a DataFrame with duplicate values in the
    # index column.
    df2 = DataFrames.DataFrame(
        id=["a", "b", "c", "a"],
        col1=[1, 2, 3, 4],
        col2=[5, 6, 7, 8]
    )
    Test.@test_throws DomainError idf2 = IndexedDataFrames.IndexedDataFrame(df2, "id")
    
    # Changing the index value to one that already exists is not allowed.
    Test.@test_throws ArgumentError idf[200, "id"] = 100

    # Messing with the underlying DataFrame won't show an error initially, but
    # we catch it when we subsequently work with the IndexedDataFrame via
    # IndexedDataFrame.check_uniqueness, which most overloads incorporate.
    push!(idf.df, (id=200, a=15, b=16, c=17))
    Test.@test_throws DomainError println(idf)
    Test.@test_throws DomainError idf[900] = (a=17, b=18, c=19)
end

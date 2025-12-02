module IndexedDataFrames

import DataFrames
import StatsBase

struct IndexedDataFrame
    df::DataFrames.DataFrame
    index_col::String
    index_col_i::Int
end

function IndexedDataFrame(df::DataFrames.DataFrame, index_col)
    index_col = String(index_col)

    # Validate the index column exists
    if !(index_col in names(df))
        throw(ArgumentError(
            "Index column $(index_col) does not exist in DataFrame"
        ))
    end

    # Validate uniqueness of index column
    check_uniqueness(df, index_col)

    # Save index column position
    index_col_i = findfirst(==(index_col), names(df))

    return IndexedDataFrame(df, index_col, index_col_i)
end

# Find the index in the underlying DataFrame corresponding to a given idx_val
function find_row(idf::IndexedDataFrame, idx_val)
    row_i = findfirst(==(idx_val), idf.df[!, idf.index_col])
    if row_i === nothing
        throw(KeyError("No row with index $(idx_val)"))
    end
    return row_i
end

# Verify that there are no duplicate index values in the index column. We're
# writing this to dake a DataFrames.DataFrame and an index column specifier as
# opposed to just an IndexedDataFrame so that we can use it to check the validity of
# the underlying DataFrames.DataFrame before we actually construct the
# IndexedDataFrame.
function check_uniqueness(
        df::DataFrames.DataFrame,
        idx_col_name::Union{String, Symbol})
    col = df[!, idx_col_name]
    counts = StatsBase.countmap(df[!, idx_col_name])
    duplicates = [k for (k, v) in counts if v > 1]
    if length(duplicates) > 0
        throw(DomainError(
            "Index column `$(idx_col_name)` contains duplicates of the" *
                " following indices: $duplicates."
        ))
    end
    return nothing
end


# Verify that setindex isn't duplicating an already existing index value,
# because we could potentially be changing the index value, E.g. 
# `idf[idx_val, idf.index_col] = new_idx_val`
function check_setindex(
        idf::IndexedDataFrame,
        val,
        idx_val,
        col::Union{String, Symbol})
    is_setting_index = String(col) == idf.index_col
    current_val = idf[idx_val, col]
    val_is_changing = current_val != val
    already_exists = val in idf.df[!, idf.index_col]
    if (
        is_setting_index
        && val_is_changing
        && already_exists
    )
        throw(ArgumentError(
            "The index column already contains a $val row."
        ))
    end
    return nothing
end

###############################################################################
# Overloads
###############################################################################
import Base: getindex, setindex!, show, getproperty, deleteat!, push!

# Allow getting a row by the value in the index column:
function getindex(idf::IndexedDataFrame, idx_val)
    row_i = find_row(idf, idx_val)

    # Always good to recheck uniqueness in case the user manually modified the
    # underlying DataFrame
    check_uniqueness(idf.df, idf.index_col)

    return idf.df[row_i, :]
end

# Allow getting a whole column
function getindex(idf::IndexedDataFrame, ::Colon, col::Union{String, Symbol})
    # Always good to recheck uniqueness in case the user manually modified the
    # underlying DataFrame
    check_uniqueness(idf.df, idf.index_col)
    return idf.df[:, col]
end

# Allow getting a specific cell by (index_val, column)
function getindex(idf::IndexedDataFrame, idx_val, col)
    col = String(col)
    row_i = find_row(idf, idx_val)

    # Always good to recheck uniqueness in case the user manually modified the
    # underlying DataFrame
    check_uniqueness(idf.df, idf.index_col)

    return idf.df[row_i, col]
end

# For updating a single cell (value can be Int, Float64, String, etc.)
function setindex!(idf::IndexedDataFrame, value, idx_val, col::Union{String, Symbol})
    row_i = find_row(idf, idx_val)
    check_setindex(idf, value, idx_val, col)
    idf.df[row_i, col] = value

    # Always good to recheck uniqueness in case the user manually modified the
    # underlying DataFrame
    check_uniqueness(idf.df, idf.index_col)

    return idf
end

# Allow setting a whole row by index_val with a NamedTuple or Dict
function setindex!(idf::IndexedDataFrame, row_data::Union{NamedTuple, Dict}, idx_val)
    already_exists = idx_val in idf.df[!, idf.index_col]
    if !already_exists
        if row_data isa NamedTuple
            # NamedTuple keys must be symbols
            idx_col_sym = Symbol(idf.index_col)
            # Using `;` to create a NamedTuple with a dynamic field name
            index_nt = (; idx_col_sym => idx_val,)
            # Combining the index with the data
            row_data = (; row_data..., index_nt...)
        else row_data isa Dict
            row_data[idf.index_col] = idx_val
        end
        push!(idf, row_data)
    else
        row_i = find_row(idf, idx_val)
        for (col, val) in pairs(row_data)
            check_setindex(idf, val, idx_val, col)
            idf.df[row_i, col] = val
        end
    end

    # Always good to recheck uniqueness in case the user manually modified the
    # underlying DataFrame
    check_uniqueness(idf.df, idf.index_col)

    return idf
end

# Show method for nicer display
function show(io::IO, idf::IndexedDataFrame)

    # Always good to recheck uniqueness in case the user manually modified the
    # underlying DataFrame
    check_uniqueness(idf.df, idf.index_col)

    print(io, "IndexedDataFrame with index column $(idf.index_col):\n")
    show(io, idf.df)
end

# Allow the user to retrieve properties from the idf.df like `idf.df.col` via
# idf.col
function getproperty(idf::IndexedDataFrame, name::Symbol)

    # Not putting a uniqueness check here, because I think it would be
    # circular. (I was getting a StackOverflowError when I tried.)

    if name in fieldnames(IndexedDataFrame)
        return getfield(idf, name)
    else
        return getproperty(idf.df, name)
    end
end

# Allow the user to delete a row by specifying the idx_val
function deleteat!(idf::IndexedDataFrame, idx_val)
    row_i = find_row(idf, idx_val)
    deleteat!(idf.df, row_i)

    # Always good to recheck uniqueness in case the user manually modified the
    # underlying DataFrame
    check_uniqueness(idf.df, idf.index_col)

    return idf
end

# Allow the user to add a row to the IndexedDataFrame via a NamedTuple or Dict
function push!(idf::IndexedDataFrame, row::Union{NamedTuple, Dict})
    # The key for a NamedTuple must be a Symbol.
    idx_col_name = isa(row, NamedTuple) ? Symbol(idf.index_col) : idf.index_col
    index_val = row[idx_col_name]
    if index_val in idf.df[!, idf.index_col]
        throw(ArgumentError("Duplicate index value: $index_val"))
    end
    push!(idf.df, row)

    # Always good to recheck uniqueness in case the user manually modified the
    # underlying DataFrame
    check_uniqueness(idf.df, idf.index_col)

    return idf
end

# Allow the user to add a row to the IndexedDataFrame via a Vector
function push!(idf::IndexedDataFrame, row::Vector)
    if length(row) != DataFrames.ncol(idf.df)
        throw(ArgumentError("Row has wrong number of columns"))
    end
    index_val = row[idf.index_col_i]
    if index_val in idf.df[!, idf.index_col]
        throw(ArgumentError("Duplicate index value: $index_val"))
    end
    push!(idf.df, row)

    # Always good to recheck uniqueness in case the user manually modified the
    # underlying DataFrame
    check_uniqueness(idf.df, idf.index_col)

    return idf
end

# Allow the user to add a column
function setindex!(
            idf::IndexedDataFrame,
            col::AbstractVector,
            ::Colon,
            col_name::Union{String, Symbol}
        )
    if length(col) != DataFrames.nrow(idf.df)
        throw(ArgumentError("Column has the wrong number of rows."))
    end
    idf.df[:, col_name] = col

    # Always good to recheck uniqueness in case the user manually modified the
    # underlying DataFrame
    check_uniqueness(idf.df, idf.index_col)

    return nothing
end

end # module

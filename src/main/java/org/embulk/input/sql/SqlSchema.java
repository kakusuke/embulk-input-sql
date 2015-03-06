package org.embulk.input.sql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.List;

public class SqlSchema
{
    private List<SqlColumn> columns;

    @JsonCreator
    public SqlSchema(List<SqlColumn> columns)
    {
        this.columns = columns;
    }

    @JsonValue
    public List<SqlColumn> getColumns()
    {
        return columns;
    }

    public int getCount()
    {
        return columns.size();
    }

    public SqlColumn getColumn(int i)
    {
        return columns.get(i);
    }

    public String getColumnName(int i)
    {
        return columns.get(i).getName();
    }
}

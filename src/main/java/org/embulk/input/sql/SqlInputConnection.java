package org.embulk.input.sql;

import com.google.common.collect.ImmutableList;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.sql.*;

public class SqlInputConnection implements AutoCloseable {
    protected final Logger logger = Exec.getLogger(getClass());

    protected final Connection connection;
    protected final String schemaName;
    protected final DatabaseMetaData databaseMetaData;
    protected String identifierQuoteString;

    public SqlInputConnection(Connection connection, String schemaName) throws SQLException {
        this.connection = connection;
        this.schemaName = schemaName;
        this.databaseMetaData = connection.getMetaData();
        this.identifierQuoteString = databaseMetaData.getIdentifierQuoteString();
        if (schemaName != null) {
            setSearchPath(schemaName);
        }
        connection.setAutoCommit(false);
    }

    protected void setSearchPath(String schema) throws SQLException {
        String sql = "SET search_path TO " + quoteIdentifierString(schema);
        executeUpdate(sql);
    }

    public SqlSchema getSchemaOfQuery(String query) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(query);
        try {
            return getSchemaOfResultMetadata(stmt.getMetaData());
        } finally {
            stmt.close();
        }
    }

    protected SqlSchema getSchemaOfResultMetadata(ResultSetMetaData metadata) throws SQLException {
        ImmutableList.Builder<SqlColumn> columns = ImmutableList.builder();
        for (int i=0; i < metadata.getColumnCount(); i++) {
            int index = i + 1;  // JDBC column index begins from 1
            String name = metadata.getColumnName(index);
            String typeName = metadata.getColumnTypeName(index);
            int sqlType = metadata.getColumnType(index);
            //String scale = metadata.getScale(index)
            //String precision = metadata.getPrecision(index)
            columns.add(new SqlColumn(name, typeName, sqlType));
        }
        return new SqlSchema(columns.build());
    }

    public BatchSelect newSelectCursor(String query, int fetchRows) throws SQLException {
        logger.info("SQL: " + query);
        PreparedStatement stmt = connection.prepareStatement(query);
        stmt.setFetchSize(fetchRows);
        return new SingleSelect(stmt);
    }

    public interface BatchSelect extends AutoCloseable {
        public ResultSet fetch() throws SQLException;

        @Override
        public void close() throws SQLException;
    }

    public class SingleSelect implements BatchSelect {
        private final PreparedStatement fetchStatement;
        private boolean fetched = false;

        public SingleSelect(PreparedStatement fetchStatement) throws SQLException {
            this.fetchStatement = fetchStatement;
        }

        public ResultSet fetch() throws SQLException {
            if (fetched == true) {
                return null;
            }

            long startTime = System.currentTimeMillis();

            ResultSet rs = fetchStatement.executeQuery();

            double seconds = (System.currentTimeMillis() - startTime) / 1000.0;
            logger.info(String.format("> %.2f seconds", seconds));
            fetched = true;
            return rs;
        }

        public void close() throws SQLException {
            // TODO close?
        }
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }

    protected void executeUpdate(String sql) throws SQLException {
        logger.info("SQL: " + sql);
        Statement stmt = connection.createStatement();
        try {
            stmt.executeUpdate(sql);
        } finally {
            stmt.close();
        }
    }

    // TODO share code with embulk-output-sql
    protected String quoteIdentifierString(String str) {
        return identifierQuoteString + str + identifierQuoteString;
    }
}

package org.embulk.input;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.embulk.config.*;
import org.embulk.input.sql.SqlColumn;
import org.embulk.input.sql.SqlInputConnection;
import org.embulk.input.sql.SqlSchema;
import org.embulk.input.sql.getter.ColumnGetter;
import org.embulk.input.sql.getter.ColumnGetterFactory;
import org.embulk.spi.*;
import org.slf4j.Logger;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class SqlInputPlugin implements InputPlugin {
    protected final Logger logger = Exec.getLogger(getClass());

    public interface PluginTask extends Task {
        @Config("driver_path")
        @ConfigDefault("null")
        public Optional<String> getDriverPath();

        @Config("driver_class")
        public String getDriverClass();

        @Config("url")
        public String getUrl();

        @Config("user")
        @ConfigDefault("null")
        public Optional<String> getUser();

        @Config("password")
        @ConfigDefault("null")
        public Optional<String> getPassword();

        @Config("schema")
        @ConfigDefault("null")
        public Optional<String> getSchema();

        @Config("options")
        @ConfigDefault("{}")
        public Properties getOptions();

        @Config("query")
        public String getQuery();

        @Config("fetch_rows")
        @ConfigDefault("10000")
        // TODO set minimum number
        public int getFetchRows();

        // TODO parallel execution using "partition_by" config

        public SqlSchema getQuerySchema();
        public void setQuerySchema(SqlSchema schema);

        @ConfigInject
        public BufferAllocator getBufferAllocator();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, InputPlugin.Control control) {
        PluginTask task = config.loadConfig(getTaskClass());

        Schema schema;
        try (SqlInputConnection con = newConnection(task)) {
            schema = setupTask(con, task);
        } catch (SQLException ex) {
            throw Throwables.propagate(ex);
        }

        return buildNextConfigDiff(task, control.run(task.dump(), schema, 1));
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, Schema schema, int taskCount, InputPlugin.Control control) {
        PluginTask task = taskSource.loadTask(getTaskClass());
        return buildNextConfigDiff(task, control.run(taskSource, schema, taskCount));
    }

    @Override
    public ConfigDiff guess(ConfigSource config) {
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource, Schema schema, int taskCount, List<CommitReport> successCommitReports) {
        // do nothing
    }

    @Override
    public CommitReport run(TaskSource taskSource, Schema schema, int taskIndex, PageOutput output) {
        PluginTask task = taskSource.loadTask(getTaskClass());

        SqlSchema querySchema = task.getQuerySchema();
        BufferAllocator allocator = task.getBufferAllocator();
        PageBuilder pageBuilder = new PageBuilder(allocator, schema, output);

        try {
            List<ColumnGetter> getters = newColumnGetters(task, querySchema);

            try (SqlInputConnection con = newConnection(task);
                 SqlInputConnection.BatchSelect cursor = con.newSelectCursor(task.getQuery(), task.getFetchRows())) {
               while (true) {
                   // TODO run fetch() in another thread asynchronously
                   // TODO retry fetch() if it failed (maybe order_by is required and unique_column(s) option is also required)
                   boolean cont = fetch(cursor, getters, pageBuilder);
                   if (!cont) {
                       break;
                   }
               }
            }

        } catch (SQLException ex) {
            throw Throwables.propagate(ex);
        }
        pageBuilder.finish();

        CommitReport report = Exec.newCommitReport();
        return report;
    }

    protected Class<? extends PluginTask> getTaskClass() {
        return PluginTask.class;
    }

    protected SqlInputConnection newConnection(PluginTask task) throws SQLException {
        PluginTask t = task;

        if(t.getDriverPath().isPresent()) {
            ensureDriver(t.getDriverPath().get());
        }

        Properties props = new Properties();
        if (t.getUser().isPresent()) {
            props.setProperty("user", t.getUser().get());
        }
        if (t.getPassword().isPresent()) {
            props.setProperty("password", t.getPassword().get());
        }

        props.putAll(t.getOptions());

        Driver driver;
        try {
            // TODO check Class.forName(driverClass) is a Driver before newInstance
            //      for security
            driver = (Driver) Class.forName(t.getDriverClass()).newInstance();
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }

        Connection con = driver.connect(t.getUrl(), props);
        try {
            SqlInputConnection c = new SqlInputConnection(con, t.getSchema().orNull());
            con = null;
            return c;
        } finally {
            if (con != null) {
                con.close();
            }
        }
    }

    private final static Set<String> loadedJarGlobs = new HashSet<>();
    private void ensureDriver(String driverPath) {
        if (loadedJarGlobs.contains(driverPath)) return;
        synchronized (loadedJarGlobs) {
            if (loadedJarGlobs.contains(driverPath)) return;
            PluginClassLoader loader = (PluginClassLoader) getClass().getClassLoader();
            loader.addPath(Paths.get(driverPath));
            loadedJarGlobs.add(driverPath);
        }
    }

    private Schema setupTask(SqlInputConnection con, PluginTask task) throws SQLException {
        // build SELECT query and gets schema of its result
        SqlSchema querySchema = con.getSchemaOfQuery(task.getQuery());
        task.setQuerySchema(querySchema);

        ColumnGetterFactory factory = newColumnGetterFactory(task);
        ImmutableList.Builder<Column> columns = ImmutableList.builder();
        for (int i = 0; i < querySchema.getCount(); i++) {
            columns.add(new Column(i,
                    querySchema.getColumnName(i),
                    factory.newColumnGetter(querySchema.getColumn(i)).getToType()));
        }
        return new Schema(columns.build());
    }

    protected ColumnGetterFactory newColumnGetterFactory(PluginTask task) throws SQLException {
        return new ColumnGetterFactory();
    }

    protected ConfigDiff buildNextConfigDiff(PluginTask task, List<CommitReport> reports) {
        return Exec.newConfigDiff();
    }

    private List<ColumnGetter> newColumnGetters(PluginTask task, SqlSchema querySchema) throws SQLException {
        ColumnGetterFactory factory = newColumnGetterFactory(task);
        ImmutableList.Builder<ColumnGetter> getters = ImmutableList.builder();
        for (SqlColumn c : querySchema.getColumns()) {
            getters.add(factory.newColumnGetter(c));
        }
        return getters.build();
    }

    private boolean fetch(SqlInputConnection.BatchSelect cursor, List<ColumnGetter> getters, PageBuilder pageBuilder) throws SQLException {
        ResultSet result = cursor.fetch();
        if (result == null || !result.next()) {
            return false;
        }

        List<Column> columns = pageBuilder.getSchema().getColumns();
        long rows = 0;
        long reportRows = 500;
        do {
            for (int i=0; i < getters.size(); i++) {
                int index = i + 1;  // JDBC column index begins from 1
                getters.get(i).getAndSet(result, index, pageBuilder, columns.get(i));
            }
            pageBuilder.addRecord();
            rows++;
            if (rows % reportRows == 0) {
                logger.info(String.format("Fetched %,d rows.", rows));
                reportRows *= 2;
            }
        } while (result.next());
        return true;
    }
}

package org.embulk.input;

import java.util.List;
import org.embulk.config.CommitReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;

public class SqlInputPlugin
        implements InputPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("property1")
        public String getProperty1();

        @Config("property2")
        @ConfigDefault("0")
        public int getProperty2();

        // TODO get schema from config or data source
        @Config("columns")
        public SchemaConfig getColumns();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config,
            InputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema schema = task.getColumns().toSchema();
        int taskCount = 1;  // number of run() method calls

        return resume(task.dump(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            InputPlugin.Control control)
    {
        control.run(taskSource, schema, taskCount);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<CommitReport> successCommitReports)
    {
    }

    @Override
    public CommitReport run(TaskSource taskSource,
            Schema schema, int taskIndex,
            PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        // TODO
        throw new UnsupportedOperationException("The 'run' method needs to be implemented");
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        // TODO
        throw new UnsupportedOperationException("'sql' input plugin does not support guessing.");
        //ConfigDiff diff = Exec.newConfigDiff();
        //diff.set("property1", "value");
        //return diff;
    }
}

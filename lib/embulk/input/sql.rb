Embulk::JavaPlugin.register_input(
  "sql", "org.embulk.input.SqlInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))

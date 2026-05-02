using Projects;

var builder = DistributedApplication.CreateBuilder(args);

var storeProvider = builder.Configuration["Surefire:Store"];
var notificationsProvider = builder.Configuration["Surefire:Notifications"];

var sample = builder.AddProject<Surefire_Sample>("surefire-sample")
    .WithEnvironment("Surefire__Store", storeProvider)
    .WithEnvironment("Surefire__Notifications", notificationsProvider)
    .WithReplicas(2);

switch (storeProvider)
{
    case "postgres":
    {
        var store = builder.AddPostgres("postgres-store")
            .WithPgAdmin()
            .AddDatabase("store");
        sample.WithReference(store).WaitFor(store);
        break;
    }
    case "sqlserver":
    {
        var store = builder.AddSqlServer("sqlserver")
            .AddDatabase("store")
            .WithCreationScript("""
                                IF DB_ID('store') IS NULL
                                    CREATE DATABASE [store];
                                ALTER DATABASE [store] SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE;
                                """);
        sample.WithReference(store).WaitFor(store);
        break;
    }
    case "redis":
    {
        var store = builder.AddRedis("store")
            .WithRedisInsight();
        sample.WithReference(store).WaitFor(store);
        break;
    }
    case "sqlite":
    {
        var store = builder.AddSqlite("store");
        sample.WithReference(store).WaitFor(store);
        break;
    }
}

switch (notificationsProvider)
{
    case "postgres":
    {
        var notifications = builder.AddPostgres("postgres-notifications")
            .WithPgAdmin()
            .AddDatabase("notifications");
        sample.WithReference(notifications).WaitFor(notifications);
        break;
    }
    case "redis":
    {
        var notifications = builder.AddRedis("notifications")
            .WithRedisInsight();
        sample.WithReference(notifications).WaitFor(notifications);
        break;
    }
}

builder.Build().Run();

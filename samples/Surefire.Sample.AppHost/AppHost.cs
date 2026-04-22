using Projects;

var builder = DistributedApplication.CreateBuilder(args);

var surefire = builder.AddPostgres("postgres")
    .WithPgAdmin()
    .AddDatabase("surefire");

//var surefire = builder.AddSqlServer("sqlserver")
//    .AddDatabase("surefire")
//    .WithCreationScript("""
//                        IF DB_ID('surefire') IS NULL
//                            CREATE DATABASE [surefire];
//                        ALTER DATABASE [surefire] SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE;
//                        """);

//var surefire = builder.AddRedis("surefire")
//    .WithRedisInsight();

builder.AddProject<Surefire_Sample>("surefire-sample")
    .WithReference(surefire)
    .WaitFor(surefire)
    .WithReplicas(2);

builder.Build().Run();
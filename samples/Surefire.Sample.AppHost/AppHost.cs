using Projects;

var builder = DistributedApplication.CreateBuilder(args);

var surefire = builder.AddPostgres("postgres")
    .WithPgAdmin()
    .AddDatabase("surefire");

//var surefire = builder.AddSqlServer("sqlserver")
//    .AddDatabase("surefire");

//var surefire = builder.AddRedis("surefire")
//    .WithRedisInsight();

builder.AddProject<Surefire_Sample>("surefire-sample")
    .WithReference(surefire)
    .WaitFor(surefire);

builder.Build().Run();
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DockerRunner;
using DockerRunner.Xunit;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Migration.Strategies;
using Hangfire.Mongo.Migration.Strategies.Backup;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.Mongo.Tests.Utils;

public class MongoDbFixture : IAsyncLifetime
{
    private const string DefaultDatabaseName = @"Hangfire-Mongo-Tests";

    private readonly DockerContainerFixture<MongoDbContainerConfiguration> _containerFixture;

    public MongoDbFixture(IMessageSink messageSink) => _containerFixture = new DockerContainerFixture<MongoDbContainerConfiguration>(messageSink);

    async Task IAsyncLifetime.InitializeAsync() => await ((IAsyncLifetime)_containerFixture).InitializeAsync();

    async Task IAsyncLifetime.DisposeAsync() => await ((IAsyncLifetime)_containerFixture).DisposeAsync();

    public MongoStorage CreateStorage(string databaseName = null)
    {
        var storageOptions = new MongoStorageOptions
        {
            MigrationOptions = new MongoMigrationOptions
            {
                MigrationStrategy = new DropMongoMigrationStrategy(),
                BackupStrategy = new NoneMongoBackupStrategy()
            }
        };
        return CreateStorage(storageOptions, databaseName);
    }

    public MongoStorage CreateStorage(MongoStorageOptions storageOptions, string databaseName=null)
    {
        var client = GetMongoClient();
        return new MongoStorage(client, databaseName ?? DefaultDatabaseName, storageOptions);
    }

    public HangfireDbContext CreateDbContext(string dbName = null)
    {
        var client = GetMongoClient();
        return new HangfireDbContext(client, dbName ?? DefaultDatabaseName);
    }

    public void CleanDatabase(string dbName = null)
    {
        try
        {
            var context = CreateDbContext(dbName);
            context.DistributedLock.DeleteMany(new BsonDocument());
            context.JobGraph.DeleteMany(new BsonDocument());
            context.Server.DeleteMany(new BsonDocument());
            context.Database.DropCollection(context.Notifications.CollectionNamespace.CollectionName);
        }
        catch (MongoException ex)
        {
            throw new InvalidOperationException("Unable to cleanup database.", ex);
        }
    }

    private MongoClient GetMongoClient()
    {
        var hostEndpoint = _containerFixture.ContainerInfo.PortMappings.First(e => e.ContainerPort == 27017).HostEndpoint;
        var connectionString = $"mongodb://{hostEndpoint.Address}:{hostEndpoint.Port}";
        var settings = MongoClientSettings.FromConnectionString(connectionString);
        return new MongoClient(settings);
    }

    private class MongoDbContainerConfiguration : DockerContainerConfiguration
    {
        public override string ImageName => "bitnami/mongodb:5.0";
        public override string ContainerName => "Hangfire.Mongo.Tests";
        public override IReadOnlyDictionary<string, string> EnvironmentVariables { get; } = new Dictionary<string, string> { ["ALLOW_EMPTY_PASSWORD"] = "yes" };
    }
}
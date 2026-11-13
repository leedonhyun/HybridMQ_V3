namespace HybridMQ.Hosting;

using HybridMQ.Client.Abstractions;
using HybridMQ.Domain.Ports;
using HybridMQ.Engine.Memory;
using HybridMQ.Engine.Pipelines;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

public static class HybridMqServiceCollectionExtensions
{
    public static IServiceCollection AddHybridMq(
        this IServiceCollection services,
        Action<HybridMqOptions>? configure = null)
    {
        var opts = new HybridMqOptions();
        configure?.Invoke(opts);

        services.AddSingleton(opts);
        services.AddSingleton<IStorageProvider>(_ => HybridMqFactory.CreateStorageProvider(opts));
        services.AddSingleton(sp => new MemoryBudgetManager(opts.Profile, opts.OverrideMaxMemoryMb));
        services.AddSingleton(sp => new BrokerEngine(
            opts.NodeId,
            sp.GetRequiredService<IStorageProvider>(),
            sp.GetRequiredService<MemoryBudgetManager>()));

        services.AddSingleton<HybridMqRuntimeHost>(sp =>
            HybridMqFactory.StartFullRuntimeAsync(o =>
            {
                o.StorageType = opts.StorageType;
                o.DataDirectory = opts.DataDirectory;
                o.ConnectionString = opts.ConnectionString;
                o.DatabaseOrSchemaName = opts.DatabaseOrSchemaName;
                o.TransportType = opts.TransportType;
                o.LocalPipePrefix = opts.LocalPipePrefix;
                o.TcpListenEndPoint = opts.TcpListenEndPoint;
                o.RemoteBrokerEndPoint = opts.RemoteBrokerEndPoint;
                o.Profile = opts.Profile;
                o.OverrideMaxMemoryMb = opts.OverrideMaxMemoryMb;
                o.NodeId = opts.NodeId;
            }).AsTask().GetAwaiter().GetResult());

        services.AddSingleton<IHybridMqInstance>(sp => sp.GetRequiredService<HybridMqRuntimeHost>().Client);
        services.AddSingleton(sp => sp.GetRequiredService<IHybridMqInstance>().Producer);
        services.AddSingleton(sp => sp.GetRequiredService<IHybridMqInstance>().Consumer);
        services.AddSingleton(sp => sp.GetRequiredService<IHybridMqInstance>().Admin);

        services.AddHostedService<HybridMqLifecycleHostedService>();
        return services;
    }

    private sealed class HybridMqLifecycleHostedService(HybridMqRuntimeHost host) : IHostedService
    {
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public async Task StopAsync(CancellationToken cancellationToken)
            => await host.DisposeAsync().ConfigureAwait(false);
    }
}

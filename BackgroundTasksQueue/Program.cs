using System;
using System.IO;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Hosting;
using CachingFramework.Redis;
using CachingFramework.Redis.Contracts.Providers;
using StackExchange.Redis;
using Microsoft.Extensions.Configuration;
using BackgroundTasksQueue.Services;
using BackgroundTasksQueue.Library.Services;

namespace BackgroundTasksQueue
{
    public class Program
    {
        public static void Main(string[] args)
        {
            //CreateHostBuilder(args).Build().Run();

            var host = CreateHostBuilder(args).Build();

            var monitorLoop = host.Services.GetRequiredService<MonitorLoop>();
            monitorLoop.StartMonitorLoop();

            host.Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
            .UseContentRoot(Directory.GetCurrentDirectory())
            .ConfigureAppConfiguration((hostContext, config) =>
            {
                var env = hostContext.HostingEnvironment;

                // find the shared folder in the parent folder
                string[] paths = { env.ContentRootPath, "..", "..", "SharedSettings" };
                var sharedFolder = Path.Combine(paths);

                //load the SharedSettings first, so that appsettings.json overrwrites it
                config
                    .AddJsonFile(Path.Combine(sharedFolder, "sharedSettings.json"), optional: true)
                    .AddJsonFile("appsettings.json", optional: true)
                    .AddJsonFile($"appsettings.{env.EnvironmentName}.json", optional: true);

                config.AddEnvironmentVariables();
            })
            .ConfigureLogging((ctx, log) => { /* elided for brevity */ })
            .UseDefaultServiceProvider((ctx, opts) => { /* elided for brevity */ })
            .ConfigureServices((hostContext, services) =>
            {
                try
                {
                    ConnectionMultiplexer muxer = ConnectionMultiplexer.Connect("localhost");
                    services.AddSingleton<ICacheProviderAsync>(new RedisContext(muxer).Cache);
                    services.AddSingleton<IPubSubProvider>(new RedisContext(muxer).PubSub);
                    services.AddSingleton<IKeyEventsProvider>(new RedisContext(muxer).KeyEvents);
                }
                catch (Exception ex)
                {
                    string message = ex.Message;
                    Console.WriteLine($"\n\n Redis server did not start: \n + {message} \n");
                    throw;
                }

                services.AddSingleton<GenerateThisInstanceGuidService>();
                services.AddSingleton<ISharedDataAccess, SharedDataAccess>();
                services.AddHostedService<QueuedHostedService>();
                services.AddSingleton<IBackgroundTaskQueue, BackgroundTaskQueue>();
                services.AddSingleton<MonitorLoop>();
                services.AddSingleton<IBackgroundTasksService, BackgroundTasksService>();
                services.AddSingleton<IOnKeysEventsSubscribeService, OnKeysEventsSubscribeService>();
                services.AddSingleton<ITasksPackageCaptureService, TasksPackageCaptureService>();
                services.AddSingleton<ITasksBatchProcessingService, TasksBatchProcessingService>();
                });
    }    

    //public static class ThisBackServerGuid
    //{
    //    static ThisBackServerGuid()
    //    {
    //        thisBackServerGuid = Guid.NewGuid().ToString();
    //    }

    //    private static readonly string thisBackServerGuid;

    //    public static string GetThisBackServerGuid()
    //    {
    //        return thisBackServerGuid;
    //    }
    //}
}

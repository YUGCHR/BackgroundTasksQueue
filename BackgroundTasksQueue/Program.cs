using System;
using BackgroundTasksQueue.Services;
using CachingFramework.Redis;
using CachingFramework.Redis.Contracts.Providers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using StackExchange.Redis;

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

                    services.AddSingleton<GenerateThisBackServerGuid>();
                    services.AddSingleton<ISettingConstants, SettingConstants>();
                    services.AddHostedService<QueuedHostedService>();
                    services.AddSingleton<IBackgroundTaskQueue, BackgroundTaskQueue>();
                    services.AddSingleton<MonitorLoop>();
                    services.AddSingleton<IBackgroundTasksService, BackgroundTasksService>();
                    services.AddSingleton<IOnKeysEventsSubscribeService, OnKeysEventsSubscribeService>();
                });
    }

    // вставить генерацию уникального номера в сервис констант
    public class GenerateThisBackServerGuid
    {
        private readonly string _thisBackServerGuid;

        public GenerateThisBackServerGuid()
        {
            _thisBackServerGuid = Guid.NewGuid().ToString();
        }        

        public string ThisBackServerGuid()
        {
            return _thisBackServerGuid;
        }
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

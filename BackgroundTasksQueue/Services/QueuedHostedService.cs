using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CachingFramework.Redis.Contracts;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using CachingFramework.Redis.Contracts.Providers;
using BackgroundTasksQueue.Models;

namespace BackgroundTasksQueue.Services
{
    public class QueuedHostedService : BackgroundService
    {
        private readonly ILogger<QueuedHostedService> _logger;
        private readonly ISettingConstants _constant;
        private readonly ICacheProviderAsync _cache;
        private readonly IKeyEventsProvider _keyEvents;
        private readonly string _guid;

        List<BackgroundProcessingTask> tasks = new List<BackgroundProcessingTask>();

        public QueuedHostedService(
            GenerateThisBackServerGuid thisGuid,
            IBackgroundTaskQueue taskQueue,
            ILogger<QueuedHostedService> logger,
            ISettingConstants constant,
            ICacheProviderAsync cache, 
            IKeyEventsProvider keyEvents)
        {
            TaskQueue = taskQueue;
            _logger = logger;
            _constant = constant;
            _cache = cache;
            _keyEvents = keyEvents;

            _guid = thisGuid.ThisBackServerGuid();
        }

        public IBackgroundTaskQueue TaskQueue { get; }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation($"Queued Hosted Service is running.{Environment.NewLine}" +
                                   $"{Environment.NewLine}Tap W to add a work item to the " +
                                   $"background queue.{Environment.NewLine}");

            await BackgroundProcessing(stoppingToken);
        }

        private async Task BackgroundProcessing(CancellationToken stoppingToken)
        {
            int tasksCount = 0;

            //await _cache.SetObjectAsync("tasksCount", 3);

            string eventKey = "task:add";
            string cancelKey = "task:del";

            string backServerGuid = $"{_constant.GetPrefixBackServer}:{_guid}"; // Guid.NewGuid()
            _logger.LogInformation(1101, "INIT No: {0} - guid of This Server was fetched in QueuedHostedService.", backServerGuid);

            // подписка на ключ добавления бэкграунд процессов (поле без разницы), в значении можно было бы ставить количество необходимых процессов
            _keyEvents.Subscribe(eventKey, (string key, KeyEvent cmd) =>
            {
                if (cmd == KeyEvent.HashSet)
                {
                    _logger.LogInformation(1111, "Received key {0} with command {1}", eventKey, cmd);

                    string guid = Guid.NewGuid().ToString();
                    CancellationTokenSource newCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                    CancellationToken newToken = newCts.Token;

                    tasks.Add(new BackgroundProcessingTask()
                    {
                        TaskId = tasksCount + 1,
                        ProcessingTaskId = guid,
                        // запускаем новый процесс
                        ProcessingTask = Task.Run(() => ProcessingTaskMethod(newToken), newToken),
                        CancellationTaskToken = newCts
                    });
                    tasksCount++;

                    //tasks.Add(Task.Run(() => ProcessingTaskMethod(newToken), newToken));
                    _logger.LogInformation(1211, "New Task for Background Processes was added, total count became {Count}", tasksCount);
                }
            });

            string eventKeyCommand = $"Key {eventKey}, HashSet command";
            _logger.LogInformation(1311, "You subscribed on event - {EventKey}.", eventKeyCommand);

            _keyEvents.Subscribe(cancelKey, (string key, KeyEvent cmd) =>
            {
                if (cmd == KeyEvent.HashSet)
                {
                    _logger.LogInformation("key {0} - command {1}", key, cmd);
                    if (tasksCount > 0)
                    {
                        // останавливаем процесс
                        var cts = tasks[tasksCount - 1].CancellationTaskToken;
                        cts.Cancel();

                        tasks.RemoveAt(tasksCount - 1);
                        tasksCount--;
                        _logger.LogInformation("One Task for Background Processes was removed, total count left {Count}", tasksCount);
                    }
                    else
                    {
                        _logger.LogInformation("Task for Background Processes cannot be removed for some reason, total count is {Count}", tasksCount);
                    }
                }
            });

            List<Task> processingTask = tasks.Select(t => t.ProcessingTask).ToList();

            await Task.WhenAll(processingTask);

            _logger.LogInformation("All Background Processes were finished, total count was {Count}", processingTask.Count);
        }

        private async Task ProcessingTaskMethod(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                var workItem = await TaskQueue.DequeueAsync(token);

                try
                {
                    await workItem(token);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error occurred executing {WorkItem}.", nameof(workItem));
                }
            }
        }

        public override async Task StopAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Queued Hosted Service is stopping.");


            await base.StopAsync(stoppingToken);
        }
    }
}


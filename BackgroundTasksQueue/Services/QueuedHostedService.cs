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
using BackgroundTasksQueue.Library.Services;
using BackgroundTasksQueue.Models;
using BackgroundTasksQueue.Library.Models;

namespace BackgroundTasksQueue.Services
{
    public class QueuedHostedService : BackgroundService
    {
        private readonly ILogger<QueuedHostedService> _logger;
        private readonly ISharedDataAccess _data;
        private readonly ICacheProviderAsync _cache;
        private readonly IKeyEventsProvider _keyEvents;
        private readonly string _guid;

        List<BackgroundProcessingTask> completingTasksProcesses = new List<BackgroundProcessingTask>();

        public QueuedHostedService(
            GenerateThisInstanceGuidService thisGuid,
            IBackgroundTaskQueue taskQueue,
            ILogger<QueuedHostedService> logger,
            ISharedDataAccess data,
            ICacheProviderAsync cache,
            IKeyEventsProvider keyEvents)
        {
            TaskQueue = taskQueue;
            _logger = logger;
            _data = data;
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
            EventKeyNames eventKeysSet = await _data.FetchAllConstants();
            //string backServerGuid = _guid ?? throw new ArgumentNullException(nameof(_guid));
            //eventKeysSet.BackServerGuid = backServerGuid;
            //string backServerPrefixGuid = $"{eventKeysSet.PrefixBackServer}:{backServerGuid}";
            //eventKeysSet.BackServerPrefixGuid = backServerPrefixGuid;

            //string eventKey = "task:add";
            string cancelKey = "task:del";
            int createdProcessesCount = 0;
            string backServerGuid = $"{eventKeysSet.PrefixBackServer}:{_guid}"; // backserver:(this server guid)
            _logger.LogInformation(1101, "INIT No: {0} - guid of This Server was fetched in QueuedHostedService.", backServerGuid);
            // создать ключ для подписки из констант
            string prefixProcessAdd = eventKeysSet.PrefixProcessAdd; // process:add
            string eventKeyProcessAdd = $"{prefixProcessAdd}:{_guid}"; // process:add:(this server guid)
            // поле-пустышка, но одинаковое с тем, что создаётся в основном методе - чтобы достать значение
            string eventFieldBack = eventKeysSet.EventFieldBack;
            _logger.LogInformation(1103, "Processes creation on This Server was subscribed on key {0} / field {1}.", eventKeyProcessAdd, eventFieldBack);
            // подписка на ключ добавления бэкграунд процессов(поле без разницы), в значении можно было бы ставить количество необходимых процессов
            // типовая блокировка множественной подписки до специального разрешения повторной подписки
            bool flagToBlockEventAdd = true;
            _keyEvents.Subscribe(eventKeyProcessAdd, async (string key, KeyEvent cmd) =>
            {
                if (cmd == KeyEvent.HashSet && flagToBlockEventAdd)
                {
                    // временная защёлка, чтобы подписка выполнялась один раз
                    flagToBlockEventAdd = false;
                    _logger.LogInformation(1111, "Received key {0} with command {1}", eventKeyProcessAdd, cmd);
                    // название поля тоже можно создать здесь и передать в метод
                    // ещё лучше - достать нужное значение заранее и передать только его, тогда метод будет синхронный (наверное)
                    // не лучше
                    // лучше
                    int requiredProcessesCount = await _cache.GetHashedAsync<int>(eventKeyProcessAdd, eventFieldBack);
                    if (requiredProcessesCount > 0)
                    {
                        createdProcessesCount = await AddProcessesToPerformingTasks(stoppingToken, requiredProcessesCount);
                        _logger.LogInformation(1131, "AddProcessesToPerformingTasks created processes count {0}", createdProcessesCount);

                        if (createdProcessesCount > 0)
                        {
                            flagToBlockEventAdd = true;
                        }
                    }
                    // если вызвали с неправильным значением в ключе, подписка навсегда останется заблокированной, где-то тут ее надо разблокировать
                }
            });

            string eventKeyCommand = $"Key {eventKeyProcessAdd}, HashSet command";
            _logger.LogInformation(1311, "You subscribed on event - {EventKey}.", eventKeyCommand);

            _keyEvents.Subscribe(cancelKey, (string key, KeyEvent cmd) =>
            {
                if (cmd == KeyEvent.HashSet)
                {
                    _logger.LogInformation("key {0} - command {1}", key, cmd);
                    if (createdProcessesCount > 0)
                    {
                        // останавливаем процесс
                        var cts = completingTasksProcesses[createdProcessesCount - 1].CancellationTaskToken;
                        cts.Cancel();

                        completingTasksProcesses.RemoveAt(createdProcessesCount - 1);
                        createdProcessesCount--;
                        _logger.LogInformation("One Task for Background Processes was removed, total count left {Count}", createdProcessesCount);
                    }
                    else
                    {
                        _logger.LogInformation("Task for Background Processes cannot be removed for some reason, total count is {Count}", createdProcessesCount);
                    }
                }
            });

            List<Task> processingTask = completingTasksProcesses.Select(t => t.ProcessingTask).ToList();

            await Task.WhenAll(processingTask);

            _logger.LogInformation("All Background Processes were finished, total count was {Count}", processingTask.Count);
        }

        private async Task<int> AddProcessesToPerformingTasks(CancellationToken stoppingToken, int requiredProcessesCount)
        {
            // requiredProcessesCount - требуемое количество процессов, начать цикл их создания
            int tasksCount = 0;
            // тут можно предусмотреть максимальное количество процессов - не из константы, а по месту
            while (tasksCount < requiredProcessesCount && tasksCount < 100)
            {
                string guid = Guid.NewGuid().ToString();
                CancellationTokenSource newCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                CancellationToken newToken = newCts.Token;
                _logger.LogInformation(1231, "AddProcessesToPerformingTasks creates process No {0}", tasksCount);

                // глобальный List, доступный во всем классе
                completingTasksProcesses.Add(new BackgroundProcessingTask()
                {
                    TaskId = tasksCount + 1,
                    ProcessingTaskId = guid,
                    // запускаем новый процесс
                    ProcessingTask = Task.Run(() => ProcessingTaskMethod(newToken), newToken),
                    CancellationTaskToken = newCts
                });
                tasksCount++;
                // что-то куда-то записать - количество созданных процессов?

            }
            _logger.LogInformation(1231, "New Task for Background Processes was added, total count became {Count}", tasksCount);
            // кроме true, надо вернуть tasksCount - можно возвращать int, а если он больше нуля, то ставить flagToBlockEventAdd в true
            return tasksCount;
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


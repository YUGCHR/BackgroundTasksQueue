using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CachingFramework.Redis.Contracts.Providers;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace BackgroundTasksQueue.Services
{
    public interface IBackgroundTasksService
    {
        void StartWorkItem(string serverNum, string guid, int loopCount);

    }

    public class BackgroundTasksService : IBackgroundTasksService
    {
        private readonly IBackgroundTaskQueue _taskQueue;
        private readonly ILogger<BackgroundTasksService> _logger;
        private readonly ICacheProviderAsync _cache;

        public BackgroundTasksService(
            IBackgroundTaskQueue taskQueue,
            ILogger<BackgroundTasksService> logger,
            ICacheProviderAsync cache
        )
        {
            _taskQueue = taskQueue;
            _logger = logger;
            _cache = cache;
        }

        public void StartWorkItem(string serverNum, string guid, int loopCount)
        {
            // Enqueue a background work item
            _taskQueue.QueueBackgroundWorkItem(async token =>
            {
                // Simulate loopCount 3-second tasks to complete for each enqueued work item

                int delayLoop = 0;
                int loopRemain = loopCount;
                //var guid = Guid.NewGuid().ToString();

                _logger.LogInformation(2101, "Queued Background Task {Guid} is starting.", guid);

                while (!token.IsCancellationRequested && delayLoop < loopCount)
                {
                    try
                    {
                        await Task.Delay(TimeSpan.FromSeconds(3), token);
                    }
                    catch (OperationCanceledException)
                    {
                        // Prevent throwing if the Delay is cancelled
                    }
                    loopRemain--;
                    await _cache.SetHashedAsync(serverNum, guid, loopRemain); // обновляем отчёт о прогрессе выполнения задания
                    delayLoop++;

                    _logger.LogInformation("Queued Background Task {Guid} is running. current Loop = {DelayLoop} / Loop remaining = {3}", guid, delayLoop, loopRemain);
                }

                if (delayLoop == loopCount)
                {
                    bool isDeletedSuccess = await _cache.RemoveHashedAsync(serverNum, guid); //HashExistsAsync
                    _logger.LogInformation("Queued Background Task {Guid} is complete on Server No. {ServerNum} / isDeleteSuccess = {3}.", guid, serverNum, isDeletedSuccess);
                    //int checkDeletedSuccess = await _cache.GetHashedAsync<int>(serverNum, guid); // проверку и сообщение о нём можно убрать после отладки
                    //_logger.LogInformation("Deleted field {Guid} checked on Server No. {ServerNum} / value = {3}.", guid, serverNum, checkDeletedSuccess);
                }
                else
                {
                    bool isDeletedSuccess = await _cache.RemoveHashedAsync(serverNum, guid);
                    _logger.LogInformation("Queued Background Task {Guid} was cancelled on Server No. {ServerNum} / isDeleteSuccess = {3}.", guid, serverNum, isDeletedSuccess);
                    // записать какой-то ключ насчёт неудачи и какую-то информацию о процессе?
                    int checkDeletedSuccess = await _cache.GetHashedAsync<int>(serverNum, guid);
                }
            });
        }
    }
}

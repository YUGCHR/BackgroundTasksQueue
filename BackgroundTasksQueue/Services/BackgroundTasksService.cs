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
        void StartWorkItem(string backServerPrefixGuid, string tasksPakageGuidValue, string singleTaskGuid, int assignmentTerms);

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

        public void StartWorkItem(string backServerPrefixGuid, string tasksPakageGuidField, string singleTaskGuid, int assignmentTerms)
        {
            // Enqueue a background work item
            _taskQueue.QueueBackgroundWorkItem(async token =>
            {
                // Simulate loopCount 3-second tasks to complete for each enqueued work item

                int delayLoop = 0;
                int loopRemain = assignmentTerms;
                //var guid = Guid.NewGuid().ToString();

                _logger.LogInformation(2101, "Queued Background Task {Guid} is starting.", singleTaskGuid);

                while (!token.IsCancellationRequested && delayLoop < assignmentTerms)
                {
                    try
                    {
                        await Task.Delay(TimeSpan.FromSeconds(3), token);
                    }
                    catch (OperationCanceledException)
                    {
                        // Prevent throwing if the Delay is cancelled
                    }
                    // здесь записать в ключ ??? и поле ??? номер текущего цикла и всего циклов, а также время и так далее (потом)
                    // рассмотреть два варианта - ключ - сервер, поле - пакет, а в значении указать номер конкретной задачи и прочее в модели
                    // второй вариант - ключ - пакет, поле - задача, а в значении сразу проценты (int)
                    // ключ - сервер не имеет большого смысла, пакет и так не потеряется, а искать его будут именно по номеру пакета, поэтому пока второй вариант
                    loopRemain--;
                    int multiplier = 10000;
                    int completionTaskPercentage = (delayLoop * multiplier / assignmentTerms) / multiplier;
                    _logger.LogInformation("completionTaskPercentage {0} = delayLoop {1} / assignmentTerms {2}", completionTaskPercentage, delayLoop, assignmentTerms);
                    // обновляем отчёт о прогрессе выполнения задания
                    await _cache.SetHashedAsync(tasksPakageGuidField, singleTaskGuid, completionTaskPercentage); // TimeSpan.FromDays - !!!
                    delayLoop++;                    
                    _logger.LogInformation("Queued Background Task {Guid} is running. Current Loop = {DelayLoop} / Loop remaining = {3}", singleTaskGuid, delayLoop, loopRemain);
                }

                if (delayLoop == assignmentTerms)
                {
                    bool isDeletedSuccess = await _cache.RemoveHashedAsync(backServerPrefixGuid, singleTaskGuid); //HashExistsAsync
                    _logger.LogInformation("Queued Background Task {Guid} is complete on Server No. {ServerNum} / isDeleteSuccess = {3}.", singleTaskGuid, backServerPrefixGuid, isDeletedSuccess);
                    //int checkDeletedSuccess = await _cache.GetHashedAsync<int>(serverNum, guid); // проверку и сообщение о нём можно убрать после отладки
                    //_logger.LogInformation("Deleted field {Guid} checked on Server No. {ServerNum} / value = {3}.", guid, serverNum, checkDeletedSuccess);
                }
                else
                {
                    bool isDeletedSuccess = await _cache.RemoveHashedAsync(backServerPrefixGuid, singleTaskGuid);
                    _logger.LogInformation("Queued Background Task {Guid} was cancelled on Server No. {ServerNum} / isDeleteSuccess = {3}.", singleTaskGuid, backServerPrefixGuid, isDeletedSuccess);
                    // записать какой-то ключ насчёт неудачи и какую-то информацию о процессе?
                    int checkDeletedSuccess = await _cache.GetHashedAsync<int>(backServerPrefixGuid, singleTaskGuid);
                }
            });
        }
    }
}

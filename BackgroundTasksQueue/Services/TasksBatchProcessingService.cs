using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CachingFramework.Redis.Contracts.Providers;
using Microsoft.Extensions.Logging;
using BackgroundTasksQueue.Library.Models;

namespace BackgroundTasksQueue.Services
{
    public interface ITasksBatchProcessingService
    {
        public Task<bool> CheckingAllTasksCompletion(EventKeyNames eventKeysSet);
    }

    public class TasksBatchProcessingService : ITasksBatchProcessingService
    {
        private readonly IBackgroundTasksService _task2Queue;
        private readonly ILogger<TasksBatchProcessingService> _logger;
        private readonly ICacheProviderAsync _cache;
        private readonly IKeyEventsProvider _keyEvents;

        public TasksBatchProcessingService(
            ILogger<TasksBatchProcessingService> logger,
            ICacheProviderAsync cache,
            IKeyEventsProvider keyEvents,
            IBackgroundTasksService task2Queue)
        {
            _task2Queue = task2Queue;
            _logger = logger;
            _cache = cache;
            _keyEvents = keyEvents;
        }

        public async Task<bool> CheckingAllTasksCompletion(EventKeyNames eventKeysSet) // Main for Check
        {
            // ----------------- вы находитесь здесь


            // подписку оформить в отдельном методе, а этот вызывать оттуда
            // можно ставить блокировку на подписку и не отвлекаться на события, пока не закончена очередная проверка

            return default;
        }
    }
}

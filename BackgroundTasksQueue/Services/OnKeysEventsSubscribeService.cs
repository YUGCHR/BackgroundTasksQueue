using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BackgroundTasksQueue.Models;
using CachingFramework.Redis.Contracts;
using CachingFramework.Redis.Contracts.Providers;
using Microsoft.Extensions.Logging;

namespace BackgroundTasksQueue.Services
{
    public interface IOnKeysEventsSubscribeService
    {
        public Task<string> FetchGuidFieldTaskRun(string eventKeyRun, string eventFieldRun);
        public void SubscribeOnEventRun(EventKeyNames eventKeysSet);
        public void SubscribeOnEventCheck(EventKeyNames eventKeysSet, string guidField);
    }

    public class OnKeysEventsSubscribeService : IOnKeysEventsSubscribeService
    {
        private readonly IBackgroundTasksService _task2Queue;
        private readonly ILogger<OnKeysEventsSubscribeService> _logger;
        private readonly ICacheProviderAsync _cache;
        private readonly IKeyEventsProvider _keyEvents;
        private readonly ITasksPackageCaptureService _captures;
        private readonly ITasksBatchProcessingService _processing;

        public OnKeysEventsSubscribeService(
            ILogger<OnKeysEventsSubscribeService> logger,
            ICacheProviderAsync cache,
            IKeyEventsProvider keyEvents,
            IBackgroundTasksService task2Queue,
            ITasksPackageCaptureService captures,
            ITasksBatchProcessingService processing)
        {
            _task2Queue = task2Queue;
            _logger = logger;
            _cache = cache;
            _keyEvents = keyEvents;
            _captures = captures;
            _processing = processing;
        }

        public async Task<string> FetchGuidFieldTaskRun(string eventKeyRun, string eventFieldRun) // not used
        {
            string eventGuidFieldRun = await _cache.GetHashedAsync<string>(eventKeyRun, eventFieldRun); //получить guid поле для "task:run"

            return eventGuidFieldRun;
        }

        // подписываемся на ключ сообщения о появлении свободных задач
        public void SubscribeOnEventRun(EventKeyNames eventKeysSet)
        {
            string eventKeyFrontGivesTask = eventKeysSet.EventKeyFrontGivesTask;
            _logger.LogInformation(201, "This BackServer subscribed on key {0}.", eventKeyFrontGivesTask);

            // типовая блокировка множественной подписки до специального разрешения повторной подписки
            bool flagToBlockEventRun = true;

            _keyEvents.Subscribe(eventKeyFrontGivesTask, async (string key, KeyEvent cmd) =>
            {
                if (cmd == eventKeysSet.EventCmd && flagToBlockEventRun)
                {
                    // временная защёлка, чтобы подписка выполнялась один раз
                    flagToBlockEventRun = false;
                    _logger.LogInformation(301, "Key {Key} with command {Cmd} was received, flagToBlockEventRun = {Flag}.", eventKeyFrontGivesTask, cmd, flagToBlockEventRun);

                    // вернуть изменённое значение flagEvent из FetchKeysOnEventRun для возобновления подписки
                    flagToBlockEventRun = await _captures.FetchKeysOnEventRun(eventKeysSet);

                    _logger.LogInformation(901, "END - FetchKeysOnEventRun finished and This BackServer waits the next event.");
                }
            });

            string eventKeyCommand = $"Key = {eventKeyFrontGivesTask}, Command = {eventKeysSet.EventCmd}";
            _logger.LogInformation(19205, "You subscribed on event - {EventKey}.", eventKeyCommand);
        }

        // вызвать из монитора или откуда-то из сервиса?
        // точно не из монитора - там неизвестен гуид пакета
        // можно из первого места, где получаем гуид пакета
        public void SubscribeOnEventCheck(EventKeyNames eventKeysSet, string guidField)
        {
            // eventKey - tasks package guid, где взять?
            string eventKeyTasksPackage = "tasks package guid, где взять?"; // надо получить в guidField или получить ключ, где можно взять?
            _logger.LogInformation(205, "This BackServer subscribed on key {0}.", eventKeyTasksPackage);

            // типовая блокировка множественной подписки до специального разрешения повторной подписки
            bool flagToBlockEventCheck = true;

            _keyEvents.Subscribe(eventKeyTasksPackage, async (string key, KeyEvent cmd) =>
            {
                if (cmd == eventKeysSet.EventCmd && flagToBlockEventCheck)
                {
                    // временная защёлка, чтобы подписка выполнялась один раз
                    flagToBlockEventCheck = false;
                    _logger.LogInformation(306, "Key {Key} with command {Cmd} was received, flagToBlockEventCheck = {Flag}.", eventKeyTasksPackage, cmd, flagToBlockEventCheck);

                    // вернуть изменённое значение flagEvent из FetchKeysOnEventRun для возобновления подписки
                    flagToBlockEventCheck = await _processing.CheckingAllTasksCompletion(eventKeysSet);

                    // что будет, если во время ожидания FetchKeysOnEventRun придёт новое сообщение по подписке? проверить экспериментально
                    _logger.LogInformation(906, "END - FetchKeysOnEventRun finished and This BackServer waits the next event.");
                }
            });

            string eventKeyCommand = $"Key = {eventKeyTasksPackage}, Command = {eventKeysSet.EventCmd}";
            _logger.LogInformation(19206, "You subscribed on event - {EventKey}.", eventKeyCommand);
        }
    }
}

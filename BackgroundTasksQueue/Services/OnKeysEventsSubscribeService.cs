using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CachingFramework.Redis.Contracts;
using CachingFramework.Redis.Contracts.Providers;
using Microsoft.Extensions.Logging;
using BackgroundTasksQueue.Library.Models;

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
        // в мониторе подписываемся на ключ сервера и когда там появится номер пакета задач, подписываемся на него

        public void SubscribeOnEventServerGuid(EventKeyNames eventKeysSet)
        {
            string backServerPrefixGuid = eventKeysSet.BackServerPrefixGuid;            
            _logger.LogInformation(19701, "This BackServer subscribed on key {0}.", backServerPrefixGuid);

            // типовая блокировка множественной подписки до специального разрешения повторной подписки
            // здесь не надо блокировать - пока что
            //bool flagToBlockEventCheck = true;

            _keyEvents.Subscribe(backServerPrefixGuid, async (string key, KeyEvent cmd) =>
            {
                if (cmd == eventKeysSet.EventCmd) // && flagToBlockEventCheck)
                {
                    // временная защёлка, чтобы подписка выполнялась один раз - нет
                    //flagToBlockEventCheck = false;
                    _logger.LogInformation(19703, "Key {Key} with command {Cmd} was received.", backServerPrefixGuid, cmd); //, flagToBlockEventCheck = {Flag} , flagToBlockEventCheck);

                    // получить ключ guidField - это не так просто, если выполняется уже не первый пакет
                    // надо как-то получить последнее созданное поле
                    // пока в значение там int в процентах и в свежесозданном поле можно ставить -1
                    // но потом там будет класс/модель и будет сложнее
                    // можно использовать разные префиксы для хранения простых процентов и модели
                    // достать все поля, проверить, что размер словаря больше нуля
                    // (пока поля не удаляем, но кто знает, что будет дальше)
                    // перебрать словарь и найти значение меньше нуля - оно должно быть одно
                    // можно искать до первого отрицательного и выйти
                    // или можно перебрать все и, если отрицательное не одно, сообщить об ошибке
                    string newlyPackageGuid = await FindFreshPackageGuidFileld(eventKeysSet);

                    // вернуть изменённое значение flagEvent из FetchKeysOnEventRun для возобновления подписки - нет
                    SubscribeOnEventCheck(eventKeysSet, newlyPackageGuid);

                    // что будет, если во время ожидания FetchKeysOnEventRun придёт новое сообщение по подписке? проверить экспериментально
                    _logger.LogInformation(19705, "END - FetchKeysOnEventRun finished and This BackServer waits the next event.");
                }
            });

            string eventKeyCommand = $"Key = {backServerPrefixGuid}, Command = {eventKeysSet.EventCmd}";
            _logger.LogInformation(19707, "You subscribed on event - {EventKey}.", eventKeyCommand);
        }

        private async Task<string> FindFreshPackageGuidFileld(EventKeyNames eventKeysSet)
        {
            string backServerPrefixGuid = eventKeysSet.BackServerPrefixGuid;
            string newlyPackageGuid;
            // этот код до --- можно выделить в отдельный метод, посмотреть остальные применения
            IDictionary<string, int> packagesList = await _cache.GetHashedAllAsync<int>(backServerPrefixGuid);
            int packagesListCount = packagesList.Count;
            if (packagesListCount == 0)
            // тогда возвращаемся с пустыми руками                
            { return null; }

            foreach (var p in packagesList)
            {
                var (packageGuid, packageStateInit) = p;
                if (packageStateInit < 0)
                {
                    newlyPackageGuid = packageGuid;
                    return newlyPackageGuid;
                }
                _logger.LogInformation(501, "Tasks package field {1} found with value {2}, total fileds {3}.", packageGuid, packageStateInit, packagesListCount);
            }

            _logger.LogInformation(511, "The newly created tasks package field was not found in total fileds {0}.", packagesListCount);
            return null;
        }

        public void SubscribeOnEventCheck(EventKeyNames eventKeysSet, string newlyPackageGuid)
        {
            // eventKey - tasks package guid, где взять?
            //string newlyPackageGuid = "tasks package guid, где взять?"; // надо получить в guidField или получить ключ, где можно взять?
            _logger.LogInformation(205, "This BackServer subscribed on key {0}.", newlyPackageGuid);

            // типовая блокировка множественной подписки до специального разрешения повторной подписки
            bool flagToBlockEventCheck = true;

            _keyEvents.Subscribe(newlyPackageGuid, async (string key, KeyEvent cmd) =>
            {
                if (cmd == eventKeysSet.EventCmd && flagToBlockEventCheck)
                {
                    // временная защёлка, чтобы подписка выполнялась один раз
                    flagToBlockEventCheck = false;
                    _logger.LogInformation(306, "Key {Key} with command {Cmd} was received, flagToBlockEventCheck = {Flag}.", newlyPackageGuid, cmd, flagToBlockEventCheck);

                    // вернуть изменённое значение flagEvent из FetchKeysOnEventRun для возобновления подписки
                    flagToBlockEventCheck = await _processing.CheckingAllTasksCompletion(eventKeysSet);

                    // что будет, если во время ожидания FetchKeysOnEventRun придёт новое сообщение по подписке? проверить экспериментально
                    _logger.LogInformation(906, "END - FetchKeysOnEventRun finished and This BackServer waits the next event.");
                }
            });

            string eventKeyCommand = $"Key = {newlyPackageGuid}, Command = {eventKeysSet.EventCmd}";
            _logger.LogInformation(19206, "You subscribed on event - {EventKey}.", eventKeyCommand);
        }
    }
}

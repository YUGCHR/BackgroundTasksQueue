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
        public void SubscribeOnEventRun(EventKeyNames eventKeysSet, string backServerGuid);
        public void SubscribeOnEventAdd(string eventKey, KeyEvent eventCmd);
    }

    public class OnKeysEventsSubscribeService : IOnKeysEventsSubscribeService
    {
        private readonly IBackgroundTasksService _task2Queue;
        private readonly ILogger<OnKeysEventsSubscribeService> _logger;
        private readonly ICacheProviderAsync _cache;
        private readonly IKeyEventsProvider _keyEvents;

        public OnKeysEventsSubscribeService(
            ILogger<OnKeysEventsSubscribeService> logger,
            ICacheProviderAsync cache,
            IKeyEventsProvider keyEvents,
            IBackgroundTasksService task2Queue)
        {
            _task2Queue = task2Queue;
            _logger = logger;
            _cache = cache;
            _keyEvents = keyEvents;
        }

        public async Task<string> FetchGuidFieldTaskRun(string eventKeyRun, string eventFieldRun)
        {
            string eventGuidFieldRun = await _cache.GetHashedAsync<string>(eventKeyRun, eventFieldRun); //получить guid поле для "task:run"

            return eventGuidFieldRun;
        }

        public void SubscribeOnEventRun(EventKeyNames eventKeysSet, string backServerGuid)
        {
            string eventKeyFrontGivesTask = eventKeysSet.EventKeyFrontGivesTask;
            _logger.LogInformation("Background server No: {EventKey} subscribed on key {}.", backServerGuid, eventKeyFrontGivesTask);

            _keyEvents.Subscribe(eventKeyFrontGivesTask, async (string key, KeyEvent cmd) =>
            {
                if (cmd == eventKeysSet.EventCmd)
                {
                    _logger.LogInformation("Key {Key} with command {Cmd} was received.", eventKeyFrontGivesTask, cmd);
                    await FetchKeysOnEventRun(eventKeysSet, backServerGuid);
                }
            });

            string eventKeyCommand = $"Key = {eventKeyFrontGivesTask}, Command = {eventKeysSet.EventCmd}";
            _logger.LogInformation("You subscribed on event - {EventKey}.", eventKeyCommand);
        }

        private async Task FetchKeysOnEventRun(EventKeyNames eventKeysSet, string backServerGuid)
        {
            string eventKeyFrontGivesTask = eventKeysSet.EventKeyFrontGivesTask;
            //_logger.LogInformation("Task FetchKeysOnEventRun with key {0} and field {1} started.", eventKeyRun, eventGuidFieldRun);

            // здесь начать цикл по получению задачи, если не получилось, то обновлять словарь ключей?
            // в дальнейшем можно обновлять словарь в зависимости от соотношения ключей/ серверов
            // если ключей заметно больше, чем серверов, то первая-вторая неудача могут быть случайными и перечитать словарь надо только после третьей неудачи
            //а лучше вообще сразу стереться такому неудачливому серверу

            // после сообщения подписки об обновлении ключа, достаём список свободных задач
            IDictionary<string, string> tasksList = await _cache.GetHashedAllAsync<string>(eventKeyFrontGivesTask);
            int tasksListCount = tasksList.Count;

            for (int i = 0; i < tasksListCount; i++)
            {
                // generate random integers from 0 to guids count
                Random rand = new Random();
                int diceRoll = rand.Next(0, tasksListCount - 1);
                var (guidField, guidValue) = tasksList.ElementAt(diceRoll);

                // проверяем захват задачи - пробуем удалить выбранное поле ключа
                bool isDeleteSuccess = await _cache.RemoveHashedAsync(eventKeyFrontGivesTask, guidField);
                // здесь может разорваться цепочка между ключом, который известен контроллеру и ключом пакета задач
                _logger.LogInformation("Background server No: {0} reported - isDeleteSuceess = {1}.", backServerGuid, isDeleteSuccess);

                if (isDeleteSuccess)
                {
                    _logger.LogInformation("Background server No: {0} fetched taskPackageKey {1} successfully.", backServerGuid, guidField); // победитель по жизни

                    // регистрируем полученную задачу на ключе выполняемых/выполненных задач
                    // поле - исходный ключ пакета (известный контроллеру, по нему он найдёт сервер, выполняющий его задание)
                    await _cache.SetHashedAsync(eventKeysSet.EventKeyBacksTasksProceed, guidField, backServerGuid);
                    // регистрируем исходный ключ и ключ пакета задач на ключе сервера - чтобы не разорвать цепочку
                    await _cache.SetHashedAsync(backServerGuid, guidField, guidValue);

                    // далее в отдельный метод

                    IDictionary<string, int> taskPackage = await _cache.GetHashedAllAsync<int>(guidValue); // получили пакет заданий - id задачи и данные (int) для неё

                    foreach (var t in taskPackage) // try to Select?
                    {
                        var (taskGuid, cycleCount) = t;
                        // складываем задачи во внутреннюю очередь сервера
                        _task2Queue.StartWorkItem(backServerGuid, taskGuid, cycleCount);
                        await _cache.SetHashedAsync(backServerGuid, taskGuid, cycleCount); // создаём ключ для контроля выполнения задания из пакета
                        _logger.LogInformation("Background server No: {0} sent Task with ID {1} and {2} cycles to Queue.", processNum, taskGuid, cycleCount);
                    }
                    return;
                }
            }

            // перебираем все guid задач, выбираем случайную (по номеру) и пробуем её получить
            foreach (var t in tasksList) // try to Select?
            {
                //var (guidFiled, guidValue) = t;






            }
            // когда номер сервера будет guid, очистку можно убрать, но поставить срок жизни ключа час или день, при нормальном завершении пакета ключ удаляется штатно
            //bool isKeyCleanedSuccess = await _cache.RemoveAsync(processNum.ToString());
            //_logger.LogInformation("Background server No: {0} reported - Server Key Cleaned Success = {1}.", processNum, isKeyCleanedSuccess);


            _logger.LogInformation("Background server No: {0} cannot catch the task.", processNum);
            return;
        }

        public void SubscribeOnEventAdd(string eventKey, KeyEvent eventCmd)
        {
            string eventKeyCommand = $"Key {eventKey}, HashSet command";
            _logger.LogInformation("You subscribed on event - {EventKey}.", eventKeyCommand);
        }

    }
}

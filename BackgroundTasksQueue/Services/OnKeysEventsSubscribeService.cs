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

        // подписываемся на ключ сообщения о появлении свободных задач
        public void SubscribeOnEventRun(EventKeyNames eventKeysSet, string backServerGuid)
        {
            string eventKeyFrontGivesTask = eventKeysSet.EventKeyFrontGivesTask;
            _logger.LogInformation(201,"This BackServer subscribed on key {0}.", eventKeyFrontGivesTask);

            bool flagEvent = true;

            _keyEvents.Subscribe(eventKeyFrontGivesTask, async (string key, KeyEvent cmd) =>
            {
                if (cmd == eventKeysSet.EventCmd && flagEvent)
                {
                    // временная защёлка, чтобы подписка выполнялась один раз
                    flagEvent = false;
                    _logger.LogInformation(301, "Key {Key} with command {Cmd} was received, flagEvent = {Flag}.", eventKeyFrontGivesTask, cmd, flagEvent);
                    await FetchKeysOnEventRun(eventKeysSet, backServerGuid);
                    // что будет, если во время ожидания FetchKeysOnEventRun придёт новое сообщение по подписке? проверить экспериментально
                    _logger.LogInformation(901,"END FetchKeysOnEventRun finished and This BackServer waits the next event.");
                }
            });

            string eventKeyCommand = $"Key = {eventKeyFrontGivesTask}, Command = {eventKeysSet.EventCmd}";
            _logger.LogInformation("You subscribed on event - {EventKey}.", eventKeyCommand);
        }

        private async Task FetchKeysOnEventRun(EventKeyNames eventKeysSet, string backServerGuid)
        {
            string eventKeyFrontGivesTask = eventKeysSet.EventKeyFrontGivesTask;
            string eventKeyBacksTasksProceed = eventKeysSet.EventKeyBacksTasksProceed;
            _logger.LogInformation(401, "This BackServer started FetchKeysOnEventRun.");

            // здесь начать цикл по получению задачи, если не получилось, то обновлять словарь ключей?
            // в дальнейшем можно обновлять словарь в зависимости от соотношения ключей/ серверов
            // если ключей заметно больше, чем серверов, то первая-вторая неудача могут быть случайными и перечитать словарь надо только после третьей неудачи
            //а лучше вообще сразу стереться такому неудачливому серверу

            // пытаемся захватить задачу, пока не получится, если никак, то там и останемся? 
            // а если захватить получится, то выйдем из цикла возвратом
            bool isDeleteSuccess = false;
            bool isExistEventKeyFrontGivesTask = true;

            // этот цикл будет чем-то большим - в нём будем крутиться вообще, пока есть задачи в ключе
            // когда задача получена надо отдать управление в метод, который будет проверять ход выполнения и await его, пока все задания в пакете не будут выполнены
            // и тогда идти опять проверять ключ кафе и пробовать взять следующую задачу - подписку на тот ключ уже никто не обновит, пока новый пакет не приедет
            // и ещё надо заблокировать подписку на этот ключ, пока выполняются задачи

            while (DoWhileCheck(isDeleteSuccess, isExistEventKeyFrontGivesTask)) // return isDeleteSuccess || isExistEventKeyFrontGivesTask
            {
                // проверить существование ключа, может, все задачи давно разобрали и ключ исчез
                isExistEventKeyFrontGivesTask = await _cache.KeyExistsAsync(eventKeyFrontGivesTask);                

                // после сообщения подписки об обновлении ключа, достаём список свободных задач
                IDictionary<string, string> tasksList = await _cache.GetHashedAllAsync<string>(eventKeyFrontGivesTask);
                int tasksListCount = tasksList.Count;
                _logger.LogInformation(403, "TasksList fetched - tasks count = {1}.", tasksListCount);
                if(tasksListCount == 0) 
                { return;}
                // generate random integers from 0 to guids count
                Random rand = new Random();
                // если осталась одна задача, кубик бросать не надо
                int diceRoll = tasksListCount - 1;
                if (tasksListCount > 1)
                {
                    diceRoll = rand.Next(0, tasksListCount - 1);
                }
                var (guidField, guidValue) = tasksList.ElementAt(diceRoll);

                // проверяем захват задачи - пробуем удалить выбранное поле ключа
                // isDeleteSuccess сделать методом и вызвать прямо из if
                // в дальнейшем можно вместо Remove использовать RedLock
                isDeleteSuccess = await _cache.RemoveHashedAsync(eventKeyFrontGivesTask, guidField);
                // здесь может разорваться цепочка между ключом, который известен контроллеру и ключом пакета задач
                _logger.LogInformation(411, "This BackServer reported - isDeleteSuccess = {1}.", isDeleteSuccess);

                if (isDeleteSuccess)
                {
                    _logger.LogInformation(421, "This BackServer fetched taskPackageKey {1} successfully.", guidField); // победитель по жизни

                    // регистрируем полученную задачу на ключе выполняемых/выполненных задач
                    // поле - исходный ключ пакета (известный контроллеру, по нему он найдёт сервер, выполняющий его задание)
                    // пока что поле задачи в кафе и ключ самой задачи совпадают, поэтому контроллер может напрямую читать состояние пакета задач по известному ему ключу
                    await _cache.SetHashedAsync(eventKeyBacksTasksProceed, guidField, backServerGuid);
                    _logger.LogInformation(431, "Tasks package was registered on key {0} - \n      with source package key {1} and original package key {2}.", eventKeyBacksTasksProceed, guidField, guidValue);

                    // регистрируем исходный ключ и ключ пакета задач на ключе сервера - чтобы не разорвать цепочку
                    await _cache.SetHashedAsync(backServerGuid, guidField, guidValue);
                    _logger.LogInformation(441, "This BackServer registered tasks package - \n      with source package key {1} and original package key {2}.", guidField, guidValue);

                    // здесь подходящее место, чтобы определить количество процессов, выполняющих задачи из пакета - в зависимости от количества задач, но не более максимума из константы

                    //string eventKey = "task:add";
                    //await _cache.SetHashedAsync(eventKey, "ttt", 1);

                    // и по завершению выполнения задач хорошо бы удалить процессы
                    // нужен внутрисерверный ключ (константа), где каждый из сервисов (каждого) сервера может узнать номер сервера, на котором запущен - чтобы правильно подписаться на событие
                    // сервера одинаковые и жёлуди у них тоже одинаковые, разница только в номере, который сервер генерирует при своём старте
                    // вот этот номер нужен сервисам, чтобы подписаться на события своего сервера, а не соседнего
                    // ключ добавления одного процесса "task:add"
                    // ключ удаления одного процесса "task:del"

                    // далее в отдельный метод и ждём в нём, пока не закончится выполнение всех задач
                    await TasksFromKeysToQueue(guidValue, backServerGuid);

                    // выйти из цикла можем только когда не останется задач в ключе кафе
                }
            }

            // пока что сюда никак попасть не может, но надо предусмотреть, что все задачи исчерпались, а никого не поймали
            // скажем, ключ вообще исчез и ловить больше нечего
            // теперь сюда попадём, если ключ eventKeyFrontGivesTask исчез и задачу не захватить
            // надо сделать возврат в исходное состояние ожидания вброса ключа
            // побочный эффект - можно смело брать последнюю задачу и не опасаться, что ключ eventKeyFrontGivesTask исчезнет
            _logger.LogInformation(481, "This BackServer cannot catch the task.");

            // возвращаемся в состояние подписки на ключ кафе и ожидания события по этой подписке
            _logger.LogInformation(491, "This BackServer goes over to the subscribe event awaiting.");
            return;
        }

        private bool DoWhileCheck(bool isDeleteSuccess, bool isExistEventKeyFrontGivesTask)
        {
            return isDeleteSuccess || isExistEventKeyFrontGivesTask;
        }

        private async Task TasksFromKeysToQueue(string guidValue, string backServerGuid)
        {
            IDictionary<string, int> taskPackage = await _cache.GetHashedAllAsync<int>(guidValue); // получили пакет заданий - id задачи и данные (int) для неё
            int taskPackageCount = taskPackage.Count;
            foreach (var t in taskPackage)
            {
                var (taskGuid, cycleCount) = t;
                // складываем задачи во внутреннюю очередь сервера
                _task2Queue.StartWorkItem(backServerGuid, taskGuid, cycleCount);
                await _cache.SetHashedAsync(backServerGuid, taskGuid, cycleCount); // создаём ключ для контроля выполнения задания из пакета
                _logger.LogInformation(501, "This BackServer sent Task with ID {1} and {2} cycles to Queue.", taskGuid, cycleCount);
            }

            _logger.LogInformation(511, "This BackServer sent total {1} tasks to Queue.",  taskPackageCount);
        }

        public void SubscribeOnEventAdd(string eventKey, KeyEvent eventCmd)
        {
            string eventKeyCommand = $"Key {eventKey}, HashSet command";
            _logger.LogInformation("You subscribed on event - {EventKey}.", eventKeyCommand);
        }

    }
}

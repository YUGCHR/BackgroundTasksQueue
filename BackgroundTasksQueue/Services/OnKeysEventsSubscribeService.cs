﻿using System;
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
                    flagToBlockEventRun = await FetchKeysOnEventRun(eventKeysSet);

                    // что будет, если во время ожидания FetchKeysOnEventRun придёт новое сообщение по подписке? проверить экспериментально
                    _logger.LogInformation(901, "END - FetchKeysOnEventRun finished and This BackServer waits the next event.");
                }
            });

            string eventKeyCommand = $"Key = {eventKeyFrontGivesTask}, Command = {eventKeysSet.EventCmd}";
            _logger.LogInformation("You subscribed on event - {EventKey}.", eventKeyCommand);
        }

        private async Task<bool> FetchKeysOnEventRun(EventKeyNames eventKeysSet)
        {
            string backServerPrefixGuid = eventKeysSet.BackServerPrefixGuid;
            string eventKeyFrontGivesTask = eventKeysSet.EventKeyFrontGivesTask;
            string eventKeyBacksTasksProceed = eventKeysSet.EventKeyBacksTasksProceed;
            _logger.LogInformation(401, "This BackServer started FetchKeysOnEventRun.");

            // начало главного цикла сразу после срабатывания подписки, условие - пока существует ключ распределения задач
            // считать пакет полей из ключа, если задач больше одной, бросить кубик
            // проверить захват задачи, если получилось - выполнять, нет - вернулись на начало главного цикла
            // выполнение - в отдельном методе, достать по ключу задачи весь пакет
            // определить, сколько надо процессов - количество задач в пакете разделить на константу, не менее одного и не более константы
            // запустить процессы в отдельном методе, сложить количество в ключ пакета
            // достать задачи из пакета и запустить их в очередь
            // следующим методом висеть и контролировать ход выполнения всех задач - подписаться на их ключи, собирать ход выполнения каждой, суммировать и складывать общий процент в ключ сервера
            // по окончанию всех задач удалить все процессы?
            // вернуться на начало главного цикла
            bool isExistEventKeyFrontGivesTask = true;

            // нет смысла проверять isDeleteSuccess, достаточно существования ключа задач - есть он, ловим задачи, нет его - возвращаемся
            while (isExistEventKeyFrontGivesTask)
            {
                // проверить существование ключа, может, все задачи давно разобрали и ключ исчез
                isExistEventKeyFrontGivesTask = await _cache.KeyExistsAsync(eventKeyFrontGivesTask);
                _logger.LogInformation(402, "isExistEventKeyFrontGivesTask = {1}.", isExistEventKeyFrontGivesTask);

                if (!isExistEventKeyFrontGivesTask)
                // если ключа нет, тогда возвращаемся в состояние подписки на ключ кафе и ожидания события по этой подписке                
                { return false; } // надо true

                // после сообщения подписки об обновлении ключа, достаём список свободных задач
                // список получается неполный! - оказывается, потому, что фронт не успеваем залить остальные поля, когда бэк с первым полем уже здесь
                IDictionary<string, string> tasksList = await _cache.GetHashedAllAsync<string>(eventKeyFrontGivesTask);
                int tasksListCount = tasksList.Count;
                _logger.LogInformation(403, "TasksList fetched - tasks count = {1}.", tasksListCount);

                // временный костыль - 0 - это задач в ключе не осталось - возможно, только что (перед носом) забрали последнюю
                if (tasksListCount == 0)
                // тогда возвращаемся в состояние подписки на ключ кафе и ожидания события по этой подписке                
                { return true; }

                // выбираем случайное поле пакета задач - скорее всего, первая попытка будет только с одним полем, остальные не успеют положить и будет драка, но на второй попытке уже разойдутся по разным полям
                (string tasksPakageGuidField, string tasksPakageGuidValue) = tasksList.ElementAt(DiceRoll(tasksListCount));

                // проверяем захват задачи - пробуем удалить выбранное поле ключа                
                // в дальнейшем можно вместо Remove использовать RedLock
                bool isDeleteSuccess = await _cache.RemoveHashedAsync(eventKeyFrontGivesTask, tasksPakageGuidField);
                // здесь может разорваться цепочка между ключом, который известен контроллеру и ключом пакета задач
                _logger.LogInformation(411, "This BackServer reported - isDeleteSuccess = {1}.", isDeleteSuccess);

                if (isDeleteSuccess)
                {
                    _logger.LogInformation(421, "This BackServer fetched taskPackageKey {1} successfully.", tasksPakageGuidField); // победитель по жизни
                    // следующие две регистрации пока непонятно, зачем нужны - доступ к состоянию пакета задач всё равно по ключу пакета

                    // регистрируем полученную задачу на ключе выполняемых/выполненных задач
                    // поле - исходный ключ пакета (известный контроллеру, по нему он найдёт сервер, выполняющий его задание)
                    // пока что поле задачи в кафе и ключ самой задачи совпадают, поэтому контроллер может напрямую читать состояние пакета задач по известному ему ключу
                    await _cache.SetHashedAsync(eventKeyBacksTasksProceed, tasksPakageGuidField, backServerPrefixGuid);
                    _logger.LogInformation(431, "Tasks package was registered on key {0} - \n      with source package key {1} and original package key {2}.", eventKeyBacksTasksProceed, tasksPakageGuidField, tasksPakageGuidValue);

                    // регистрируем исходный ключ и ключ пакета задач на ключе сервера - чтобы не разорвать цепочку
                    await _cache.SetHashedAsync(backServerPrefixGuid, tasksPakageGuidField, tasksPakageGuidValue);
                    _logger.LogInformation(441, "This BackServer registered tasks package - \n      with source package key {1} and original package key {2}.", tasksPakageGuidField, tasksPakageGuidValue);

                    // и по завершению выполнения задач хорошо бы удалить процессы
                    // нужен внутрисерверный ключ (константа), где каждый из сервисов (каждого) сервера может узнать номер сервера, на котором запущен - чтобы правильно подписаться на событие
                    // сервера одинаковые и жёлуди у них тоже одинаковые, разница только в номере, который сервер генерирует при своём старте
                    // вот этот номер нужен сервисам, чтобы подписаться на события своего сервера, а не соседнего
                    // ключ добавления одного процесса "task:add"
                    // ключ удаления одного процесса "task:del"

                    // складываем задачи во внутреннюю очередь сервера
                    int taskPackageCount = await TasksFromKeysToQueue(tasksPakageGuidField, tasksPakageGuidValue, backServerPrefixGuid);

                    // здесь подходящее место, чтобы определить количество процессов, выполняющих задачи из пакета - в зависимости от количества задач, но не более максимума из константы
                    // PrefixProcessAdd - префикс ключа (+ backServerGuid) управления добавлением процессов
                    // PrefixProcessCancel - префикс ключа (+ backServerGuid) управления удалением процессов
                    // в значение положить требуемое количество процессов
                    // имя поля должно быть общим для считывания значения
                    // PrefixProcessCount - 
                    // не забыть обнулить (или удалить) ключ после считывания и добавления процессов
                    int addProcessesCount = await AddProcessesToPerformingTasks(eventKeysSet, taskPackageCount);

                    // тут ждать, пока не будут посчитаны всё задачи пакета
                    int percents = await CheckingAllTasksCompletion(tasksPakageGuidField);

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
            // восстанавливаем условие разрешения обработки подписки
            return false; // надо true
        }

        private int DiceRoll(int tasksListCount)
        {
            // generate random integers from 0 to guids count
            Random rand = new();
            // индекс словаря по умолчанию
            int diceRoll = tasksListCount - 1;
            // если осталась одна задача, кубик бросать не надо
            if (tasksListCount > 1)
            {
                diceRoll = rand.Next(0, tasksListCount - 1);
            }
            _logger.LogInformation(407, "DiceRoll rolled {1}.", diceRoll);
            return diceRoll;
        }

        private async Task<int> CheckingAllTasksCompletion(string guidField)
        {
            // тут подписаться на ключ пакета задач, но будет много событий
            // можно ставить блокировку на подписку и не отвлекаться на события, пока не закончена очередная проверка

            return default;
        }

        private async Task<int> AddProcessesToPerformingTasks(EventKeyNames eventKeysSet, int taskPackageCount)
        {
            string backServerPrefixGuid = eventKeysSet.BackServerPrefixGuid;
            string backServerGuid = eventKeysSet.BackServerGuid;
            string prefixProcessAdd = eventKeysSet.PrefixProcessAdd;
            string eventFieldBack = eventKeysSet.EventFieldBack;
            string eventKeyProcessAdd = $"{prefixProcessAdd}:{backServerGuid}"; // process:add:(this server guid) 

            // вычисляем нужное количество процессов
            int toAddProcessesCount = CalcAddProcessesCount(eventKeysSet, taskPackageCount);

            // создаём ключ добавления процессов и в значении нужное количество процессов
            await _cache.SetHashedAsync(eventKeyProcessAdd, eventFieldBack, toAddProcessesCount);

            _logger.LogInformation(517, "This BackServer ask to start {0} processes, key = {1}, field = {2}.", toAddProcessesCount, eventKeyProcessAdd, eventFieldBack);
            return toAddProcessesCount;
        }

        private int CalcAddProcessesCount(EventKeyNames eventKeysSet, int taskPackageCount)
        {
            int balanceOfTasksAndProcesses = eventKeysSet.BalanceOfTasksAndProcesses;
            int maxProcessesCountOnServer = eventKeysSet.MaxProcessesCountOnServer;
            int toAddProcessesCount;

            switch (balanceOfTasksAndProcesses)
            {
                // 0 - автовыбор - создаём процессов по числу задач
                case 0:
                    toAddProcessesCount = taskPackageCount;
                    return toAddProcessesCount;
                // больше нуля - основной вариант - делим количество задач на эту константу и если она больше максимума, берём константу максимума
                case > 0:
                    int multiplier = 10000;
                    toAddProcessesCount = (taskPackageCount * multiplier / balanceOfTasksAndProcesses) / multiplier;
                    // если константа максимума неправильная - 0 или отрицательная, игнорируем ее
                    if (toAddProcessesCount > maxProcessesCountOnServer && maxProcessesCountOnServer > 0)
                    {
                        toAddProcessesCount = maxProcessesCountOnServer;
                    }
                    if (toAddProcessesCount < 1)
                    { toAddProcessesCount = 1; }
                    return toAddProcessesCount;
                // меньше нуля - тайный вариант для настройки - количество процессов равно константе (с обратным знаком, естественно)
                case < 0:
                    toAddProcessesCount = balanceOfTasksAndProcesses * -1;
                    _logger.LogInformation(517, "CalcAddProcessesCount calculated total {1} processes are necessary.", toAddProcessesCount);
                    return toAddProcessesCount;
            }
        }

        private async Task<int> TasksFromKeysToQueue(string tasksPakageGuidField, string tasksPakageGuidValue, string backServerPrefixGuid)
        {
            IDictionary<string, int> taskPackage = await _cache.GetHashedAllAsync<int>(tasksPakageGuidValue); // получили пакет заданий - id задачи и данные (int) для неё
            int taskPackageCount = taskPackage.Count;
            foreach (var t in taskPackage)
            {
                var (singleTaskGuid, assignmentTerms) = t;
                // складываем задачи во внутреннюю очередь сервера
                _task2Queue.StartWorkItem(backServerPrefixGuid, tasksPakageGuidValue, singleTaskGuid, assignmentTerms);
                //await _cache.SetHashedAsync(backServerPrefixGuid, singleTaskGuid, assignmentTerms); // создаём ключ для контроля выполнения задания из пакета
                _logger.LogInformation(501, "This BackServer sent Task with ID {1} and {2} cycles to Queue.", singleTaskGuid, assignmentTerms);
            }

            _logger.LogInformation(511, "This BackServer sent total {1} tasks to Queue.", taskPackageCount);
            return taskPackageCount;
        }

        public void SubscribeOnEventAdd(string eventKey, KeyEvent eventCmd)
        {
            string eventKeyCommand = $"Key {eventKey}, HashSet command";
            _logger.LogInformation("You subscribed on event - {EventKey}.", eventKeyCommand);
        }

    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CachingFramework.Redis.Contracts;
using CachingFramework.Redis.Contracts.Providers;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using BackgroundTasksQueue.Services;
using BackgroundTasksQueue.Models;

namespace BackgroundTasksQueue
{
    public class MonitorLoop
    {
        private readonly ILogger<MonitorLoop> _logger;
        private readonly ISettingConstants _constant;
        private readonly CancellationToken _cancellationToken;
        private readonly ICacheProviderAsync _cache;
        private readonly IOnKeysEventsSubscribeService _subscribe;
        private readonly string _guid;

        public MonitorLoop(
            GenerateThisBackServerGuid thisGuid,
            ILogger<MonitorLoop> logger,
            ISettingConstants constant,
            ICacheProviderAsync cache,
            IHostApplicationLifetime applicationLifetime,
            IOnKeysEventsSubscribeService subscribe)
        {
            _logger = logger;
            _constant = constant;
            _cache = cache;
            _subscribe = subscribe;
            _cancellationToken = applicationLifetime.ApplicationStopping;
            _guid = thisGuid.ThisBackServerGuid();
        }

        public void StartMonitorLoop()
        {
            _logger.LogInformation(100, "BackServer's MonitorLoop is starting.");

            // Run a console user input loop in a background thread
            Task.Run(Monitor, _cancellationToken);
        }

        public async Task Monitor()
        {
            // концепция хищных бэк-серверов, борющихся за получение задач
            // контроллеров же в лесу (на фронте) много и желудей, то есть задач, у них тоже много
            // а несколько (много) серверов могут неспешно выполнять задачи из очереди в бэкграунде

            // собрать все константы в один класс
            EventKeyNames eventKeysSet = InitialiseEventKeyNames();

            // множественные контроллеры по каждому запросу (пользователей) создают очередь - каждый создаёт ключ, на который у back-servers подписка, в нём поле со своим номером, а в значении или имя ключа с заданием или само задание            
            // дальше бэк-сервера сами разбирают задания
            // бэк после старта кладёт в ключ ___ поле со своим сгенерированным guid для учета?
            // все бэк-сервера подписаны на базовый ключ и получив сообщение по подписке, стараются взять задание - у кого получилось удалить ключ, тот и взял

            //string test = ThisBackServerGuid.GetThisBackServerGuid(); // guid from static class
            // получаем уникальный номер этого сервера, сгенерированный при старте экземпляра сервера
            //string backServerGuid = $"{eventKeysSet.PrefixBackServer}:{_guid}"; // Guid.NewGuid()
            //EventId aaa = new EventId(222, "INIT");
            string backServerPrefixGuid = eventKeysSet.BackServerPrefixGuid;
            string backServerGuid = eventKeysSet.BackServerGuid;
            _logger.LogInformation(101, "INIT No: {0} - guid of This Server was fetched in MonitorLoop.", backServerPrefixGuid);

            // в значение можно положить время создания сервера
            // проверить, что там за время на ключах, подумать, нужно ли разное время для разных ключей - скажем, кафе и регистрация серверов - день, пакет задач - час
            // регистрируем поле guid сервера на ключе регистрации серверов, а в значение кладём чистый гуид, без префикса
            await _cache.SetHashedAsync<string>(eventKeysSet.EventKeyBackReadiness, backServerPrefixGuid, backServerGuid, eventKeysSet.EventKeyBackReadinessTimeDays);
            // восстановить время жизни ключа регистрации сервера перед новой охотой - где и как?
            // при завершении сервера успеть удалить своё поле из ключа регистрации серверов - обработать cancellationToken

            // подписываемся на ключ сообщения о появлении свободных задач
            _subscribe.SubscribeOnEventRun(eventKeysSet);

            // слишком сложная цепочка guid
            // оставить в общем ключе задач только поле, известное контроллеру и в значении сразу положить сумму задачу в модели
            // первым полем в модели создать номер guid задачи - прямо в модели?
            // оставляем слишком много guid, но добавляем к ним префиксы, чтобы в логах было понятно, что за guid
            // key EventKeyFrontGivesTask, fields - request:guid (model property - PrefixRequest), values - package:guid (PrefixPackage)
            // key package:guid, fileds - task:guid (PrefixTask), values - models
            // key EventKeyBackReadiness, fields - back(server):guid (PrefixBackServer)
            // key EventKeyBacksTasksProceed, fields - request:guid (PrefixRequest), values - package:guid (PrefixPackage)
            // method to fetch package (returns dictionary) from request:guid

            // можно здесь (в while) ждать появления гуид пакета задач, чтобы подписаться на ход его выполнения
            // а можно подписаться на стандартный ключ появления пакета задач - общего для всех серверов, а потом проверять, что это событие на своём сервере
            // хотелось, чтобы вся подписка происходила из monitorLoop, но тут пока никак не узнать номера пакета
            // а если подписываться там, где становится известен номер, придётся перекрёстно подключать сервисы


            // ----------------- вы находитесь здесь

            while (IsCancellationNotYet())
            {
                var keyStroke = Console.ReadKey();

                if (keyStroke.Key == ConsoleKey.W)
                {
                    _logger.LogInformation("ConsoleKey was received {KeyStroke}.", keyStroke.Key);
                }
            }

            _logger.LogInformation("MonitorLoop was canceled by Token.");
        }

        private bool IsCancellationNotYet()
        {
            _logger.LogInformation("Is Cancellation Token obtained? - {1}", _cancellationToken.IsCancellationRequested);
            return !_cancellationToken.IsCancellationRequested; // add special key from Redis?
        }

        private EventKeyNames InitialiseEventKeyNames()
        {
            return new EventKeyNames
            {
                TaskDelayTimeInSeconds = _constant.GetTaskDelayTimeInSeconds, // время задержки в секундах для эмулятора счета задачи
                BalanceOfTasksAndProcesses = _constant.GetBalanceOfTasksAndProcesses, // соотношение количества задач и процессов для их выполнения на back-processes-servers (количества задач разделить на это число и сделать столько процессов)
                MaxProcessesCountOnServer = _constant.GetMaxProcessesCountOnServer, // максимальное количество процессов на back-processes-servers (минимальное - 1)
                EventKeyFrom = _constant.GetEventKeyFrom, // "subscribeOnFrom" - ключ для подписки на команду запуска эмулятора сервера
                EventFieldFrom = _constant.GetEventFieldFrom, // "count" - поле для подписки на команду запуска эмулятора сервера
                EventCmd = KeyEvent.HashSet,
                EventKeyBackReadiness = _constant.GetEventKeyBackReadiness, // ключ регистрации серверов
                EventFieldBack = _constant.GetEventFieldBack,
                EventKeyFrontGivesTask = _constant.GetEventKeyFrontGivesTask, // кафе выдачи задач
                PrefixRequest = _constant.GetPrefixRequest, // request:guid
                PrefixPackage = _constant.GetPrefixPackage, // package:guid
                PrefixTask = _constant.GetPrefixTask, // task:guid
                PrefixBackServer = _constant.GetPrefixBackServer, // backserver:guid
                BackServerGuid = _guid, // this server guid
                BackServerPrefixGuid = $"{_constant.GetPrefixBackServer}:{_guid}", // backserver:(this server guid)
                PrefixProcessAdd = _constant.GetPrefixProcessAdd, // process:add
                PrefixProcessCancel = _constant.GetPrefixProcessCancel, // process:cancel
                PrefixProcessCount = _constant.GetPrefixProcessCount, // process:count
                EventFieldFront = _constant.GetEventFieldFront,
                EventKeyBacksTasksProceed = _constant.GetEventKeyBacksTasksProceed, //  ключ выполняемых/выполненных задач                
                EventKeyFromTimeDays = TimeSpan.FromDays(_constant.GetEventKeyFromTimeDays), // срок хранения ключа eventKeyFrom
                EventKeyBackReadinessTimeDays = TimeSpan.FromDays(_constant.GetEventKeyBackReadinessTimeDays), // срок хранения ключа BackReadiness
                EventKeyFrontGivesTaskTimeDays = TimeSpan.FromDays(_constant.GetEventKeyFrontGivesTaskTimeDays), // срок хранения ключа FrontGivesTask
                EventKeyBackServerMainTimeDays = TimeSpan.FromDays(_constant.GetEventKeyBackServerMainTimeDays), // срок хранения ключа BackServerMain
                EventKeyBackServerAuxiliaryTimeDays = TimeSpan.FromDays(_constant.GetEventKeyBackServerAuxiliaryTimeDays), // срок хранения ключа BackServerAuxiliary
            };
        }
    }
}

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
using BackgroundTasksQueue.Library.Services;
using BackgroundTasksQueue.Library.Models;

namespace BackgroundTasksQueue
{
    public class MonitorLoop
    {
        private readonly ILogger<MonitorLoop> _logger;
        private readonly ISharedDataAccess _data;
        private readonly CancellationToken _cancellationToken;
        private readonly ICacheProviderAsync _cache;
        private readonly IOnKeysEventsSubscribeService _subscribe;
        private readonly string _guid;

        public MonitorLoop(
            GenerateThisInstanceGuidService thisGuid,
            ILogger<MonitorLoop> logger,
            ISharedDataAccess data,
            ICacheProviderAsync cache,
            IHostApplicationLifetime applicationLifetime,
            IOnKeysEventsSubscribeService subscribe)
        {
            _logger = logger;
            _data = data;
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
            //EventKeyNames eventKeysSet = InitialiseEventKeyNames();
            EventKeyNames eventKeysSet = await _data.FetchAllConstants();

            // множественные контроллеры по каждому запросу (пользователей) создают очередь - каждый создаёт ключ, на который у back-servers подписка, в нём поле со своим номером, а в значении или имя ключа с заданием или само задание            
            // дальше бэк-сервера сами разбирают задания
            // бэк после старта кладёт в ключ ___ поле со своим сгенерированным guid для учета?
            // все бэк-сервера подписаны на базовый ключ и получив сообщение по подписке, стараются взять задание - у кого получилось удалить ключ, тот и взял

            //string test = ThisBackServerGuid.GetThisBackServerGuid(); // guid from static class
            // получаем уникальный номер этого сервера, сгенерированный при старте экземпляра сервера
            //string backServerGuid = $"{eventKeysSet.PrefixBackServer}:{_guid}"; // Guid.NewGuid()
            //EventId aaa = new EventId(222, "INIT");

            string backServerGuid = _guid ?? throw new ArgumentNullException(nameof(_guid));
            eventKeysSet.BackServerGuid = backServerGuid;
            string backServerPrefixGuid = $"{eventKeysSet.PrefixBackServer}:{backServerGuid}";
            eventKeysSet.BackServerPrefixGuid = backServerPrefixGuid;

            _logger.LogInformation(101, "INIT No: {0} - guid of This Server was fetched in MonitorLoop.", backServerPrefixGuid);

            // в значение можно положить время создания сервера
            // проверить, что там за время на ключах, подумать, нужно ли разное время для разных ключей - скажем, кафе и регистрация серверов - день, пакет задач - час
            // регистрируем поле guid сервера на ключе регистрации серверов, а в значение кладём чистый гуид, без префикса
            await _cache.SetHashedAsync<string>(eventKeysSet.EventKeyBackReadiness, backServerPrefixGuid, backServerGuid, TimeSpan.FromDays(eventKeysSet.EventKeyBackReadinessTimeDays));
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
    }
}

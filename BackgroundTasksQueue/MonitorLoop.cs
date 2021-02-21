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
            ThisBackServerGuid thisGuid,
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

            _guid = thisGuid.GetThisBackServerGuid();
        }

        public void StartMonitorLoop()
        {
            _logger.LogInformation("Monitor Loop is starting.");

            // Run a console user input loop in a background thread
            Task.Run(Monitor, _cancellationToken);
        }

        public async Task Monitor()
        {
            // концепция хищных бэк-серверов, борющихся за получение задач
            // контроллеров же в лесу (на фронте) много и желудей, то есть задач, у них тоже много
            // а несколько (много) серверов могут неспешно выполнять задачи из очереди в бэкграунде

            EventKeyNames eventKeysSet = InitialiseEventKeyNames();

            // множественные контроллеры по каждому запросу (пользователей) создают очередь - каждый создаёт ключ, на который у back-servers подписка, в нём поле со своим номером, а в значении или имя ключа с заданием или само задание            
            // дальше бэк-сервера сами разбирают задания
            // бэк после старта кладёт в ключ ___ поле со своим сгенерированным guid для учета?
            // все бэк-сервера подписаны на базовый ключ и получив сообщение по подписке, стараются взять задание - у кого получилось удалить ключ, тот и взял

            //string test = ThisBackServerGuid.GetThisBackServerGuid(); // guid from static class
            // получаем уникальный номер этого сервера, сгенерированный при старте экземпляра сервера
            string backServerGuid = $"{eventKeysSet.PrefixBackServer}:{_guid}"; // Guid.NewGuid()
            _logger.LogInformation("INIT - No: {0} - guid of This Server was fetched in MonitorLoop.", backServerGuid);

            // в значение можно положить время создания сервера
            await _cache.SetHashedAsync<string>(eventKeysSet.EventKeyBackReadiness, backServerGuid, backServerGuid, eventKeysSet.Ttl);
            // при завершении сервера удалить своё поле из ключа регистрации серверов

            // подписываемся на ключ сообщения о появлении свободных задач
            _subscribe.SubscribeOnEventRun(eventKeysSet, backServerGuid);

            // слишком сложная цепочка guid
            // оставить в общем ключе задач только поле, известное контроллеру и в значении сразу положить сумму задачу в модели
            // первым полем в модели создать номер guid задачи - прямо в модели?
            // оставляем слишком много guid, но добавляем к ним префиксы, чтобы в логах было понятно, что за guid
            // key EventKeyFrontGivesTask, fields - request:guid (model property - PrefixRequest), values - package:guid (PrefixPackage)
            // key package:guid, fileds - task:guid (PrefixTask), values - models
            // key EventKeyBackReadiness, fields - back(server):guid (PrefixBackServer)
            // key EventKeyBacksTasksProceed, fields - request:guid (PrefixRequest), values - package:guid (PrefixPackage)
            // method to fetch package (returns dictionary) from request:guid

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
            _logger.LogInformation("Cancellation Token is obtained.");
            return !_cancellationToken.IsCancellationRequested; // add special key from Redis?
        }

        private EventKeyNames InitialiseEventKeyNames()
        {
            return new EventKeyNames
            {
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
                EventFieldFront = _constant.GetEventFieldFront,
                EventKeyBacksTasksProceed = _constant.GetEventKeyBacksTasksProceed, //  ключ выполняемых/выполненных задач                
                Ttl = TimeSpan.FromDays(_constant.GetKeyFromTimeDays) // срок хранения ключа eventKeyFrom
            };
        }        
    }
}

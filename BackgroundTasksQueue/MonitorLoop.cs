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

namespace BackgroundTasksQueue
{
    public class MonitorLoop
    {
        private readonly ILogger<MonitorLoop> _logger;
        private readonly ISettingConstants _constant;
        private readonly CancellationToken _cancellationToken;
        private readonly IOnKeysEventsSubscribeService _subscribe;

        public MonitorLoop(
            ILogger<MonitorLoop> logger,
            ISettingConstants constant,
            IHostApplicationLifetime applicationLifetime,
            IOnKeysEventsSubscribeService subscribe)
        {
            _logger = logger;
            _constant = constant;
            _subscribe = subscribe;
            _cancellationToken = applicationLifetime.ApplicationStopping;
        }

        public void StartMonitorLoop()
        {
            _logger.LogInformation("Monitor Loop is starting.");

            // Run a console user input loop in a background thread
            Task.Run(Monitor, _cancellationToken);
        }

        public async Task Monitor()
        {
            // To start tasks batch enter from Redis console the command - hset subscribeOnFrom tasks:count 30 (where 30 is tasks count - from 10 to 50)            
            KeyEvent eventCmdSet = KeyEvent.HashSet;
            // все ключи положить в константы
            string eventKeyFrom = _constant.GetEventKeyFrom; // "subscribeOnFrom" - ключ для подписки на команду запуска эмулятора сервера
            string eventFieldFrom = _constant.GetEventFieldFrom; // "count" - поле для подписки на команду запуска эмулятора сервера

            TimeSpan ttl = TimeSpan.FromDays(_constant.GetKeyFromTimeDays); // срок хранения ключа eventKeyFrom

            string eventKeyRun = _constant.GetEventKeyRun; // "task:run" - ключ и поле для подписки на ключи задач, создаваемые сервером (или эмулятором)
            string eventFieldRun = _constant.GetEventFieldRun;

            // сервер кладёт название поля ключа в заранее обусловленную ячейку ("task:run/Guid") и тут её можно прочитать
            string eventGuidFieldRun = await _subscribe.FetchGuidFieldTaskRun(eventKeyRun, eventFieldRun);

            _subscribe.SubscribeOnEventRun(eventKeyRun, eventCmdSet, eventGuidFieldRun, 1); // 1 - номер сервера, потом можно заменить на guid

            while (IsCancellationNotYet())
            {
                var keyStroke = Console.ReadKey();

                if (keyStroke.Key == ConsoleKey.W)
                {
                    _logger.LogInformation("ConsoleKey was received {KeyStroke}.", keyStroke.Key);
                }
            }
        }

        private bool IsCancellationNotYet()
        {
            return !_cancellationToken.IsCancellationRequested; // add special key from Redis?
        }
    }
}

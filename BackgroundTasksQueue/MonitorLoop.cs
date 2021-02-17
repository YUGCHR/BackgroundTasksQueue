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
            // концепция хищных бэк-серверов, борющихся за получение задач
            // контроллеров же в лесу (на фронте) много и желудей, то есть задач, у них тоже много
            // а несколько (много) серверов могут неспешно выполнять задачи из очереди в бэкграунде

            // ----------------- вы находитесь здесь









            // To start tasks batch enter from Redis console the command - hset subscribeOnFrom tasks:count 30 (where 30 is tasks count - from 10 to 50)            
            KeyEvent eventCmdSet = KeyEvent.HashSet;
            string eventKeyFrom = _constant.GetEventKeyFrom; // "subscribeOnFrom" - key to find guid-field with tasks package
            string eventFieldFrom = _constant.GetEventFieldFrom; // "count" - field

            TimeSpan ttl = TimeSpan.FromDays(_constant.GetKeyFromTimeDays); // срок хранения ключа eventKeyFrom (unused here)

            string eventKeyRun = _constant.GetEventKeyRun; // "task:run" - ключ и поле для подписки на ключи задач, создаваемые сервером (или эмулятором)
            string eventFieldRun = _constant.GetEventFieldRun; // "ttt" - temporary base field to fetch the actual field

            // сервер кладёт название поля ключа в заранее обусловленную ячейку ("task:run/Guid") и тут её можно прочитать
            string eventGuidFieldRun = await _subscribe.FetchGuidFieldTaskRun(eventKeyRun, eventFieldRun);

            _subscribe.SubscribeOnEventRun(eventKeyRun, eventCmdSet, eventGuidFieldRun, 1); // 1 - номер сервера, потом можно заменить на guid





            // после получения задачи фронт опрашивает (не подписка) ключ eventKeyBackReadiness и получает список полей - это готовые к работе бэк-сервера
            // дальше фронт выбирает первое поле или случайнее (так надёжнее?) и удаляет его - забирает заявку
            string capturedBackServerGuid = await CaptureBackServerGuid(eventKeysSet.EventKeyBackReadiness);


            // затем фронт создаёт в ключе кафе (eventKeyFrontGivesTask) поле с захваченным guid бэка, а в значение кладёт имя ключа (тоже guid) пакета задач
            // или кафе не создавать, а сразу идти на ключ (guid бэк-сервера) для получения задачи
            // кафе позволяет стороннему процессу узнать количество серверов за работой - для чего ещё может понадобиться кафе?


            // создаём имя ключа, содержащего пакет задач 
            string taskPackageGuid = Guid.NewGuid().ToString();
            
            
            // в методе FrontServerSetTasks записываем ключ пакета задач в ключ eventKeyFrontGivesTask, а в сам ключ - сами задачи
            // можно положить новые переменные тоже в eventKeysSet
            int inPackageTaskCount = await FrontServerSetTasks(taskPackage, eventKeysSet, taskPackageGuid, capturedBackServerGuid);
            // можно возвращать количество созданных задач и проверять, что не нуль - но это чтобы хоть что-то проверять (или проверять наличие созданных ключей)
            // на создание ключа с пакетом задач уйдёт заметное время, поэтому кафе оправдано - можно будет положить этот ключ в кафе на имя сервера после его создания
            if (inPackageTaskCount > 0)
            {
                //then all rirght
            }

            // бэк подписан на ключ кафе (или на ключ свой guid, если без кафе) и получив сообщение о событии, проверяет своё поле (или сразу берёт задачу)
            // начав работу, бэк кладёт в ключ сообщение о ходе выполнения пакета и/или отдельной задачи (типовой класс - номер цикла, всего цикла, время цикла, всего время и так далее)
            // окончив задачу, бэк должен вернуть поле со своим guid на биржу
            // но сначала проверить сколько там есть свободных серверов - если больше х + какой-то запас, тогда просто раствориться
            // ключ об отчёте выполнения останется на заданное время и потом тоже исчезнет
            // сервер может подписываться на свои процессы и следить за ходом их выполнения - сразу увидеть, когда процесс выполнит все задачи
            // и контроллер может подписаться на сервер, от которого ждёт инфы о ходе выполнения
            // сделать метод, проверяющий показатели всех задач сервера и возвращающий интегральный показатель общего прогресса
            // одновременное количество потоков серверу брать из своих настроек


            //await _cache.SetHashedAsync(eventKeyRun, eventFieldRun, packageGuid, ttl); // создаём ключ ("task:run"), на который подписана очередь и в значении передаём имя ключа, содержащего пакет задач

            //_logger.LogInformation("Key {0}, field {1} with {2} KeyName was set.", eventKeyRun, eventFieldRun, packageGuid);





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

        private async Task<string> CaptureBackServerGuid(string eventKeyBackReadiness)
        {
            // secede in method            
            // проверить, что ключ вообще существует, это автоматически означает, что в нём есть хоть одно поле - есть свободный сервер

            bool isExistEventKeyBackReadiness = await _cache.KeyExistsAsync(eventKeyBackReadiness);
            if (isExistEventKeyBackReadiness)
            {
                // после получения задачи фронт опрашивает ключ eventKeyBackReadiness и получает список полей
                IDictionary<string, string> taskPackage = await _cache.GetHashedAllAsync<string>(eventKeyBackReadiness);

                // дальше фронт выбирает первое поле или случайнее (так надёжнее?) и удаляет его - забирает заявку

                // если удаление получилось, значит, бэк-сервер получен и можно ставить ему задачу
                // если удаление не прошло, фронт (в цикле) опять опрашивает ключ
                // если полей в ключе нет, ключ исчезнет - надо что-то предусмотреть
                // например, не брать задачу, если в списке только один сервер/поле - подождать X секунд и ещё раз опросить ключ
                // после удачного захвата сервера надо дать команду на запуск ещё одного бэка - восстановить свободное количество


                foreach (var t in taskPackage)
                {
                    var (backServerGuid, unusedValue) = t; // пока пробуем первое поле
                    string capturedBackServerGuid = backServerGuid;
                    // пробуем удалить поле ключа - захватить свободный сервер
                    bool isDeleteSuccess = await _cache.RemoveHashedAsync(eventKeyBackReadiness, backServerGuid);
                    _logger.LogInformation("Background server No: {0} captured successfully = {1}.", backServerGuid, isDeleteSuccess);
                    if (isDeleteSuccess)
                    {
                        return capturedBackServerGuid;
                    }
                    // если удаление не удалось, берём следующее поле (номер сервера)
                }
            }
            // если захват сервера не удался совсем, то надо что-то сделать, пока сообщаем
            _logger.LogInformation("Background server capture was failed, total attempts = {1}.", backServerGuid, isDeleteSuccess);
            return default;
        }
    }
}

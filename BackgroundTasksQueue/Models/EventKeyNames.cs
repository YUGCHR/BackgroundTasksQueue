using CachingFramework.Redis.Contracts;
using System;

namespace BackgroundTasksQueue.Models
{
    public class EventKeyNames
    {
        public int TaskDelayTimeInSeconds { get; set; }
        public int BalanceOfTasksAndProcesses { get; set; }
        public int MaxProcessesCountOnServer { get; set; }
        public int MinBackProcessesServersCount { get; set; } // for FrontEmulator only
        public string EventKeyFrom { get; set; }
        public string EventFieldFrom { get; set; }
        public KeyEvent EventCmd { get; set; }
        public string EventKeyBackReadiness { get; set; }
        public string EventFieldBack { get; set; }
        public string EventKeyFrontGivesTask { get; set; }
        public string PrefixRequest { get; set; }
        public string PrefixPackage { get; set; }
        public string PrefixTask { get; set; }
        public string PrefixBackServer { get; set; }
        public string BackServerGuid { get; set; }
        public string BackServerPrefixGuid { get; set; }
        public string PrefixProcessAdd { get; set; }
        public string PrefixProcessCancel { get; set; }
        public string PrefixProcessCount { get; set; }
        public string EventFieldFront { get; set; }
        public string EventKeyBacksTasksProceed { get; set; }        
        public TimeSpan EventKeyFromTimeDays { get; set; }
        public TimeSpan EventKeyBackReadinessTimeDays { get; set; }
        public TimeSpan EventKeyFrontGivesTaskTimeDays { get; set; }
        public TimeSpan EventKeyBackServerMainTimeDays { get; set; }
        public TimeSpan EventKeyBackServerAuxiliaryTimeDays { get; set; }
        public TimeSpan PercentsKeysExistingTimeInMinutes { get; set; } // for Controller only

    }
}

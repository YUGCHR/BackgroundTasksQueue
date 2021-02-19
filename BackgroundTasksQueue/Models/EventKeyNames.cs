using CachingFramework.Redis.Contracts;
using System;

namespace BackgroundTasksQueue.Models
{
    public class EventKeyNames
    {
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
        public string EventFieldFront { get; set; }
        public string EventKeyBacksTasksProceed { get; set; }        
        public TimeSpan Ttl { get; set; }
    }
}

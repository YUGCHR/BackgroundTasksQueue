using System;
using Microsoft.Extensions.Configuration;

namespace BackgroundTasksQueue.Services
{
    public interface ISettingConstants
    {
        public int GetRecordActualityLevel { get; }
        public int GetTaskDelayTimeInSeconds { get; }
        public int GetPercentsKeysExistingTimeInMinutes { get; }
        public int GetKeyFromTimeDays { get; }
        public string GetEventKeyFrom { get; }
        public string GetEventFieldFrom { get; }
        public string GetEventKeyBackReadiness { get; }
        public string GetEventKeyFrontGivesTask { get; }
        public string GetEventFieldBack { get; }
        public string GetEventFieldFront { get; }
        public string GetEventKeyBacksTasksProceed { get; }
    }

    public class SettingConstants : ISettingConstants
    {
        private readonly int _getRecordActualityLevel;
        private readonly int _getTaskDelayTimeInSeconds;
        private readonly int _getPercentsKeysExistingTimeInMinutes;
        private readonly int _getEventKeyFromTimeDays;
        private readonly string _getEventKeyFrom;
        private readonly string _getEventFieldFrom;
        private readonly string _getEventKeyBackReadiness;
        private readonly string _getEventKeyFrontGivesTask;
        private readonly string _getEventFieldBack;
        private readonly string _getEventFieldFront;
        private readonly string _getEventKeyBacksTasksProceed;

        public SettingConstants(IConfiguration configuration)
        {
            Configuration = configuration;

            string recordActualityLevel = Configuration.GetSection("SettingConstants").GetSection("Constants").GetSection("RecordActualityLevel").Value;
            _getRecordActualityLevel = Convert.ToInt32(recordActualityLevel);
            _getTaskDelayTimeInSeconds = Convert.ToInt32(Configuration.GetSection("SettingConstants").GetSection("Constants").GetSection("TaskDelayTimeInSeconds").Value);
            _getPercentsKeysExistingTimeInMinutes = Convert.ToInt32(Configuration.GetSection("SettingConstants").GetSection("Constants").GetSection("PercentsKeysExistingTimeInMinutes").Value);

            _getEventKeyFromTimeDays = Convert.ToInt32(Configuration.GetSection("SettingConstants").GetSection("RedisKeysTimes").GetSection("eventKeyFromTimeDays").Value);

            _getEventKeyFrom = Configuration.GetSection("SettingConstants").GetSection("RedisKeys").GetSection("eventKeyFrom").Value;
            _getEventFieldFrom = Configuration.GetSection("SettingConstants").GetSection("RedisKeys").GetSection("eventFieldFrom").Value;
            _getEventKeyBackReadiness = Configuration.GetSection("SettingConstants").GetSection("RedisKeys").GetSection("eventKeyBackReadiness").Value;
            _getEventKeyFrontGivesTask = Configuration.GetSection("SettingConstants").GetSection("RedisKeys").GetSection("eventKeyFrontGivesTask").Value;
            _getEventFieldBack = Configuration.GetSection("SettingConstants").GetSection("RedisKeys").GetSection("eventFieldBack").Value;
            _getEventFieldFront = Configuration.GetSection("SettingConstants").GetSection("RedisKeys").GetSection("eventFieldFront").Value;
            _getEventKeyBacksTasksProceed = Configuration.GetSection("SettingConstants").GetSection("RedisKeys").GetSection("eventKeyBacksTasksProceed").Value;
        }
        private IConfiguration Configuration { get; }

        public int GetRecordActualityLevel => _getRecordActualityLevel;
        public int GetTaskDelayTimeInSeconds => _getTaskDelayTimeInSeconds;
        public int GetPercentsKeysExistingTimeInMinutes => _getPercentsKeysExistingTimeInMinutes;
        public int GetKeyFromTimeDays => _getEventKeyFromTimeDays;
        public string GetEventKeyFrom => _getEventKeyFrom;
        public string GetEventFieldFrom => _getEventFieldFrom;
        public string GetEventKeyBackReadiness => _getEventKeyBackReadiness;
        public string GetEventKeyFrontGivesTask => _getEventKeyFrontGivesTask;
        public string GetEventFieldBack => _getEventFieldBack;
        public string GetEventFieldFront => _getEventFieldFront;
        public string GetEventKeyBacksTasksProceed => _getEventKeyBacksTasksProceed;

    }

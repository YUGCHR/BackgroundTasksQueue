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
        public string GetEventKeyRun { get; }
        public string GetEventFieldRun { get; }
    }

    public class SettingConstants : ISettingConstants
    {
        private readonly int _getRecordActualityLevel;
        private readonly int _getTaskDelayTimeInSeconds;
        private readonly int _getPercentsKeysExistingTimeInMinutes;
        private readonly int _getEventKeyFromTimeDays;
        private readonly string _getEventKeyFrom;
        private readonly string _getEventFieldFrom;
        private readonly string _getEventKeyRun;
        private readonly string _getEventFieldRun;


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
            _getEventKeyRun = Configuration.GetSection("SettingConstants").GetSection("RedisKeys").GetSection("eventKeyRun").Value;
            _getEventFieldRun = Configuration.GetSection("SettingConstants").GetSection("RedisKeys").GetSection("eventFieldRun").Value;
        }
        private IConfiguration Configuration { get; }

        public int GetRecordActualityLevel => _getRecordActualityLevel;
        public int GetTaskDelayTimeInSeconds => _getTaskDelayTimeInSeconds;
        public int GetPercentsKeysExistingTimeInMinutes => _getPercentsKeysExistingTimeInMinutes;
        public int GetKeyFromTimeDays => _getEventKeyFromTimeDays;
        public string GetEventKeyFrom => _getEventKeyFrom;
        public string GetEventFieldFrom => _getEventFieldFrom;
        public string GetEventKeyRun => _getEventKeyRun;
        public string GetEventFieldRun => _getEventFieldRun;

    }
}

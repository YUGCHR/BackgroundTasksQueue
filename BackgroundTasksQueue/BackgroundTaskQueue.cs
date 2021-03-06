﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace BackgroundTasksQueue
{
    public interface IBackgroundTaskQueue
    {
        void QueueBackgroundWorkItem(Func<CancellationToken, Task> workItem);

        Task<Func<CancellationToken, Task>> DequeueAsync(CancellationToken cancellationToken);
    }

    public class BackgroundTaskQueue : IBackgroundTaskQueue
    {
        private readonly ILogger<BackgroundTaskQueue> _logger;
        private ConcurrentQueue<Func<CancellationToken, Task>> _workItems = new ConcurrentQueue<Func<CancellationToken, Task>>();
        private SemaphoreSlim _signal = new SemaphoreSlim(0);

        public BackgroundTaskQueue(ILogger<BackgroundTaskQueue> logger)
        {
            _logger = logger;
        }

        public void QueueBackgroundWorkItem(Func<CancellationToken, Task> workItem)
        {
            if (workItem == null)
            {
                throw new ArgumentNullException(nameof(workItem));
            }

            _logger.LogInformation("Task {Name} received in the Queue.", nameof(workItem));
            // Adds an object to the end of the System.Collections.Concurrent.ConcurrentQueue`1
            _workItems.Enqueue(workItem);
            _signal.Release();
            _logger.LogInformation("QueueBackgroundWorkItem finished");
        }

        public async Task<Func<CancellationToken, Task>> DequeueAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("BEFORE await _signal.WaitAsync");
            await _signal.WaitAsync(cancellationToken);
            _logger.LogInformation("AFTER await _signal.WaitAsync");
            _workItems.TryDequeue(out var workItem);
            _logger.LogInformation("AFTER _workItems.TryDequeue");


            return workItem;
        }
    }
}


// Ported from Azure.Messaging.ServiceBus.Tests.Processor.SessionProcessorLiveTests
using Azure.Messaging.ServiceBus;

namespace AlmostServiceBus.SdkLive.Tests;

public class SessionProcessorLiveTests : SdkLiveTestBase
{
    [Theory]
    [InlineData(1, false)]
    [InlineData(5, true)]
    public async Task ProcessSessionMessage(int numThreads, bool autoComplete)
    {
        var queueName = await CreateQueueAsync(enableSession: true);
        await using var client = CreateClient(60);
        var sender = client.CreateSender(queueName);

        var sessionId = Guid.NewGuid().ToString();
        var messageSendCt = numThreads * 2;
        using var batch = await sender.CreateMessageBatchAsync();
        AddMessages(batch, messageSendCt, sessionId);
        await sender.SendMessagesAsync(batch);

        var options = new ServiceBusSessionProcessorOptions
        {
            MaxConcurrentSessions = numThreads,
            AutoCompleteMessages = autoComplete,
        };
        await using var processor = client.CreateSessionProcessor(queueName, options);
        int messageCt = 0;

        var completionSources = Enumerable.Range(0, numThreads)
            .Select(_ => new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously))
            .ToArray();
        var completionSourceIndex = -1;

        processor.ProcessMessageAsync += async args =>
        {
            try
            {
                Assert.Equal(sessionId, args.SessionId);
                if (!autoComplete)
                    await args.CompleteMessageAsync(args.Message);
                Interlocked.Increment(ref messageCt);
            }
            finally
            {
                var setIndex = Interlocked.Increment(ref completionSourceIndex);
                if (setIndex < numThreads)
                    completionSources[setIndex].SetResult(true);
            }
        };
        processor.ProcessErrorAsync += ExceptionHandler;
        await processor.StartProcessingAsync();

        await Task.WhenAll(completionSources.Select(s => s.Task));
        await processor.StopProcessingAsync();

        Assert.True(messageCt >= numThreads, $"Expected >= {numThreads}, got {messageCt}");
    }

    [Fact]
    public async Task ProcessMessagesFromMultipleNamedSessions()
    {
        var queueName = await CreateQueueAsync(enableSession: true);
        await using var client = CreateClient(60);
        var sender = client.CreateSender(queueName);

        var sessions = new[] { "session-1", "session-2", "session-3" };
        foreach (var sessionId in sessions)
        {
            using var batch = await sender.CreateMessageBatchAsync();
            AddMessages(batch, 5, sessionId);
            await sender.SendMessagesAsync(batch);
        }

        var options = new ServiceBusSessionProcessorOptions
        {
            MaxConcurrentSessions = 3,
            SessionIds = { "session-1", "session-2", "session-3" },
            AutoCompleteMessages = true,
        };
        await using var processor = client.CreateSessionProcessor(queueName, options);

        var receivedSessions = new System.Collections.Concurrent.ConcurrentBag<string>();
        int messageCt = 0;
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        processor.ProcessMessageAsync += args =>
        {
            receivedSessions.Add(args.SessionId);
            if (Interlocked.Increment(ref messageCt) >= 15)
                tcs.TrySetResult(true);
            return Task.CompletedTask;
        };
        processor.ProcessErrorAsync += ExceptionHandler;
        await processor.StartProcessingAsync();

        var completed = await Task.WhenAny(tcs.Task, Task.Delay(TimeSpan.FromSeconds(30)));
        await processor.StopProcessingAsync();

        Assert.Same(tcs.Task, completed);
        Assert.Equal(15, messageCt);
        Assert.Contains("session-1", receivedSessions);
        Assert.Contains("session-2", receivedSessions);
        Assert.Contains("session-3", receivedSessions);
    }

    [Fact]
    public async Task ProcessConsumesAllMessages()
    {
        var queueName = await CreateQueueAsync(enableSession: true);
        await using var client = CreateClient(60);
        var sender = client.CreateSender(queueName);

        var sessionId = Guid.NewGuid().ToString();
        var messageCount = 20;
        for (int i = 0; i < messageCount; i++)
        {
            var msg = GetMessage(sessionId);
            await sender.SendMessageAsync(msg);
        }

        var options = new ServiceBusSessionProcessorOptions
        {
            MaxConcurrentSessions = 1,
            AutoCompleteMessages = true,
        };
        await using var processor = client.CreateSessionProcessor(queueName, options);

        int messageCt = 0;
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        processor.ProcessMessageAsync += args =>
        {
            if (Interlocked.Increment(ref messageCt) >= messageCount)
                tcs.TrySetResult(true);
            return Task.CompletedTask;
        };
        processor.ProcessErrorAsync += ExceptionHandler;
        await processor.StartProcessingAsync();

        var completed = await Task.WhenAny(tcs.Task, Task.Delay(TimeSpan.FromSeconds(30)));
        await processor.StopProcessingAsync();

        Assert.Same(tcs.Task, completed);
        Assert.Equal(messageCount, messageCt);
    }

    [Fact]
    public async Task UserCallbackThrowingCausesMessageToBeAbandoned()
    {
        var queueName = await CreateQueueAsync(enableSession: true);
        await using var client = CreateClient(60);
        var sender = client.CreateSender(queueName);
        var sessionId = Guid.NewGuid().ToString();
        await sender.SendMessageAsync(GetMessage(sessionId));

        var options = new ServiceBusSessionProcessorOptions
        {
            MaxConcurrentSessions = 1,
            AutoCompleteMessages = false,
        };
        await using var processor = client.CreateSessionProcessor(queueName, options);

        int deliveryCount = 0;
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        processor.ProcessMessageAsync += args =>
        {
            var ct = Interlocked.Increment(ref deliveryCount);
            if (ct == 1)
                throw new InvalidOperationException("user callback error");

            // Second delivery: complete
            args.CompleteMessageAsync(args.Message).GetAwaiter().GetResult();
            tcs.SetResult(true);
            return Task.CompletedTask;
        };
        processor.ProcessErrorAsync += _ => Task.CompletedTask;

        await processor.StartProcessingAsync();
        var completed = await Task.WhenAny(tcs.Task, Task.Delay(TimeSpan.FromSeconds(30)));
        await processor.StopProcessingAsync();

        Assert.Same(tcs.Task, completed);
        Assert.True(deliveryCount >= 2, $"Expected >= 2 deliveries, got {deliveryCount}");
    }

    [Fact]
    public async Task GetAndSetSessionStateInProcessor()
    {
        var queueName = await CreateQueueAsync(enableSession: true);
        await using var client = CreateClient(60);
        var sender = client.CreateSender(queueName);
        var sessionId = Guid.NewGuid().ToString();
        await sender.SendMessageAsync(GetMessage(sessionId));

        var options = new ServiceBusSessionProcessorOptions
        {
            MaxConcurrentSessions = 1,
            AutoCompleteMessages = true,
        };
        await using var processor = client.CreateSessionProcessor(queueName, options);
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        processor.ProcessMessageAsync += async args =>
        {
            var state = await args.GetSessionStateAsync();
            Assert.Null(state);

            await args.SetSessionStateAsync(new BinaryData("test-state"));
            state = await args.GetSessionStateAsync();
            Assert.NotNull(state);
            Assert.Equal("test-state", state.ToString());

            tcs.SetResult(true);
        };
        processor.ProcessErrorAsync += ExceptionHandler;

        await processor.StartProcessingAsync();
        var completed = await Task.WhenAny(tcs.Task, Task.Delay(TimeSpan.FromSeconds(30)));
        await processor.StopProcessingAsync();

        Assert.Same(tcs.Task, completed);
    }
}

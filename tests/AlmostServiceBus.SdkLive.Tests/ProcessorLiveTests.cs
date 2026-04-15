// Ported from Azure.Messaging.ServiceBus.Tests.Processor.ProcessorLiveTests
using Azure.Messaging.ServiceBus;

namespace AlmostServiceBus.SdkLive.Tests;

public class ProcessorLiveTests : SdkLiveTestBase
{
    [Theory]
    [InlineData(1, false)]
    [InlineData(5, true)]
    [InlineData(10, false)]
    public async Task ProcessMessages(int numThreads, bool autoComplete)
    {
        var queueName = await CreateQueueAsync();
        await using var client = CreateClient(60);
        var sender = client.CreateSender(queueName);

        var messageSendCt = numThreads * 2;
        using var batch = await sender.CreateMessageBatchAsync();
        AddMessages(batch, messageSendCt);
        await sender.SendMessagesAsync(batch);

        var options = new ServiceBusProcessorOptions
        {
            MaxConcurrentCalls = numThreads,
            AutoCompleteMessages = autoComplete,
            PrefetchCount = 20
        };
        await using var processor = client.CreateProcessor(queueName, options);
        int messageCt = 0;

        var completionSources = Enumerable.Range(0, numThreads)
            .Select(_ => new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously))
            .ToArray();
        var completionSourceIndex = -1;

        processor.ProcessMessageAsync += async args =>
        {
            try
            {
                if (!autoComplete)
                    await args.CompleteMessageAsync(args.Message, args.CancellationToken);
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

        Assert.True(messageCt >= numThreads, $"Expected >= {numThreads} but got {messageCt}");
        Assert.True(messageCt <= messageSendCt, $"Expected <= {messageSendCt} but got {messageCt}");
    }

    [Theory]
    [InlineData(1)]
    [InlineData(5)]
    [InlineData(10)]
    public async Task UserSettlingWithAutoCompleteDoesNotThrow(int numThreads)
    {
        var queueName = await CreateQueueAsync();
        await using var client = CreateClient();
        var sender = client.CreateSender(queueName);

        var messageSendCt = numThreads * 2;
        using var batch = await sender.CreateMessageBatchAsync();
        AddMessages(batch, messageSendCt);
        await sender.SendMessagesAsync(batch);

        var options = new ServiceBusProcessorOptions
        {
            MaxConcurrentCalls = numThreads,
            AutoCompleteMessages = true,
        };
        await using var processor = client.CreateProcessor(queueName, options);

        var completionSources = Enumerable.Range(0, numThreads)
            .Select(_ => new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously))
            .ToArray();
        var completionSourceIndex = -1;

        processor.ProcessMessageAsync += async args =>
        {
            try
            {
                // Explicitly settling with auto-complete should not throw
                await args.CompleteMessageAsync(args.Message, args.CancellationToken);
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
    }

    [Fact]
    public async Task CanStopProcessingFromHandler()
    {
        var queueName = await CreateQueueAsync();
        await using var client = CreateClient();
        var sender = client.CreateSender(queueName);
        await sender.SendMessageAsync(GetMessage());

        await using var processor = client.CreateProcessor(queueName);
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        processor.ProcessMessageAsync += async args =>
        {
            await args.CompleteMessageAsync(args.Message);
            // Stop on a background task so we don't deadlock waiting for our own handler.
            _ = Task.Run(() => processor.StopProcessingAsync());
            tcs.SetResult(true);
        };
        processor.ProcessErrorAsync += ExceptionHandler;

        await processor.StartProcessingAsync();
        await tcs.Task;
        // Wait briefly for IsProcessing to flip — the SDK transitions the flag when the
        // background StopProcessing task completes, which happens after the handler returns.
        for (int i = 0; i < 50 && processor.IsProcessing; i++)
            await Task.Delay(100);
        Assert.False(processor.IsProcessing);
    }

    [Fact]
    public async Task OnMessageExceptionHandlerCalled()
    {
        var queueName = await CreateQueueAsync();
        await using var client = CreateClient();
        var sender = client.CreateSender(queueName);
        await sender.SendMessageAsync(GetMessage());

        await using var processor = client.CreateProcessor(queueName, new ServiceBusProcessorOptions
        {
            AutoCompleteMessages = false
        });
        var errorTcs = new TaskCompletionSource<ProcessErrorEventArgs>(TaskCreationOptions.RunContinuationsAsynchronously);

        processor.ProcessMessageAsync += _ => throw new TestException();
        processor.ProcessErrorAsync += args =>
        {
            if (args.Exception is TestException)
                errorTcs.TrySetResult(args);
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync();
        var errorArgs = await errorTcs.Task;
        Assert.IsType<TestException>(errorArgs.Exception);
        await processor.StopProcessingAsync();
    }

    [Fact]
    public async Task StartStopMultipleTimes()
    {
        var queueName = await CreateQueueAsync();
        await using var client = CreateClient();
        var sender = client.CreateSender(queueName);

        for (int i = 0; i < 3; i++)
        {
            await sender.SendMessageAsync(GetMessage());
        }

        await using var processor = client.CreateProcessor(queueName);
        int messageCt = 0;

        processor.ProcessMessageAsync += async args =>
        {
            await args.CompleteMessageAsync(args.Message);
            Interlocked.Increment(ref messageCt);
        };
        processor.ProcessErrorAsync += ExceptionHandler;

        for (int i = 0; i < 3; i++)
        {
            await processor.StartProcessingAsync();
            await Task.Delay(1000);
            await processor.StopProcessingAsync();
        }

        Assert.True(messageCt >= 1, $"Should have processed at least 1 message, got {messageCt}");
    }

    [Fact]
    public async Task ProcessorStopsWhenClientIsClosed()
    {
        var queueName = await CreateQueueAsync();
        await using var client = CreateClient();
        var sender = client.CreateSender(queueName);
        await sender.SendMessageAsync(GetMessage());

        var processor = client.CreateProcessor(queueName);
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        processor.ProcessMessageAsync += async args =>
        {
            await args.CompleteMessageAsync(args.Message);
            tcs.SetResult(true);
        };
        processor.ProcessErrorAsync += _ => Task.CompletedTask;

        await processor.StartProcessingAsync();
        await tcs.Task;

        await client.DisposeAsync();
        // Wait briefly for IsProcessing to flip after dispose
        for (int i = 0; i < 50 && processor.IsProcessing; i++)
            await Task.Delay(100);
        Assert.False(processor.IsProcessing);
    }

    [Fact]
    public async Task ProcessDlq()
    {
        var queueName = await CreateQueueAsync();
        await using var client = CreateClient();
        var sender = client.CreateSender(queueName);
        await sender.SendMessageAsync(GetMessage());

        // Send to DLQ
        var receiver = client.CreateReceiver(queueName);
        var msg = await receiver.ReceiveMessageAsync();
        Assert.NotNull(msg);
        await receiver.DeadLetterMessageAsync(msg);

        // Process DLQ
        var dlqPath = $"{queueName}/$deadletterqueue";
        await using var dlqProcessor = client.CreateProcessor(dlqPath);
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        dlqProcessor.ProcessMessageAsync += async args =>
        {
            await args.CompleteMessageAsync(args.Message);
            tcs.SetResult(true);
        };
        dlqProcessor.ProcessErrorAsync += ExceptionHandler;

        await dlqProcessor.StartProcessingAsync();
        var completed = await Task.WhenAny(tcs.Task, Task.Delay(TimeSpan.FromSeconds(30)));
        Assert.Same(tcs.Task, completed);
        await dlqProcessor.StopProcessingAsync();
    }

    [Fact]
    public async Task UserCallbackThrowingCausesMessageToBeAbandonedIfNotSettled()
    {
        var queueName = await CreateQueueAsync();
        await using var client = CreateClient();
        var sender = client.CreateSender(queueName);
        await sender.SendMessageAsync(GetMessage());

        await using var processor = client.CreateProcessor(queueName, new ServiceBusProcessorOptions
        {
            AutoCompleteMessages = false,
            MaxConcurrentCalls = 1
        });

        var errorCount = 0;
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        processor.ProcessMessageAsync += args =>
        {
            if (Interlocked.Increment(ref errorCount) == 1)
                throw new InvalidOperationException("user callback error");

            // On second delivery, complete it
            args.CompleteMessageAsync(args.Message).GetAwaiter().GetResult();
            tcs.SetResult(true);
            return Task.CompletedTask;
        };
        processor.ProcessErrorAsync += _ => Task.CompletedTask;

        await processor.StartProcessingAsync();
        var completed = await Task.WhenAny(tcs.Task, Task.Delay(TimeSpan.FromSeconds(30)));
        Assert.Same(tcs.Task, completed);
        await processor.StopProcessingAsync();

        Assert.True(errorCount >= 2, $"Expected >= 2 deliveries due to abandon, got {errorCount}");
    }

    private class TestException : Exception { }
}

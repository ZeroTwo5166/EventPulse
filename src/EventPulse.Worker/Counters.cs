namespace EventPulse.Worker;

public sealed class Counters
{
    private long _deadLettered;
    private long _duplicates;
    private long _retries;
    private long _deadLetterWriteRetries;

    public long DeadLettered => Interlocked.Read(ref _deadLettered);
    public long Duplicates => Interlocked.Read(ref _duplicates);
    public long Retries => Interlocked.Read(ref _retries);
    public long DeadLetterWriteRetries => Interlocked.Read(ref _deadLetterWriteRetries);

    public void AddDeadLettered() => Interlocked.Increment(ref _deadLettered);
    public void AddDuplicate() => Interlocked.Increment(ref _duplicates);
    public void AddRetry() => Interlocked.Increment(ref _retries);
    public void AddDeadLetterWriteRetry() => Interlocked.Increment(ref _deadLetterWriteRetries);
}

namespace EventPulse.Worker;

internal sealed class Counters
{
    private long _deadLettered;
    private long _duplicates;

    public long DeadLettered => Interlocked.Read(ref _deadLettered);
    public long Duplicates => Interlocked.Read(ref _duplicates);

    public void AddDeadLettered() => Interlocked.Increment(ref _deadLettered);
    public void AddDuplicate() => Interlocked.Increment(ref _duplicates);
}

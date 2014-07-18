namespace EventBunny
{
    public interface IEventPublisher
    {
        void Dispatch<T>(EventMessage<T> processedEvent);
    }
}
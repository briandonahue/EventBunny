namespace EventBunny
{
    public interface IEventDispatcher
    {
        void StartDispatching();
        void StopDispatching();
    }
}
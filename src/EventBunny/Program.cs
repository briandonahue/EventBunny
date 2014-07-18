using Topshelf;

namespace EventBunny
{
    internal class Program
    {

        static void Main(string[] args)
        {
            HostFactory.Run(configurator =>
                configurator.Service<DispatcherHost>(s =>
                {
                    s.ConstructUsing(c => new DispatcherHost());
                    s.WhenStarted((service, hostControl) => service.Start(hostControl));
                    s.WhenStopped((service, hostControl) => service.Stop(hostControl));

                    configurator.RunAsLocalSystem();
                    configurator.SetDisplayName("EventBunny");
                    configurator.SetDescription("Dispatch Events from EventStore to RabbitMQ");
                    configurator.SetServiceName("EventBunny");
                }));




        }
    }
}
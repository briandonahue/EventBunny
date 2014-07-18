using System;
using System.Net;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using RabbitMQ.Client;
using Topshelf;

namespace EventBunny
{
    public class DispatcherHost
    {
        EventStoreDispatcher _storeDispatcher;

        public DispatcherHost()
        {
            var settings = ConnectionSettings.Create().UseDebugLogger();
            settings.SetDefaultUserCredentials(new UserCredentials("admin", "changeit"));

            var connection =
                EventStoreConnection.Create(settings,
                                            new IPEndPoint(
                                                IPAddress.Parse("127.0.0.1"),
                                                Int32.Parse("1113")));
            connection.Connect();

            var factory = new ConnectionFactory
                {
                    UserName = "guest",
                    Password = "guest",
                    VirtualHost = "/",
                    Protocol = Protocols.FromEnvironment(),
                    HostName = "localhost",
                    Port = AmqpTcpEndpoint.UseDefaultPort
                };
            var conn = factory.CreateConnection();
            _storeDispatcher = new EventStoreDispatcher(connection, new RabbitPublisher(conn));
        }

        public bool Start(HostControl hostControl)
        {
            _storeDispatcher.StartDispatching();
            return true;
        }

        public bool Stop(HostControl hostControl)
        {
            _storeDispatcher.StopDispatching();
            return true;
        }
    }
}
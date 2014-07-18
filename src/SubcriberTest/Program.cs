using System;
using System.Text;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.MessagePatterns;

namespace SubcriberTest
{
    class Program
    {
        static void Main(string[] args)
        {

            var serializationSettings = new JsonSerializerSettings {TypeNameHandling = TypeNameHandling.None};
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
            var channel = conn.CreateModel();

            var queue = "testSubscriber";
                    channel.QueueDeclare(queue, true, false, false, null);
                    channel.QueueBind(queue, "testQueue", "");

            var sub = new Subscription(channel, queue);
            foreach (BasicDeliverEventArgs e in sub)
            {
                Console.WriteLine(Encoding.UTF8.GetString(e.Body));
                sub.Ack(e);
            }


            Console.ReadKey();


        }
    }
}

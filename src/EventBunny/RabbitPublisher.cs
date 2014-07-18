using System;
using System.Text;
using Newtonsoft.Json;
using RabbitMQ.Client;

namespace EventBunny
{
    public class RabbitPublisher : IEventPublisher
    {
        readonly IConnection _conn;
        IModel _channel;

        public RabbitPublisher(IConnection conn)
        {
            if (conn == null) throw new ArgumentNullException("conn");
            if (!conn.IsOpen) throw new Exception("RabbitMQ Connection is closed.");
            _channel = conn.CreateModel();
        }

        public void Dispatch<T>(EventMessage<T> processedEvent)
        {
                    var exchange = processedEvent.EventClrTypeName.Split(',')[0];
                    _channel.ExchangeDeclare(exchange, ExchangeType.Fanout);
            var json = JsonConvert.SerializeObject(processedEvent, Constants.JsonSerializerSettings);
            Console.WriteLine(json);
            _channel.BasicPublish(exchange, "", null, 
                        Encoding.UTF8.GetBytes(json));
        }
    }
}
using System;
using System.Collections.Generic;

namespace EventBunny
{
    public class EventMessage<T>
    {
        public Guid EventId { get; set; }

        public string StreamName { get; set; }

        public int EventNumber { get; set; }

        public string EventType { get; set; }

        public Dictionary<string, object> MetaData { get; set; }

        public T Data { get; set; }

        public string EventClrTypeName { get; set; }
    }
}
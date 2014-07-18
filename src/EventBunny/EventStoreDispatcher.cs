using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Xml.Serialization;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Common.Concurrent;
using Newtonsoft.Json;

namespace EventBunny
{
    public class EventStoreDispatcher : IEventDispatcher
    {
        const int RECONNECT_TIMEOUT_MILLISEC = 5000;
        const int THREAD_KILL_TIMEOUT_MILLISEC = 5000;
        const int READ_PAGE_SIZE = 500;
        const int LIVE_QUEUE_SIZE_LIMIT = 10000;

        readonly IEventPublisher _eventPublisher;

        readonly ManualResetEventSlim _historicalDone = new ManualResetEventSlim(true);
        readonly ConcurrentQueue<ResolvedEvent> _historicalQueue = new ConcurrentQueue<ResolvedEvent>();
        readonly ManualResetEventSlim _liveDone = new ManualResetEventSlim(true);
        readonly ConcurrentQueue<ResolvedEvent> _liveQueue = new ConcurrentQueue<ResolvedEvent>();
        readonly XmlSerializer _serializer = new XmlSerializer(typeof (Position));
        readonly IEventStoreConnection _store;

        int _isPublishing;
        Position _lastProcessed;

        volatile bool _livePublishingAllowed;
        const string _positionFile = @"position.json";
        volatile bool _stop;
        EventStoreSubscription _subscription;

        public EventStoreDispatcher(IEventStoreConnection store, IEventPublisher eventPublisher)
        {
            if (store == null) throw new ArgumentNullException("store");
            if (eventPublisher == null) throw new ArgumentNullException("eventPublisher");

            _store = store;
            _eventPublisher = eventPublisher;
            if (File.Exists(_positionFile))
            {
                dynamic o = JsonConvert.DeserializeObject(File.ReadAllText(_positionFile), Constants.JsonSerializerSettings);
                _lastProcessed = new Position((long)o.CommitPosition, (long)o.PreparePosition);
            }
            else
            {
                _lastProcessed = new Position(-1, -1);
            }
        }

        public Position LastProcessed
        {
            get { return _lastProcessed; }
            set
            {
                var json = JsonConvert.SerializeObject(value, Constants.JsonSerializerSettings);
                File.WriteAllText(_positionFile, json);
                _lastProcessed = value;
            }
        }

        // Credit algorithm to Szymon Pobiega
        // http://simon-says-architecture.com/2013/02/02/mechanics-of-durable-subscription/#comments
        // 1. The subscriber always starts with pull assuming there were some messages generated while it was offline
        // 2. The subscriber pulls messages until there’s nothing left to pull (it is up to date with the stream)
        // 3. Push subscription is started  but arriving messages are not processed immediately but temporarily redirected to a buffer
        // 4. One last pull is done to ensure nothing happened between step 2 and 3
        // 5. Messages from this last pull are processed
        // 6. Processing messages from push buffer is started. While messages are processed, they are checked against IDs of messages processed in step 5 to ensure there’s no duplicates.
        // 7. System works in push model until subscriber is killed or subscription is dropped by publisher drops push subscription.

        //Credit to Andrii Nakryiko
        //If data is written to storage at such a speed, that between the moment you did your last 
        //pull read and the moment you subscribed to push notifications more data (events) were 
        //generated, than you request in one pull request, you would need to repeat steps 4-5 few 
        //times until you get a pull message which position is >= subscription position 
        //(EventStore provides you with those positions).
        public void StartDispatching()
        {
            RecoverSubscription();
        }

        void RecoverSubscription()
        {
            _livePublishingAllowed = false;
            _liveDone.Wait(); // wait until all live processing is finished (queue is empty, LastProcessed updated)

            if (LastProcessed == new Position(-1, -1))
            {
                _subscription = SubscribeToAll();

            }
            else
            {
                var startPos = LastProcessed == new Position(-1, -1) ? Position.Start : LastProcessed;
                var nextPos = ReadHistoricalEventsFrom(startPos);

                _subscription = SubscribeToAll();

                ReadHistoricalEventsFrom(nextPos);
                _historicalDone.Wait(); // wait until historical queue is empty and LastProcessed updated
            }

            _livePublishingAllowed = true;
            EnsurePublishEvents(_liveQueue, _liveDone);
        }

        public void StopDispatching()
        {
            _stop = true;
            if (_subscription != null)
                _subscription.Unsubscribe();
            // hopefully additional check in PublishEvents (additional check for _stop after setting event) prevents race conditions
            if (!_historicalDone.Wait(THREAD_KILL_TIMEOUT_MILLISEC))
                throw new TimeoutException("Unable to stop dispatching in time.");
            if (!_liveDone.Wait(THREAD_KILL_TIMEOUT_MILLISEC))
                throw new TimeoutException("Unable to stop dispatching in time.");
        }

        Position ReadHistoricalEventsFrom(Position from)
        {
            var position = from;
            AllEventsSlice slice;
            while (!_stop && (slice = _store.ReadAllEventsForward(position, READ_PAGE_SIZE, false)).Events.Length > 0)
            {
                foreach (var rawEvent in slice.Events)
                {
                    _historicalQueue.Enqueue(rawEvent);
                }
                EnsurePublishEvents(_historicalQueue, _historicalDone);

                position = slice.NextPosition;
            }
            return position;
        }

        EventStoreSubscription SubscribeToAll()
        {
            //TODO: Before trying to resubscribe - how to ensure that store is active and ready to accept.
            //AN: EventStoreConnection automatically tries to connect (if not already connected) to EventStore,
            //so you don't have to do something manually
            //Though in case of errors, you need to do some actions (if EventStore server is down or not yet up, etc)

            var task = _store.SubscribeToAllAsync(false, HandleEventAppeared, HandleSubscriptionDropped);
            if (!task.Wait(RECONNECT_TIMEOUT_MILLISEC))
                throw new TimeoutException("Could not reconnect after the subscription was dropped");
            return task.Result;
        }

        void HandleEventAppeared(EventStoreSubscription eventStoreSubscription, ResolvedEvent rawEvent)
        {
            if (_stop) return;

            _liveQueue.Enqueue(rawEvent);

            //Prevent live queue memory explosion.
            if (!_livePublishingAllowed && _liveQueue.Count > LIVE_QUEUE_SIZE_LIMIT)
            {
                ResolvedEvent throwAwayEvent;
                _liveQueue.TryDequeue(out throwAwayEvent);
            }

            if (_livePublishingAllowed)
                EnsurePublishEvents(_liveQueue, _liveDone);
        }

        void HandleSubscriptionDropped(EventStoreSubscription eventStoreSubscription,
                                       SubscriptionDropReason subscriptionDropReason, Exception arg3)
        {
            if (_stop) return;

            RecoverSubscription();
        }

        void EnsurePublishEvents(ConcurrentQueue<ResolvedEvent> queue, ManualResetEventSlim doneEvent)
        {
            if (_stop) return;

            if (Interlocked.CompareExchange(ref _isPublishing, 1, 0) == 0)
                ThreadPool.QueueUserWorkItem(_ => PublishEvents(queue, doneEvent));
        }

        void PublishEvents(ConcurrentQueue<ResolvedEvent> queue, ManualResetEventSlim doneEvent)
        {
            var keepGoing = true;
            while (keepGoing)
            {
                doneEvent.Reset(); // signal we start processing this queue
                if (_stop)
                    // this is to avoid race condition in StopDispatching, though it is 1AM here, so I could be wrong :)
                {
                    doneEvent.Set();
                    Interlocked.CompareExchange(ref _isPublishing, 0, 1);
                    return;
                }
                ResolvedEvent evnt;
                while (!_stop && queue.TryDequeue(out evnt))
                {
                    if (evnt.OriginalPosition > LastProcessed) // this ensures we don't process same events twice
                    {
                        var processedEvent = ProcessRawEvent(evnt);
                        if (processedEvent != null)
                            _eventPublisher.Dispatch(processedEvent);
                        LastProcessed = evnt.OriginalPosition.Value;
                    }
                }
                doneEvent.Set(); // signal end of processing particular queue
                Interlocked.CompareExchange(ref _isPublishing, 0, 1);
                // try to reacquire lock if needed
                keepGoing = !_stop && queue.Count > 0 && Interlocked.CompareExchange(ref _isPublishing, 1, 0) == 0;
            }
        }

        EventMessage<object> ProcessRawEvent(ResolvedEvent rawEvent)
        {
            if (rawEvent.OriginalEvent.Metadata.Length > 0 && 
                rawEvent.OriginalEvent.Data.Length > 0 &&
                !rawEvent.OriginalEvent.EventType.StartsWith("$"))
                return DeserializeEvent(rawEvent.OriginalEvent);
            return null;
        }

        /// <summary>
        /// Deserializes the event from the raw GetEventStore event to my event.
        /// </summary>
        /// <param name="originalEvent"></param>
        /// <returns></returns>
        static EventMessage<object> DeserializeEvent(RecordedEvent originalEvent)
        {
            var headers =
                JsonConvert.DeserializeObject<Dictionary<string, object>>(
                    Encoding.UTF8.GetString(originalEvent.Metadata), Constants.JsonSerializerSettings);
            var data = JsonConvert.DeserializeObject(Encoding.UTF8.GetString(originalEvent.Data),
                                                     Constants.JsonSerializerSettings);
            var eventClrTypeName = headers["EventClrTypeName"].ToString();

            var e = new EventMessage<object>
            {
                EventId = originalEvent.EventId,
                StreamName = originalEvent.EventStreamId,
                EventNumber = originalEvent.EventNumber,
                EventType = originalEvent.EventType,
                EventClrTypeName = eventClrTypeName,
                MetaData = headers,
                Data = data,
            };

            return e;
        }
    }
}
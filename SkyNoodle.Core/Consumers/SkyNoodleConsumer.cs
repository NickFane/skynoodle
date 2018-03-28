using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;
using SkyNoodle.Core.Models;
using SkyNoodle.Core.Processors;
using SkyNoodle.Core.Serialization;

namespace SkyNoodle.Core.Consumers
{
    public class SkyNoodleConsumer : IDisposable
    {
        private readonly Consumer<string, ScramJetMessage> _consumer;

        private readonly IDictionary<string, List<IStreamProcessor>> _streamProcessors;

        private readonly IDictionary<string, List<Action<string>>> _streamFunctions;

        /// <summary>
        /// Initialises the consumer which will connect to the provided Kafka cluster.
        /// </summary>
        /// <param name="brokerList">The brokers of the Kafka cluster that the consumer will connect to</param>
        /// <param name="consumerGroup">Consumer group that this consumer will use</param>
        public SkyNoodleConsumer(string brokerList, string consumerGroup)
        {
            _streamProcessors = new Dictionary<string, List<IStreamProcessor>>();
            _streamFunctions = new Dictionary<string, List<Action<string>>>();

            var config = new Dictionary<string, object>()
            {
                { "bootstrap.servers", brokerList },
                { "group.id", consumerGroup },
                { "enable.auto.commit", true },
                { "auto.commit.interval.ms", 60000 },
                { "default.topic.config", new Dictionary<string, object>()
                    {
                        { "auto.offset.reset", "smallest" }
                    }
                }
            };

            _consumer = new Consumer<string, ScramJetMessage>(config, new StringDeserializer(Encoding.UTF8), new ScramJetFormatDeserialiser());
            _consumer.OnError += OnError;
            _consumer.OnConsumeError += OnConsumeError;
            _consumer.OnPartitionsAssigned += OnPartitionsAssigned;
            _consumer.OnPartitionsRevoked += OnPartitionsRevoked;
        }

        /// <summary>
        /// Adds/Replaces the function associated with the MessageType
        /// </summary>
        /// <param name="messageType">MessageType that will trigger the function</param>
        /// <param name="streamFunction">The function which will be invoked when the MessageType is received</param>
        /// <returns></returns>
        public SkyNoodleConsumer WithStreamProcessor(string messageType, Action<string> streamFunction)
        {
            if (_streamFunctions.ContainsKey(messageType))
            {
                _streamFunctions[messageType] = new List<Action<string>>() { streamFunction };
                return this;
            }

            _streamFunctions.Add(messageType, new List<Action<string>> { streamFunction });
            return this;
        }

        /// <summary>
        /// Adds/Replaces the processor associated with the MessageType
        /// </summary>
        /// <param name="messageType">MessageType that will trigger the processor</param>
        /// <param name="streamProcessor">The processor which will be invoked when the MessageType is received</param>
        /// <returns></returns>
        public SkyNoodleConsumer WithStreamProcessor(string messageType, IStreamProcessor streamProcessor)
        {
            if (_streamProcessors.ContainsKey(messageType))
            {
                _streamProcessors[messageType] = new List<IStreamProcessor>() { streamProcessor };
                return this;
            }

            _streamProcessors.Add(messageType, new List<IStreamProcessor> { streamProcessor });
            return this;
        }

        /// <summary>
        /// Sets the list of topics that the consumer will listen to
        /// This operation is not accumulative. It replaces the current list of topics.
        /// </summary>
        /// <param name="topics">List of topics on the kafka cluster that the consumer will listen to</param>
        /// <returns></returns>
        public SkyNoodleConsumer WithTopics(IEnumerable<string> topics)
        {
            _consumer.Subscribe(topics);
            return this;
        }

        /// <summary>
        /// Sets the topic that the consumer will listen to
        /// This operation is not accumulative. It replaces the current list of topics being listened to.
        /// </summary>
        /// <param name="topic">The topic on the kafka cluster that the consumer will listen to</param>
        /// <returns></returns>
        public SkyNoodleConsumer WithTopics(string topic)
        {
            _consumer.Subscribe(topic);
            return this;
        }

        /// <summary>
        /// A method which will read a message regardless of message type, and invoke a stream or processor associated with it
        /// If no messages are on the queue, or the next message is not a supported message type, the method will simply return
        /// </summary>
        /// <param name="timeout"></param>
        public void ReceiveMessage(TimeSpan timeout)
        {
            if (!_consumer.Consume(out var message, timeout))
            {
                return;
            }

            var messagetype = message.Value.MessageType;

            if (_streamProcessors.TryGetValue(messagetype, out var streamProcessor))
            {
                var payload = message.Value.Payload.ToString();
                streamProcessor.ForEach(processor => processor.ProcessMessage(payload));
                return;
            }

            if (_streamFunctions.TryGetValue(messagetype, out var streamFunction))
            {
                var payload = message.Value.Payload.ToString();
                streamFunction.ForEach(function => function.Invoke(payload));
            }
        }

        private void OnConsumeError(object sender, Message e)
        {
            Console.Error.WriteLine($"{e}");
        }

        private void OnError(object sender, Error e)
        {
            Console.Error.WriteLine($"{e}");
        }

        private void OnPartitionsRevoked(object sender, List<TopicPartition> partitions)
        {
            Console.WriteLine($"Revoked partitions: [{string.Join(", ", partitions)}]");
            _consumer.Unassign();
        }

        private void OnPartitionsAssigned(object sender, List<TopicPartition> partitions)
        {
            Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}], member id: {_consumer.MemberId}");
            _consumer.Assign(partitions);
        }

        public void Dispose()
        {
            _consumer?.Dispose();
        }

    }
}

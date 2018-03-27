using System;
using System.Collections.Generic;
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
        private readonly Dictionary<string, object> _config;
        private readonly Consumer<string, ScramJetMessage> _consumer;

        private readonly IDictionary<string, IStreamProcessor> _streamProcessors;

        // Not sure
        private readonly IDictionary<string, Action<string>> _streamFunctions;

        public SkyNoodleConsumer(string brokerList, string consumerGroup)
        {
            _streamProcessors = new Dictionary<string, IStreamProcessor>();
            _streamFunctions = new Dictionary<string, Action<string>>();

            _config = new Dictionary<string, object>()
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

            _consumer = new Consumer<string, ScramJetMessage>(_config, new StringDeserializer(Encoding.UTF8), new ScramJetFormatDeserialiser());
            _consumer.OnError += OnError;
            _consumer.OnConsumeError += OnConsumeError;
            _consumer.OnPartitionsAssigned += OnPartitionsAssigned;
            _consumer.OnPartitionsRevoked += OnPartitionsRevoked;
        }

        public SkyNoodleConsumer WithStreamProcessor(string messageType, Action<string> streamFunction)
        {
            if (_streamFunctions.TryAdd(messageType, streamFunction))
            {
                return this;
            }

            Console.WriteLine($"Failed to add stream processor assigned to MessageType: {messageType}");
            return this;
        }

        public SkyNoodleConsumer WithTopics(params string[] topics)
        {
            _consumer.Subscribe(topics);
            return this;
        }

        private void OnConsumeError(object sender, Message e)
        {
            Console.Error.WriteLine($"{e}");
        }

        private void OnError(object sender, Error e)
        {
            Console.Error.WriteLine($"{e}");
        }

        public SkyNoodleConsumer WithStreamProcessor(string messageType, IStreamProcessor streamProcessor)
        {
            if (_streamProcessors.TryAdd(messageType, streamProcessor))
            {
                return this;
            }

            Console.Error.WriteLine($"Failed to add stream processor assigned to MessageType: {messageType}");
            return this;
        }

        public void ReceiveMessage(TimeSpan timeout)
        {
            if (!_consumer.Consume(out var message, timeout))
            {
                return;
            }

            var messagetype = message.Value.MessageType;

            if (_streamProcessors.TryGetValue(messagetype, out var streamProcessor))
            {
                streamProcessor.ProcessMessage(message.Value.Payload.ToString());
                return;
            }

            if (_streamFunctions.TryGetValue(messagetype, out var streamFunction))
            {
                streamFunction.Invoke(message.Value.Payload.ToString());
            }
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

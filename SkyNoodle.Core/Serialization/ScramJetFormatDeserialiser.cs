using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using SkyNoodle.Core.Models;

namespace SkyNoodle.Core.Serialization
{
    internal class ScramJetFormatDeserialiser : IDeserializer<ScramJetMessage>
    {
        public ScramJetMessage Deserialize(string topic, byte[] data)
        {
            var message = Encoding.UTF8.GetString(data);
            try
            {
                return JsonConvert.DeserializeObject<ScramJetMessage>(message, new JsonSerializerSettings() { ContractResolver = new CustomContractResolver(false) });
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
        {
            return config;
        }
    }
}

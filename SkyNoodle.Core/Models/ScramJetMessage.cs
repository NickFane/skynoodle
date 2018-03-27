using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;

namespace SkyNoodle.Core.Models
{
    public class ScramJetMessage
    {
        public string MessageType { get; set; }
        public object Payload { get; set; }
        public string Id { get; set; }
        public DateTime Timestamp { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Text;
using SkyNoodle.Core.Models;

namespace SkyNoodle.Core.Processors
{
    public interface IStreamProcessor
    {
        void ProcessMessage(string messagePayload);
    }
}

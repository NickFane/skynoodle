using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace SkyNoodle.Core.Serialization
{
    public class CustomContractResolver : DefaultContractResolver
    {
        public bool UseJsonPropertyName { get; }

        public CustomContractResolver(bool useJsonPropertyName)
        {
            UseJsonPropertyName = useJsonPropertyName;
        }

        /// <summary>
        /// Custom property name binding which will override property.PropertyName to set it to camel case.
        /// </summary>
        /// <param name="member"></param>
        /// <param name="memberSerialization"></param>
        /// <returns></returns>
        protected override JsonProperty CreateProperty(MemberInfo member, MemberSerialization memberSerialization)
        {
            var property = base.CreateProperty(member, memberSerialization);
            if (!UseJsonPropertyName)
            {
                property.PropertyName = property.UnderlyingName.ToCamelCase();
            }

            return property;
        }

    }

    internal static class Extensions
    {
        public static string ToCamelCase(this string input)
        {
            return char.ToLowerInvariant(input[0]) + input.Substring(1);
        }
    }
}

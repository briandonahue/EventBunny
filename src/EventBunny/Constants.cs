using Newtonsoft.Json;

namespace EventBunny
{
    public class Constants
    {
            public static readonly JsonSerializerSettings JsonSerializerSettings = new JsonSerializerSettings {TypeNameHandling = TypeNameHandling.None};
         
    }
}
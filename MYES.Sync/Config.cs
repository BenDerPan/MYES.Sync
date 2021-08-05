using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using YamlDotNet;
using YamlDotNet.Serialization;

namespace MYES
{
    public class Config
    {
        public string MySqlConnectionString { get; set; }

        public List<string> SyncDatabases{ get; set; }

        public List<string> IgnoreDatabases { get; set; }

        public Config()
        {
            MySqlConnectionString= "server=localhost;port=3306;uid=test;pwd=test;charset=utf8";
            SyncDatabases = new List<string>();
            IgnoreDatabases = new List<string>();
        }


        public bool Save(string cfgFile = "myes.yaml")
        {
            try
            {
                using (TextWriter w = File.CreateText(cfgFile))
                {
                    var serializer = new Serializer();
                    w.Write(serializer.Serialize(this));
                }

                return true;
            }
            catch (Exception e)
            {
                return false;
            }
        }
        
        public static Config Load(string cfgFile = "myes.yaml")
        {
            try
            {
                using (TextReader r = File.OpenText(cfgFile))
                {
                    Deserializer deserializer = new Deserializer();
                    return deserializer.Deserialize<Config>(r);
                }
            }
            catch (Exception)
            {

                return null;
            }
        }
    }


}

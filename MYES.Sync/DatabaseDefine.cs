using System;
using System.Collections.Generic;
using System.Text;

namespace MYES
{
    public class DatabaseDefine
    {
        public string SchemaName { get; private set; }

        public string DefaultCharacterSetName { get; private set; }

        public string DefaultCollationName { get; private set; }

        public DatabaseDefine(string schemaName,string defaultCharacterSetName,string defaultCollationName)
        {
            SchemaName = schemaName;
            DefaultCharacterSetName = defaultCharacterSetName;
            DefaultCollationName = defaultCollationName;
        }
    }
}

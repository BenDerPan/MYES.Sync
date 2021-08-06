using System;
using System.Collections.Generic;
using System.Text;

namespace MYES
{
    public class TableDefine
    {
        public string TableName { get; private set; }

        public TableDefine(string tableName)
        {
            TableName = tableName;
        }
    }
}

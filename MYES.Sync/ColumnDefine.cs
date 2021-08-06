using System;
using System.Collections.Generic;
using System.Text;

namespace MYES
{
    public class ColumnDefine
    {
        public string ColumnName { get; private set; }

        public string DataType { get; private set; }

        public bool IsPrimaryKey{ get; private set; }

        public ColumnDefine(string colName,string dataType,bool isPrimary)
        {
            ColumnName = colName;
            DataType = dataType;
            IsPrimaryKey = isPrimary;
        }
    }
}

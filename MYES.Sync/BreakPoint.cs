using System;
using System.Collections.Generic;
using System.Text;

namespace MYES
{
    public class BreakPoint
    {
        public string Prefix { get; set; }

        public string DatabaseName { get; set; }
        public string TableName { get; set; }

        public long ProcessedCount { get; set; }

        public BreakPoint(string dbName,string tableName, string prefix = "",long processedCount=0)
        {
            Prefix = prefix;
            DatabaseName = dbName;
            TableName = tableName;
            ProcessedCount = processedCount;
        }

        public void SetProcessedCount(int processedCount)
        {
            ProcessedCount = processedCount>=0?processedCount:0;
        }

        public void AddProcessedCount(int count)
        {
            if (count>0)
            {
                ProcessedCount += count;
            }
        }

        public string GetKey()
        {
            return GetKey(DatabaseName, TableName, Prefix);
        }

        public static string GetKey(string dbName,string tableName,string prefix="")
        {
            if (!string.IsNullOrWhiteSpace(prefix))
            {
                return $"{prefix}__{dbName}__{tableName}";
            }
            else
            {
                return $"{dbName}__{tableName}";
            }
        }
    }
}

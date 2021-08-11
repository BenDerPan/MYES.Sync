using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using MySql.Data.MySqlClient;
using Nest;

namespace MYES
{
    public class SyncLoader
    {
        Config _cfg;
        public SyncLoader(string cfgFile = "myes.yaml")
        {
            _cfg = Config.Load(cfgFile);
        }
        
        public void Start()
        {
            var es = ESBulk.GetElasticClient(new Uri(_cfg.ElasticSearchUris[0]));
            var conStr = _cfg.MySqlConnectionString;
            using (MySqlConnection conn = new MySqlConnection(conStr))
            {
                conn.Open();

                var dbs = GetAllDatabses(conn);

                foreach (var db in dbs)
                {
                    var tables = GetDatabaseTables(conn, db.SchemaName);
                    foreach (var table in tables)
                    {
                        var colums = GetTableColumns(conn, db.SchemaName, table.TableName);

                        CreateTableIndex(es,conn,db, table, colums);


                    }
                }
            }
        }

        long GetMysqlTableRecordCount(MySqlConnection conn, string dbName, string tableName)
        {
            long retCount = 0;
            var sql = $"select count(*) from `{dbName}`.`{tableName}`";

            MySqlCommand cmd = new MySqlCommand(sql, conn);
            MySqlDataReader reader = null;
            using (reader = cmd.ExecuteReader())
            {
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        try
                        {
                            retCount = (long)reader.GetValue(0);
                        }
                        catch (Exception)
                        {
                            retCount = 0;
                        }
                    }
                }
            }

            return retCount;

        }


        public void CreateTableIndex(IElasticClient es,MySqlConnection conn ,DatabaseDefine db,TableDefine table,List<ColumnDefine> columns)
        {
            var tableRecordCount = GetMysqlTableRecordCount(conn, db.SchemaName, table.TableName);
            if (tableRecordCount <= 0)
            {
                Console.WriteLine($"Database: {db.SchemaName}, Table:{table.TableName} is empty, will ignore......");
                return;
            }

            var breakPoint = BreakPointRecorder.Current.Get<BreakPoint>(BreakPoint.GetKey(db.SchemaName, table.TableName, _cfg.IndexPrefix));
            if (breakPoint==null)
            {
                breakPoint = new BreakPoint(db.SchemaName, table.TableName, _cfg.IndexPrefix);
            }

            var indexName = breakPoint.GetKey();

            //检查索引是否存在
            ESBulk.CreateIndex(es, indexName, out var isIndexExist,_cfg.NumberOfReplicas,_cfg.NumberOfShards);
            if (!isIndexExist)
            {
                //初始化创建索引后，直接填充一条默认数据结构，用于初始化ES Mapping。
                Dictionary<string, object> indexMapping = new Dictionary<string, object>();
                for (int i = 0; i < columns.Count; i++)
                {
                    indexMapping[columns[i].ColumnName] = GetDefaultValue(columns[i].DataType);
                }

                if (ESBulk.BulkAll(es, indexName, new List<Dictionary<string, object>> { indexMapping }, bulkAllResponse: out var res))
                {
                    Console.WriteLine("Init table index mapping......OK");
                }
                else
                {

                }

                Console.WriteLine($"Create index: {indexName} ...OK");
            }

            var sql = $"select {string.Join(",", columns.Select(s => $"`{s.ColumnName}`"))} from `{db.SchemaName}`.`{table.TableName}` limit {tableRecordCount} offset {breakPoint.ProcessedCount}";

            
            MySqlCommand cmd = new MySqlCommand(sql, conn);
            MySqlDataReader reader = null;
            try
            {
                Console.WriteLine($"Executing SQL: {sql}");
                int totalCount = 0;
                int successCount = 0;
                using (reader = cmd.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        List<Dictionary<string, object>> bulkList = new List<Dictionary<string, object>>();
                        while (reader.Read())
                        {
                            var dict = new Dictionary<string, object>();
                            for (int i = 0; i < columns.Count; i++)
                            {
                                try
                                {
                                    var value = reader.GetValue(i);
                                    if (value == DBNull.Value)
                                    {
                                        dict[columns[i].ColumnName] = GetDefaultValue(columns[i].DataType);
                                    }
                                    else
                                    {
                                        dict[columns[i].ColumnName] = reader.GetValue(i);
                                    }

                                }
                                catch (Exception e)
                                {
                                    dict[columns[i].ColumnName] = GetDefaultValue(columns[i].DataType);
                                }


                            }

                            bulkList.Add(dict);

                            if (bulkList.Count >= 1000)
                            {
                                var isOk = ESBulk.BulkAll<Dictionary<string, object>>(es, indexName, new List<Dictionary<string, object>>(bulkList), bulkAllResponse: out var res);

                                if (res!=null)
                                {
                                    var okCount = res.Items.Where(s => s.IsValid).ToList().Count;
                                    successCount += okCount;
                                    breakPoint.AddProcessedCount(okCount);
                                    BreakPointRecorder.Current.Set<BreakPoint>(breakPoint.GetKey(), breakPoint);
                                }
                                
                                
                                bulkList.Clear();

                                if (!isOk)
                                {
                                    break;
                                }
                            }

                            totalCount++;
                        }

                        if (bulkList.Count > 0)
                        {
                            var isOk = ESBulk.BulkAll<Dictionary<string, object>>(es, indexName, new List<Dictionary<string, object>>(bulkList), bulkAllResponse: out var res);

                            if (res != null)
                            {
                                var okCount = res.Items.Where(s => s.IsValid).ToList().Count;
                                successCount += okCount;
                                breakPoint.AddProcessedCount(okCount);
                                BreakPointRecorder.Current.Set<BreakPoint>(breakPoint.GetKey(), breakPoint);
                            }

                            bulkList.Clear();
                        }
                    }
                }

                Console.WriteLine($"Total: {totalCount}, Success: {successCount}");
            }
            catch (Exception e)
            {
                Console.WriteLine($"Import Error：{e}");
            }

        }

        private object GetDefaultValue(string dataType)
        {
            switch (dataType)
            {
                case "varchar":
                    return "";
                case "longtext":
                    return "";
                case "bigint":
                    return 0;
                case "datetime":
                    return DateTime.MinValue;
                case "time":
                    return DateTime.MinValue;
                case "timestamp":
                    return 0;
                case "int":
                    return 0;
                case "tinyint":
                    return 0;
                case "integer":
                    return 0;
                case "decimal":
                    return 0.0;
                case "double":
                    return 0.0;
                case "float":
                    return 0.0;
                case "text":
                    return "";
                case "mediumtext":
                    return "";
                case "tinytext":
                    return "";
                case "char":
                    return "";
                case "enum":
                    return 0;
                case "blob":
                    return "";
                case "tinyblob":
                    return "";
                default:
                    return  "";
            }
        }

        public List<DatabaseDefine> GetAllDatabses(MySqlConnection conn)
        {
            string sql = "select schema_name,default_character_set_name,default_collation_name from information_schema.schemata where schema_name!='information_schema';";
            var dbNames = new List<DatabaseDefine>();
            MySqlCommand cmd = new MySqlCommand(sql, conn);
            MySqlDataReader reader = null;
            try
            {
                using (reader = cmd.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            string dbName = reader.GetString(0);
                            string defaultCharacterSetName = reader.GetString(1);
                            string defaultCollationName = reader.GetString(2);
                            dbNames.Add(new DatabaseDefine(dbName,defaultCharacterSetName,defaultCollationName));
                        }
                    }
                }
            }
            catch (Exception e)
            {

            }

            return dbNames;
        }

        public List<TableDefine> GetDatabaseTables(MySqlConnection conn,string dbName)
        {
            string sql = $"select table_name from information_schema.tables where table_schema='{dbName}';";
            var tableNames = new List<TableDefine>();
            MySqlCommand cmd = new MySqlCommand(sql, conn);
            MySqlDataReader reader = null;
            try
            {
                using (reader = cmd.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            string t = reader.GetString(0);
                            tableNames.Add(new TableDefine(t));
                        }
                    }
                }
            }
            catch (Exception e)
            {

            }

            return tableNames;
        }

        public List<ColumnDefine> GetTableColumns(MySqlConnection conn, string dbName,string tableName)
        {
            string sql = $"select column_name,data_type,column_key from information_schema.columns where table_schema='{dbName}' and table_name='{tableName}';";
            var columnNames = new List<ColumnDefine>();
            MySqlCommand cmd = new MySqlCommand(sql, conn);
            MySqlDataReader reader = null;
            try
            {
                using (reader = cmd.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            string colName = reader.GetString(0);
                            var colType = reader.GetString(1);
                            var isPrimaryKey = reader.GetString(2)=="PRI"?true:false;
                           
                            columnNames.Add(new ColumnDefine(colName,colType,isPrimaryKey));
                        }
                    }
                }
            }
            catch (Exception e)
            {

            }

            return columnNames;
        }
    }
}

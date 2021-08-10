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
        
        public void Test()
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
            if (GetMysqlTableRecordCount(conn, db.SchemaName, table.TableName) <= 0)
            {
                Console.WriteLine($"库：{db.SchemaName}， 表：{table.TableName}数据表为空，跳过......");
                return;
            }



            var indexName = $"index_{db.SchemaName}___{table.TableName}".ToLower();

            //检查索引是否存在
            if (ESBulk.CreateIndex(es,indexName))
            {
                //初始化创建索引后，直接填充一条默认数据结构，用于初始化ES Mapping。
                Dictionary<string, object> indexMapping = new Dictionary<string, object>();
                for (int i = 0; i < columns.Count; i++)
                {
                    indexMapping[columns[i].ColumnName] = GetDefaultValue(columns[i].DataType);
                }

                if (ESBulk.BulkAll(es, indexName, new List<Dictionary<string, object>> { indexMapping }))
                {
                    Console.WriteLine(">>>>>>>>>>>>>>>>>>>>>>>>初始Mapping完成...OK");
                }
                else
                {

                }

                Console.WriteLine($"创建索引:{indexName} 成功...OK");
            }
            var currentPage = 1;
            var pageSize = 1000;
            var sql = $"select {string.Join(",", columns.Select(s => $"`{s.ColumnName}`"))} from `{db.SchemaName}`.`{table.TableName}`";// limit {(currentPage - 1) * pageSize},{pageSize}";

            
            MySqlCommand cmd = new MySqlCommand(sql, conn);
            MySqlDataReader reader = null;
            try
            {
                Console.WriteLine($"执行SQL：{sql}");
                int counter = 0;
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
                                var retryCount = 0;
                                while (!ESBulk.BulkAll<Dictionary<string, object>>(es, indexName, new List<Dictionary<string, object>>(bulkList)))
                                {
                                    bulkList.ForEach(s =>
                                    {
                                        if (!ESBulk.BulkAll<Dictionary<string, object>>(es, indexName, new List<Dictionary<string, object>> { s }))
                                        {

                                        }
                                    });
                                    var sleepSeconds = 30 + 10000 * retryCount;
                                    Console.WriteLine($"[{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}] 速度过快，需要等待索引创建完成！等待{sleepSeconds}秒后重试...");
                                    break;
                                    Thread.Sleep(sleepSeconds);
                                    retryCount++;
                                }

                                bulkList.Clear();

                                //Thread.Sleep(1000);
                            }

                            counter++;
                        }

                        if (bulkList.Count > 0)
                        {
                            var retryCount = 0;
                            while (!ESBulk.BulkAll<Dictionary<string, object>>(es, indexName, new List<Dictionary<string, object>>(bulkList)))
                            {
                                var sleepSeconds = 30 + 10000 * retryCount;
                                Console.WriteLine($"[{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}] 速度过快，需要等待索引创建完成！等待{sleepSeconds}秒后重试...");
                                break;
                                Thread.Sleep(sleepSeconds);
                                retryCount++;
                            }
                            bulkList.Clear();
                            //Thread.Sleep(1000);
                        }
                    }
                }

                Console.WriteLine($"成功导入：{counter} 条数据");
            }
            catch (Exception e)
            {
                Console.WriteLine($"导入异常：{e.Message}");
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

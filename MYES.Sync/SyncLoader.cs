using System;
using System.Collections.Generic;
using System.Text;
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

                        CreateTableIndex(conn,db, table, colums);


                    }
                }
            }
        }

        public void CreateTableIndex(MySqlConnection conn ,DatabaseDefine db,TableDefine table,List<ColumnDefine> columns)
        {
            var node = new Uri("http://localhost:9200");
            var es = new ElasticClient(node);

            var indexName = $"index_{db.SchemaName}___{table.TableName}".ToLower();

            //检查索引是否存在
            if (!es.Indices.Exists(indexName).Exists)
            {
                var res=es.Indices.Create(indexName);

              
               
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

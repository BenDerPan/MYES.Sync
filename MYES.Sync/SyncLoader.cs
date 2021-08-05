using System;
using System.Collections.Generic;
using System.Text;
using MySql.Data.MySqlClient;

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

                var dbNames = GetAllDatabses(conn);

                foreach (var dbName in dbNames)
                {
                    var tableNames = GetDatabaseTables(conn, dbName.Item1);
                    foreach (var tableName in tableNames)
                    {
                        var colDict = GetTableColumns(conn, dbName.Item1, tableName);
                    }
                }
            }
        }

        public List<ValueTuple<string,string,string>> GetAllDatabses(MySqlConnection conn)
        {
            string sql = "select schema_name,default_character_set_name,default_collation_name from information_schema.schemata where schema_name!='information_schema';";
            var dbNames = new List<ValueTuple<string, string, string>>();
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
                            dbNames.Add((dbName,defaultCharacterSetName,defaultCollationName));
                        }
                    }
                }
            }
            catch (Exception e)
            {

            }

            return dbNames;
        }

        public List<string> GetDatabaseTables(MySqlConnection conn,string dbName)
        {
            string sql = $"select table_name from information_schema.tables where table_schema='{dbName}';";
            var tableNames = new List<string>();
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
                            tableNames.Add(t);
                        }
                    }
                }
            }
            catch (Exception e)
            {

            }

            return tableNames;
        }

        public List<ValueTuple<string, string, bool>> GetTableColumns(MySqlConnection conn, string dbName,string tableName)
        {
            string sql = $"select column_name,data_type,column_key from information_schema.columns where table_schema='{dbName}' and table_name='{tableName}';";
            var columnNames = new List<ValueTuple<string, string, bool>>();
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
                           
                            columnNames.Add((colName,colType,isPrimaryKey));
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

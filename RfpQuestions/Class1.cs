using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;

namespace RfpQuestions
{
    [TestFixture]
    internal class DatabaseProfiler
    {
        [Test]
        public void GetRowsByDatabaseAndRowCount()
        {
            var databaseServers = new[] { "WWMAVASQL01", "WWMAVASQL02"};


            foreach(var databaseServer in databaseServers)
            {
                var tempFile = Path.Combine(Path.GetTempPath(), $"{databaseServer}DatabaseTableRowCount.csv");
                var streamWriter = File.CreateText(tempFile);
                streamWriter.WriteLine("Database, Table, Row Count");

                using (var conn = GetConnection(databaseServer))
                {
                    conn.Open();

                    foreach (var database in GetDatabases(conn))
                    {
                        if (!database.StartsWith("Analytics_", System.StringComparison.CurrentCultureIgnoreCase))
                        {
                            continue;
                        }
                        try
                        {
                            conn.ChangeDatabase(database);
                        }
                        catch (Exception ex)
                        {
                            streamWriter.WriteLine($"{database}, {ex.Message}, [NULL]");
                            continue;
                        }

                        var commandText = GetTableCountQuery();
                        using (var command = new SqlCommand(commandText, conn))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    var tableName = reader["TableName"].ToString();
                                    var rowCount = reader["Rows"].ToString();

                                    if (tableName.ToLower().Contains("fact") ||
                                        tableName.ToLower().Contains("dim"))
                                    {
                                        streamWriter.WriteLine($"{database}, {tableName}, {rowCount}");
                                    }
                                }
                            }
                        }
                    }

                }
                streamWriter.Flush();
                streamWriter.Close();
                System.Diagnostics.Process.Start(tempFile);

            }

        }

        public string GetTableCountQuery()
        {
            var query = @"SELECT 
    t.NAME AS TableName,
    i.name as indexName,
    p.[Rows],
    sum(a.total_pages) as TotalPages, 
    sum(a.used_pages) as UsedPages, 
    sum(a.data_pages) as DataPages,
    (sum(a.total_pages) * 8) / 1024 as TotalSpaceMB, 
    (sum(a.used_pages) * 8) / 1024 as UsedSpaceMB, 
    (sum(a.data_pages) * 8) / 1024 as DataSpaceMB
FROM
    sys.tables t
INNER JOIN
    sys.indexes i ON t.OBJECT_ID = i.object_id
INNER JOIN
    sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
INNER JOIN
    sys.allocation_units a ON p.partition_id = a.container_id
WHERE
    t.NAME NOT LIKE 'dt%' AND
    i.OBJECT_ID > 255 AND
    i.index_id <= 1 AND
    p.[Rows] > 0
GROUP BY
    t.NAME, i.object_id, i.index_id, i.name, p.[Rows]
ORDER BY
    object_name(i.object_id) ";
            return query;
        }

        private SqlConnection GetConnection(string serverName)
        {
            var sqlConnectionString = $"Data Source={serverName};" +
                "Integrated Security=SSPI;";
            return new SqlConnection(sqlConnectionString);
        }
        public List<string> GetDatabases(SqlConnection connection)
        {
            var commandText = "SELECT * FROM master.dbo.sysdatabases";
            var command = new SqlCommand(commandText,connection);

            var databases = new List<string>();
            using (SqlDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    databases.Add(reader["name"].ToString());
                }
            }

            return databases;
        }
    }
}

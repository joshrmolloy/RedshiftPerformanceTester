using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using CsvHelper;

namespace RedshiftPerformanceTester
{
    public class Program
    {

        private const string Query = @"SELECT  
        p.accountId,
        p.glaccountname,
        p.buildingId as buildingId,
        p.glAccountId,
        SUM(CASE WHEN p.isCashPosting = True THEN p.amount ELSE 0 END) AS cashAmount,
        SUM(CASE WHEN p.isCashPosting = False THEN p.Amount ELSE 0 END) AS accrualAmount,
SUM(CASE WHEN (p.isCashPosting = True AND p.achBillingId IS NOT NULL AND (p.resultCode IS NULL OR p.resultDate > '20170522')) THEN p.Amount ELSE CAST(0 AS DECIMAL) END) AS pendingAmount,
        p.GLAccountTypeId,
        p.glsubtypeid,
        p.isExcludedCashBalances as isExcluded,
        p.accountingBookId
FROM
        key_acctposting_accountid2 p
WHERE
p.accountid = 126 and
 p.entryDate < '20170522' and glaccountid > 15 -- filter system wide accounts for testing only
GROUP BY p.accountid, p.glaccountname, p.glaccounttypeid, p.glsubtypeid, p.isExcludedCashBalances, p.accountingBookId, p.buildingId, p.glaccountid";

        private IEnumerable<OdbcConnection> connectionPool = new List<OdbcConnection>();
        private const int maxConnections = 4;

        static void Main(string[] args)
        {
            int threadCount = 100;
            using (var sw = new StreamWriter("perfResults.csv"))
            {
                var writer = new CsvWriter(sw);
                writer.WriteHeader<PerfResult>();
                QueryDatabase(writer, threadCount, Query);
            }
            Console.WriteLine("Completed");
        }

        private static async void QueryDatabase(CsvWriter writer, int threadCount, string query)
        {
            var tasks = new List<Task>();
            for (var i = 0; i < threadCount; i++)
            {
                var threadId = i;
                tasks.Add(Task.Run(() => QueryDatabaseAsync(writer, threadId.ToString(), query)));
                //Thread.Sleep(100);
            }
            Task.WaitAll(tasks.ToArray());
        }

        private static void QueryDatabaseAsync(CsvWriter writer, string threadId, string query)
        {
            OdbcConnection odbcConnection = null;
            OdbcCommand command = null;
            OdbcDataReader odbcReader = null;
            try
            {
                Console.WriteLine($"Thread {threadId} started...");
                var connectionString = "Dsn=redshifttest;";
                odbcConnection = new OdbcConnection(connectionString);
                command = new OdbcCommand(query, odbcConnection);
                odbcConnection.Open();
                var watch = System.Diagnostics.Stopwatch.StartNew();
                odbcReader = command.ExecuteReader(CommandBehavior.CloseConnection);
                watch.Stop();
                var perfResult = new PerfResult
                {
                    ThreadId = threadId,
                    QueryTime = watch.ElapsedMilliseconds
                };
                writer.WriteRecord(perfResult);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error querying redshift instance: {e.Message}");
            }
            finally
            {
                odbcReader?.Close();
                command?.Dispose();
                odbcConnection?.Close();
                Console.WriteLine($"Thread {threadId} finished!");
            }
        }

        private class PerfResult
        {
            public string ThreadId { get; set; }
            public long QueryTime { get; set; }
        }
    }
}

using Hangfire;
using hangfire_webapi.Models;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace hangfire_webapi
{
    public class Startup
    {
        private long? timeForExecutionInSeconds = 80 * 60;

        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddHangfire(x => x.UseSqlServerStorage("Password=eoffice;Persist Security Info=False;User ID=eoffice; Initial Catalog=hangfire-webapi-db; Data Source=srb-content-tst.src.si"));
            services.AddHangfireServer();

            services.AddControllers();            
        }      

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseHangfireDashboard();

            app.UseRouting();

            app.UseAuthorization();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });

            RecurringJob.AddOrUpdate(() => DailyFileGeneratorOrchestration(), "*/10 * * * *");
        }

        public void DailyFileGeneratorOrchestration()
        {

            List<string> filesForProcessing = new List<string>();

            string connectionString = "Password=eoffice;Persist Security Info=False;User ID=eoffice; Initial Catalog=OrdersDb; Data Source=srb-content-tst.src.si";

            SqlConnection connection = null;

            using (connection = new SqlConnection(connectionString))
            {
                connection.Open();

                //string selectFilesForProcessing = $"SELECT Name, Path " +
                //                                  $"FROM Orders " +
                //                                  $"INNER JOIN OrdersJobs ON Orders.Id = OrdersJobs.OrderId " +
                //                                  $"WHERE IsConfirmed = 1 AND IsCompleted = 0 AND CreatedAt = '{DateTime.Now.AddDays(-1).Date}';";

                string selectFilesForProcessing = $"SELECT Name " +
                                                  $"FROM Orders " +
                                                  $"INNER JOIN OrdersJobs ON Orders.Id = OrdersJobs.OrderId ";

                SqlCommand sql_cmnd = new SqlCommand(selectFilesForProcessing, connection);
                SqlDataReader reader = sql_cmnd.ExecuteReader();

                while (reader.Read())
                {
                    filesForProcessing.Add(reader["Name"].ToString());
                }

                connection.Close();
            }

            string jobId1 = BackgroundJob.Enqueue(() => WriteToTxtFile(filesForProcessing[0]));
            string jobid2 = BackgroundJob.ContinueJobWith(jobId1, () => WriteToTxtFile(filesForProcessing[1]));
            string jobId3 = BackgroundJob.ContinueJobWith(jobid2, () => WriteToTxtFile(filesForProcessing[2]));
            string jobId4 = BackgroundJob.ContinueJobWith(jobId3, () => WriteToTxtFile(filesForProcessing[3]));
            string jobId5 = BackgroundJob.ContinueJobWith(jobId4, () => WriteToTxtFile(filesForProcessing[4]));

            //foreach (var fileForProcessing in filesForProcessing)
            //{
            //    string jobId1 = BackgroundJob.Enqueue(() => WriteToTxtFile(fileForProcessing));
            //}
        }

        private bool IsEnoughTimeForExecution(string fileName) 
        {
            bool result = false;


            string csvFileDirectory = @"C:\csv\";
            string csvFilePath = $"{csvFileDirectory}{fileName}.csv";
            FileInfo fileInfo = new FileInfo(csvFilePath);
            long fileSize = fileInfo.Length;

            long processedSizeInBytes = 0;
            double BytesProcessedPerSecond = 0;
            long timeSpent = 0;
            double predictedTimeInSeconds = 0;

            string connectionString = "Password=eoffice;Persist Security Info=False;User ID=eoffice; Initial Catalog=OrdersDb; Data Source=srb-content-tst.src.si";

            SqlConnection connection = null;

            using (connection = new SqlConnection(connectionString))
            {
                connection.Open();

                string countOfCompletedJobsQuery = $"SELECT count(*) " +
                                              $"FROM [OrdersDb].[dbo].[OrdersJobs] " +
                                              $"where IsCompleted = 1 ";

                SqlCommand sql_cmnd = new SqlCommand(countOfCompletedJobsQuery, connection);
                long countOfCompleted = (int)sql_cmnd.ExecuteScalar();
                if (countOfCompleted == 0) 
                {
                    return true;
                }

                string timeSpentQuery = $"SELECT sum([ExecutionTimeInSeconds])" +
                                        $"FROM [OrdersDb].[dbo].[OrdersJobs]" +
                                        $"where IsCompleted = 1 AND CreatedAt = '{DateTime.Now.Date}'";

                sql_cmnd = new SqlCommand(timeSpentQuery, connection);
                timeSpent = (long)sql_cmnd.ExecuteScalar();

                string countFilesForProcessing = $"SELECT sum(AttachmentSizeInBytes) " +
                              $"FROM Orders " +
                              $"INNER JOIN OrdersJobs ON Orders.Id = OrdersJobs.OrderId " +
                              $"WHERE IsConfirmed = 1 AND IsCompleted = 1 AND AttachmentSizeInBytes > 0 AND ExecutionTimeInSeconds > 0 AND CreatedAt = '{DateTime.Now.Date}';";


                sql_cmnd = new SqlCommand(countFilesForProcessing, connection);
                processedSizeInBytes = (long)sql_cmnd.ExecuteScalar();

                BytesProcessedPerSecond = processedSizeInBytes / timeSpent;
                predictedTimeInSeconds = fileSize / BytesProcessedPerSecond;

                result = (timeForExecutionInSeconds - timeSpent >= predictedTimeInSeconds) ? true
                                                                                           : false;              


                connection.Close();
            }

           

            return result;
        }

        private bool IsConfirmed(string fileName) 
        {
            bool result = false;

            string connectionString = "Password=eoffice;Persist Security Info=False;User ID=eoffice; Initial Catalog=OrdersDb; Data Source=srb-content-tst.src.si";

            SqlConnection connection = null;

            using (connection = new SqlConnection(connectionString))
            {
                connection.Open();


                string countFilesForProcessing = $"SELECT COUNT(*) " +
                                              $"FROM Orders " +
                                              $"INNER JOIN OrdersJobs ON Orders.Id = OrdersJobs.OrderId " +
                                              $"WHERE IsConfirmed = 1 AND IsCompleted = 0 AND Name = '{fileName}';";


                SqlCommand sql_cmnd = new SqlCommand(countFilesForProcessing, connection);
                int count = (int)sql_cmnd.ExecuteScalar();

                connection.Close();

                if (count != 0)
                {
                    result = true;
                }
            }

            return result;
        }

        private void UpdateAttachment(string filePath, string fileName)
        {
            FileInfo fileInfo = new FileInfo(filePath);
            double attachmentSize = fileInfo.Length;

            string connectionString = "Password=eoffice;Persist Security Info=False;User ID=eoffice; Initial Catalog=OrdersDb; Data Source=srb-content-tst.src.si";
            SqlConnection connection = null;

            using (connection = new SqlConnection(connectionString))
            {
                connection.Open();

                int id = 0;
                string selectFileForExecuteTimeUpdate = $"SELECT Orders.Id " +
                                                $"FROM Orders " +
                                                $"INNER JOIN OrdersJobs ON Orders.Id = OrdersJobs.OrderId " +
                                                $"WHERE  Name = '{fileName}'";


                SqlCommand sql_cmnd = new SqlCommand(selectFileForExecuteTimeUpdate, connection);
                SqlDataReader reader = sql_cmnd.ExecuteReader();

                if (reader.Read())
                {
                    id = Convert.ToInt32(reader["Id"].ToString());
                }

                reader.Close();


                string updateAttachmentSizeInBytesQuery = $"UPDATE [dbo].[OrdersJobs] " +
                                               $"SET [AttachmentSizeInBytes] = {attachmentSize} " +
                                               $"WHERE Id = {id} ";

                sql_cmnd = new SqlCommand(updateAttachmentSizeInBytesQuery, connection);
                sql_cmnd.ExecuteNonQuery();

                connection.Close();
            }
        }

        public void WriteToTxtFile(string fileName)
        {
            var timer = new Stopwatch();
            timer.Start();

            string message = $"Start procesing {fileName}{System.Environment.NewLine}";
            System.IO.File.AppendAllText(@"C:\txt\log.txt", message);

            string csvFileDirectory = @"C:\csv\";
            string csvFilePath = $"{csvFileDirectory}{fileName}.csv";

            UpdateAttachment(csvFilePath, fileName);

            if (!IsConfirmed(fileName))
            {
                return;
            }

            if (!IsEnoughTimeForExecution(fileName))
            {
                return;
            }



            //Thread.Sleep(10 * 1000);

            List<string> csvLines = System.IO.File.ReadAllLines(csvFilePath).ToList();
            List<OrderItem> orderItems = new List<OrderItem>();

            foreach (var csvLine in csvLines)
            {
                List<string> orderItemParts = csvLine.Split(',').ToList();

                OrderItem orderItem = new OrderItem
                {
                    Id = Convert.ToInt32(orderItemParts[0]),
                    Country = orderItemParts[1],
                    UnitByPiece = Convert.ToDouble(orderItemParts[2]),
                    Pieces = Convert.ToInt32(orderItemParts[3])
                };

                orderItems.Add(orderItem);
            }

            string txtFileDirectory = @"C:\txt\";
            string txtFilePath = $"{txtFileDirectory}{fileName}.txt";

            List<string> txtLines = new List<string>();
            foreach (OrderItem item in orderItems)
            {
                string txtLine = $"[ {item.Country} | {item.UnitByPiece} | {item.Pieces} ]";
                txtLines.Add(txtLine);
            }

            System.IO.File.WriteAllLines(txtFilePath, txtLines);

            if (System.IO.File.Exists(txtFilePath))
            {
                CsvFileProcessingSetToCompleted(fileName);
            }

            message = $"End procesing {fileName}{System.Environment.NewLine}";
            System.IO.File.AppendAllText(@"C:\txt\log.txt", message);

            timer.Stop();

            TimeSpan timeTaken = timer.Elapsed;
            string timeExecution = $"Time taken: {timeTaken.ToString(@"m\:ss\.fff")}{System.Environment.NewLine}" ;

            FileInfo csvAttachmentFile = new FileInfo(csvFilePath);
            long csvAttachmentSize = csvAttachmentFile.Length;
            string csvAttachmentSizeMessage = $"Attachment size: {csvAttachmentSize} bytes.{System.Environment.NewLine}";


            System.IO.File.AppendAllText(@"C:\txt\log.txt", timeExecution);
            System.IO.File.AppendAllText(@"C:\txt\log.txt", csvAttachmentSizeMessage);

            UpdateExecutionTime(fileName, (long)timeTaken.TotalSeconds);
        }

        private void UpdateExecutionTime(string fileName,long executionTimeInSeconds) 
        {
            string connectionString = "Password=eoffice;Persist Security Info=False;User ID=eoffice; Initial Catalog=OrdersDb; Data Source=srb-content-tst.src.si";

            SqlConnection connection = null;
            using (connection = new SqlConnection(connectionString))
            {
                connection.Open();

              
                int id = 0;
                string selectFileForExecuteTimeUpdate = $"SELECT Orders.Id " +
                                                $"FROM Orders " +
                                                $"INNER JOIN OrdersJobs ON Orders.Id = OrdersJobs.OrderId " +
                                                $"WHERE  Name = '{fileName}'";


                SqlCommand sql_cmnd = new SqlCommand(selectFileForExecuteTimeUpdate, connection);
                SqlDataReader reader = sql_cmnd.ExecuteReader();

                if (reader.Read())
                {
                    id = Convert.ToInt32(reader["Id"].ToString());
                }

                reader.Close();

                string updateExecutionTimeQuery = $"UPDATE [dbo].[OrdersJobs] " +
                                                    $"SET [ExecutionTimeInSeconds] = {executionTimeInSeconds} " +
                                                    $"WHERE Id = {id} ";

                sql_cmnd = new SqlCommand(updateExecutionTimeQuery, connection);
                sql_cmnd.ExecuteNonQuery();

                connection.Close();
            }
         }

        private void CsvFileProcessingSetToCompleted(string fileName) 
        {
            int id = 0;
            string connectionString = "Password=eoffice;Persist Security Info=False;User ID=eoffice; Initial Catalog=OrdersDb; Data Source=srb-content-tst.src.si";

            SqlConnection connection = null;

            using (connection = new SqlConnection(connectionString))
            {
                connection.Open();

                string selectOrderByFilename = $"SELECT * " +
                                     $"FROM [OrdersDb].[dbo].[Orders] " +
                                     $"WHERE [Name] = '{fileName}'";

                SqlCommand sql_cmnd = new SqlCommand(selectOrderByFilename, connection);
                SqlDataReader reader = sql_cmnd.ExecuteReader();

                if (reader.Read())
                {
                    id = Convert.ToInt32(reader["id"].ToString());
                }

                reader.Close();

                string updateOrderJobByOrderId = $"UPDATE [dbo].[OrdersJobs] " +
                                                    $"SET [IsCompleted] = 1 " +
                                                    $"WHERE [OrderId] = {id}";

                sql_cmnd = new SqlCommand(updateOrderJobByOrderId, connection);
                sql_cmnd.ExecuteNonQuery();

                connection.Close();
            }
        }
    }
}

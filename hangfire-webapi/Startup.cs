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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace hangfire_webapi
{
    public class Startup
    {
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
                //                                  $"INNER JOIN OrderJobs ON Orders.Id = OrderJobs.OrderId " +
                //                                  $"WHERE IsConfirmed = 1 AND IsCompleted = 0 AND CreatedAt = '{DateTime.Now.AddDays(-1).Date}';";

                string selectFilesForProcessing = $"SELECT Name " +
                                                  $"FROM Orders " +
                                                  $"INNER JOIN OrderJobs ON Orders.Id = OrderJobs.OrderId ";

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

        public void WriteToTxtFile(string fileName)
        {

            string connectionString = "Password=eoffice;Persist Security Info=False;User ID=eoffice; Initial Catalog=OrdersDb; Data Source=srb-content-tst.src.si";

            SqlConnection connection = null;

            using (connection = new SqlConnection(connectionString))
            {
                connection.Open();


                string countFilesForProcessing = $"SELECT COUNT(*) " +
                                              $"FROM Orders " +
                                              $"INNER JOIN OrderJobs ON Orders.Id = OrderJobs.OrderId " +
                                              $"WHERE IsConfirmed = 1 AND IsCompleted = 0 AND Name = '{fileName}';";


                SqlCommand sql_cmnd = new SqlCommand(countFilesForProcessing, connection);
                int count = (int)sql_cmnd.ExecuteScalar();

                connection.Close();

                if (count == 0) 
                {
                    return;
                }
            }
            string message = $"Start procesing {fileName}{System.Environment.NewLine}";
            System.IO.File.AppendAllText(@"C:\txt\log.txt", message);

            string csvFileDirectory = @"C:\csv\";
            string csvFilePath = $"{csvFileDirectory}{fileName}.csv";

            Thread.Sleep(10 * 1000);

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

                string updateOrderJobByOrderId = $"UPDATE [dbo].[OrderJobs] " +
                                                    $"SET [IsCompleted] = 1 " +
                                                    $"WHERE [OrderId] = {id}";

                sql_cmnd = new SqlCommand(updateOrderJobByOrderId, connection);
                sql_cmnd.ExecuteNonQuery();

                connection.Close();
            }
        }
    }
}

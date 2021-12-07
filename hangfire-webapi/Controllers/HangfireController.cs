using Hangfire;
using hangfire_webapi.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Data.SqlClient;

namespace hangfire_webapi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class HangfireController : ControllerBase
    {
      
        private readonly ILogger<HangfireController> _logger;

        public HangfireController(ILogger<HangfireController> logger)
        {
            _logger = logger;
        }

      
        [HttpPost]
        [Route("[action]")]
        public IActionResult Welcome() 
        {
            var jobId = BackgroundJob.Enqueue(() => SendWelcomeEmail("Welcome to our app"));
            return Ok($"Job ID: {jobId} Welcome email sent to the user!");
        }

        [HttpPost]
        [Route("[action]")]
        public IActionResult Discount()
        {
            int timeInSeconds = 30;
            var jobId = BackgroundJob.Schedule(() => SendDiscountEmail("Welcome to our app"), TimeSpan.FromSeconds(30));
            return Ok($"Job ID: {jobId} Discount email will be sent in {timeInSeconds} seconds!");
        }

        [HttpPost]
        [Route("[action]")]
        public IActionResult NighlyJobFileGenerator()
        {
            RecurringJob.AddOrUpdate(() => DatabaseUpdated(), "*/3 * * * *");
            return Ok($"Database check job initiated at {DateTime.Now}!");
        }

        [HttpPost]
        [Route("[action]")]
        public IActionResult ConvertToTxtRequest(string csvFileName)
        { 
            string csvFileDirectory = @"C:\csv\";            

            string csvFilePath = $"{csvFileDirectory}{csvFileName}.csv";
            Order order = new Order(csvFilePath, csvFileName);

            SaveInDatabase(order);


            return Ok($"File {csvFileName}.csv is processed.");
        }

        [HttpPost]
        [Route("[action]")]
        public IActionResult ConvertToTxtRequestConfirmation(string csvFileName)
        {
            string csvFileDirectory = @"C:\csv\";

            string csvFilePath = $"{csvFileDirectory}{csvFileName}.csv";
            Order order = new Order(csvFilePath, csvFileName);

            ConfirmProcessingInDatabase(order);


            return Ok($"File {csvFileName}.csv is confirmed.");
        }

        public void ConfirmProcessingInDatabase(Order order)
        {
            int id = 0;
            string connectionString = "Password=eoffice;Persist Security Info=False;User ID=eoffice; Initial Catalog=OrdersDb; Data Source=srb-content-tst.src.si";

            SqlConnection connection = null;

            using (connection = new SqlConnection(connectionString))
            {
                connection.Open();

                string selectOrderByFilename = $"SELECT *" +
                                     $"FROM [OrdersDb].[dbo].[Orders]" +
                                     $"WHERE [Name] = '{order.FileName}'";

                SqlCommand sql_cmnd = new SqlCommand(selectOrderByFilename, connection);
                SqlDataReader reader = sql_cmnd.ExecuteReader();

                if (reader.Read()) 
                {
                    id = Convert.ToInt32(reader["id"].ToString());
                }

                reader.Close();

                string updateOrderJobByOrderId = $"UPDATE [dbo].[OrderJobs]" +
                                                    $"SET [IsConfirmed] = 1" +
                                                    $"WHERE [OrderId] = {id}";

                sql_cmnd = new SqlCommand(updateOrderJobByOrderId, connection);
                sql_cmnd.ExecuteNonQuery();

                connection.Close();
            }
        }

        public void SaveInDatabase(Order order) 
        {
            string connectionString = "Password=eoffice;Persist Security Info=False;User ID=eoffice; Initial Catalog=OrdersDb; Data Source=srb-content-tst.src.si";

            SqlConnection connection = null;

            using (connection = new SqlConnection(connectionString))
            {
                connection.Open();

                string insertOrder = $"INSERT INTO [dbo].[Orders]" +
                                     $"([Id]" +
                                     $",[Filepath]" +
                                     $",[Filename])" +
                                     $"VALUES(" +
                                     $"{order.Id}," +
                                     $"'{order.FilePath}'," +
                                     $"'{order.FileName}')";

                SqlCommand sql_cmnd = new SqlCommand(insertOrder, connection);
                sql_cmnd.ExecuteNonQuery();

                string insertOrderJob = $"INSERT INTO [dbo].[OrderJobs]" +
                                        $"([Id]" +
                                        $",[OrderId]" +
                                        $",[IsConfirmed]" +
                                        $",[IsCompleted]" +
                                        $",[CreatedAt])" +
                                        $"VALUES" +
                                        $"({order.Id}," +
                                        $"{order.Id}," +
                                        $"0," +
                                        $"0," +
                                        $"'{DateTime.Now}')";

                sql_cmnd = new SqlCommand(insertOrderJob, connection);
                sql_cmnd.ExecuteNonQuery();

                connection.Close();
            }
        }

      

     

        public void DatabaseUpdated() 
        {
            Console.WriteLine("Database updated.");
        }

        public void SendWelcomeEmail(string text) 
        {
            Console.WriteLine(text);
        }

        public void SendDiscountEmail(string text)
        {
            Console.WriteLine(text);
        }
    }
}

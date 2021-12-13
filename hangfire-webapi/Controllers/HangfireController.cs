using Hangfire;
using hangfire_webapi.Models;
using hangfire_webapi.Sql;
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

                string selectOrderByFilename = Queries.selectOrderByFilename;

                SqlCommand selectOrderByFilenameCommand = new SqlCommand(selectOrderByFilename, connection);
                selectOrderByFilenameCommand.Parameters.Add(new SqlParameter("Name", order.FileName));


                SqlDataReader reader = selectOrderByFilenameCommand.ExecuteReader();

                if (reader.Read()) 
                {
                    id = Convert.ToInt32(reader["id"].ToString());
                }

                reader.Close();

                string updateOrderJobByOrderId = Queries.updateOrderJobByOrderId;

                SqlCommand updateOrderJobByOrderIdCommand = new SqlCommand(updateOrderJobByOrderId, connection);
                updateOrderJobByOrderIdCommand.Parameters.Add(new SqlParameter("OrderId", id));

                updateOrderJobByOrderIdCommand.ExecuteNonQuery();

                connection.Close();
            }
        }

        public void SaveInDatabase(Order order) 
        {
            FileInfo fileInfo = new FileInfo(order.FilePath);

            string connectionString = "Password=eoffice;Persist Security Info=False;User ID=eoffice; Initial Catalog=OrdersDb; Data Source=srb-content-tst.src.si";

            SqlConnection connection = null;

            using (connection = new SqlConnection(connectionString))
            {
                connection.Open();

                SqlTransaction transaction;



                // Start a local transaction.
                transaction = connection.BeginTransaction("Ordertransaction");
                try
                {
         
                    string insertOrder = Queries.InsertOrder;

                    SqlCommand insertOrderCommand = new SqlCommand(insertOrder, connection);
                    insertOrderCommand.Parameters.Add(new SqlParameter("Id", order.Id));
                    insertOrderCommand.Parameters.Add(new SqlParameter("Path", order.FilePath));
                    insertOrderCommand.Parameters.Add(new SqlParameter("Name", order.FileName));


                    insertOrderCommand.Transaction = transaction;
                    insertOrderCommand.ExecuteNonQuery();

                    string insertOrderJob = Queries.InsertOrderJob;

                    SqlCommand insertOrderJobCommand = new SqlCommand(insertOrderJob, connection);
                    insertOrderJobCommand.Parameters.Add(new SqlParameter("Id", order.Id));
                    insertOrderJobCommand.Parameters.Add(new SqlParameter("OrderId", order.Id));
                    insertOrderJobCommand.Parameters.Add(new SqlParameter("IsConfirmed", "0"));
                    insertOrderJobCommand.Parameters.Add(new SqlParameter("IsCompleted", "0"));
                    insertOrderJobCommand.Parameters.Add(new SqlParameter("CreatedAt", DateTime.Now.Date));
                    insertOrderJobCommand.Parameters.Add(new SqlParameter("AttachmentSizeInBytes", fileInfo.Length));
                    insertOrderJobCommand.Parameters.Add(new SqlParameter("ExecutionTimeInSeconds", "0"));


                    insertOrderJobCommand.Transaction = transaction;
                    insertOrderJobCommand.ExecuteNonQuery();

                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    throw ex;
                }

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

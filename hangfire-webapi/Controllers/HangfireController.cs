using Hangfire;
using hangfire_webapi.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

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
        public IActionResult ConvertToExcel(string csvFileName)
        { 
            string csvFileDirectory = @"C:\csv\";            

            string csvFilePath = $"{csvFileDirectory}{csvFileName}";
            Order order = MakeOrder(csvFilePath);

            WriteToTxtFile(order, csvFileName);

            return Ok($"File {csvFileName} is converted to txt file");
        }

        public Order MakeOrder(string csvFilePath) 
        {
            Order order = new Order();
            List<OrderItem> orderItems = new List<OrderItem>();

            List<string> csvFileContent = System.IO.File.ReadAllLines(csvFilePath).ToList();
            foreach (string csvFileContentRow in csvFileContent)
            {
                List<string> orderItemParts = csvFileContentRow.Split(',').ToList();
                OrderItem orderItem = new OrderItem
                {
                    Country = orderItemParts[1],
                    UnitByPiece = Convert.ToDouble(orderItemParts[1]),
                    Pieces = Convert.ToInt32(orderItemParts[2])
                };

                orderItems.Add(orderItem);
            }

            order.OrderItems = orderItems;

            return order;
        }

        public void WriteToTxtFile(Order order, string fileName) 
        {
            string txtFileDirectory = @"C:\txt\";
            string txtFilePath = $"{txtFileDirectory}{fileName}";

            List<string> txtLines = new List<string>();
            foreach (OrderItem item in order.OrderItems)
            {
                string txtLine = $"[ {item.Country} | {item.UnitByPiece} | {item.Pieces}";
                txtLines.Add(txtLine);
            }

            System.IO.File.WriteAllLines(txtFilePath, txtLines);
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

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace hangfire_webapi.Models
{
    public class Order
    {
        public int Id { get; set; }
        public string FilePath { get; set; }
        public string FileName { get; set; }

        public Order(string filepath, string filename) 
        {
            FilePath = filepath;
            FileName = filename;

            string connectionString = "Password=eoffice;Persist Security Info=False;User ID=eoffice; Initial Catalog=OrdersDb; Data Source=srb-content-tst.src.si";

            SqlConnection connection = null;

            using (connection = new SqlConnection(connectionString))
            {
                connection.Open();

                string countOrdersQuery = $"SELECT count(*)" +
                                     $"FROM [OrdersDb].[dbo].[Orders]";

                SqlCommand sql_cmnd = new SqlCommand(countOrdersQuery, connection);
                int numberOfOrders = (int)sql_cmnd.ExecuteScalar();
                Id = numberOfOrders + 1;

                connection.Close();
            }
        }

    }
}

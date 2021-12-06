using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace hangfire_webapi.Models
{
    public class Order
    {
        public int Id { get; set; }
        public List<OrderItem> OrderItems { get; set; }

        public Order() 
        {
            OrderItems = new List<OrderItem>();
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace hangfire_webapi.Models
{
    public class OrderItem
    {
        public long Id { get; set; }
        public string Country { get; set; }
        public double UnitByPiece { get; set; }
        public int Pieces { get; set; }

        public OrderItem() 
        {

        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common.Entities;

namespace Common.Requests
{
    public record CustomerSession
    {
        public string Type { get; init; }
        
        public CartItem CartItem { get; init; } // This will be null if Type is not "ADD_TO_CART"
    
        public CustomerCheckout CustomerCheckout { get; init; }      
    }
}


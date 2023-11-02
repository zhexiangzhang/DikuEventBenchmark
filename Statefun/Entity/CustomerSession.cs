using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common.Entities;
using Common.Requests;

namespace Statefun.Entity
{
    public record CustomerSession
    {
        public string Type { get; init; }
        
        public CartItem CartItem { get; init; } // This will be null if Type is not "ADD_TO_CART"
    
        public CustomerCheckout CustomerCheckout { get; init; }      
    }
}
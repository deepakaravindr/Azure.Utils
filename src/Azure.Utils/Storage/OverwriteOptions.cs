using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Azure.Utils.Storage
{
    public enum OverwriteOptions
    {
        Never = 0,  
        OnlyIfNewer = 1,
        Always = 2,
    }
}

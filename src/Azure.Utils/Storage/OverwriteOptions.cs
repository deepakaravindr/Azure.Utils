using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Azure.Utils.Storage
{
    public enum OverwriteOptions
    {
        /// <summary>
        /// Does not overwrite when the file exists in destination
        /// </summary>
        Never = 0,
        /// <summary>
        /// Overwrites if the destination metadata does not have source blob information or if the source blob information is old
        /// NOTE:- Does not use timestamp
        /// </summary>
        OnlyIfNewer = 1,
        /// <summary>
        /// Always overwrite when the file exists in destination
        /// </summary>
        Always = 2,
    }
}

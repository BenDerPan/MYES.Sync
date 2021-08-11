using System;

namespace MYES
{
    class Program
    {
        static void Main(string[] args)
        {
            var loader = new SyncLoader();
            loader.Start();
        }
    }
}

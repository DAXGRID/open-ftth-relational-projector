using Microsoft.Extensions.Hosting;
using System;

namespace OpenFTTH.RelationalProjector
{
    class Program
    {
        static void Main(string[] args)
        {
            Startup.CreateHostBuilder(args).Build().Run();
        }
    }
}

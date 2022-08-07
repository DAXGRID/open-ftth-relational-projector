using Microsoft.Extensions.Hosting;

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

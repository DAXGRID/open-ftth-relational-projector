using Microsoft.Extensions.Hosting;
using System.Threading.Tasks;

namespace OpenFTTH.RelationalProjector
{
    class Program
    {
        static async Task Main(string[] args)
        {
            await Startup.CreateHostBuilder(args).Build().RunAsync();
        }
    }
}

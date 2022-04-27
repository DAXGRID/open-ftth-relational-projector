using OpenFTTH.UtilityGraphService.API.Model.UtilityNetwork;
using System;
using System.Linq;

namespace OpenFTTH.RelationalProjector.State
{
    /// <summary>
    /// Service termination state needed by projection logic
    /// </summary>
    public class ServiceTerminationState
    {
        public Guid Id { get; set; }
        public Guid RouteNodeId { get; set; }
        public string Name { get; set; }

        public static ServiceTerminationState Create(TerminalEquipment terminalEquipment, Guid routeNodeId)
        {
            return new ServiceTerminationState()
            {
                Id = terminalEquipment.Id,
                RouteNodeId = routeNodeId,
                Name = terminalEquipment.Name
            };
        }
    }
}

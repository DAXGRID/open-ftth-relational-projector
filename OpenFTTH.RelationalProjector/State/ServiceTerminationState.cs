using OpenFTTH.UtilityGraphService.API.Model.UtilityNetwork;
using System;
using System.Linq;

namespace OpenFTTH.RelationalProjector.State
{
    /// <summary>
    /// Service termination state needed by projection logic
    /// </summary>
    public class ServiceTerminationState : ObjectState
    {
        public Guid Id { get; set; }
        public Guid RouteNodeId { get; set; }
        public string Name { get; set; }
        public Guid? AccessAddressId { get; internal set; }
        public Guid? UnitAddressId { get; internal set; }

        public ServiceTerminationState(LatestChangeType latestChangeType, TerminalEquipment terminalEquipment, Guid routeNodeId) : base(latestChangeType)
        {
            Id = terminalEquipment.Id;
            RouteNodeId = routeNodeId;
            Name = terminalEquipment.Name;
            AccessAddressId = terminalEquipment.AddressInfo?.AccessAddressId;
            UnitAddressId = terminalEquipment.AddressInfo?.UnitAddressId;
        }
    }
}

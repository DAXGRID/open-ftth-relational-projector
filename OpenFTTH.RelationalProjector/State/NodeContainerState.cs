using OpenFTTH.UtilityGraphService.API.Model.UtilityNetwork;
using System;

namespace OpenFTTH.RelationalProjector.State
{
    /// <summary>
    /// Node container state needed by projection logic
    /// </summary>
    public class NodeContainerState : ObjectState
    {
        public Guid Id { get; set; }
        public Guid RouteNodeId { get; set; }
        public Guid SpecificationId { get; set; }
        public string SpecificationName { get; set; }

        public NodeContainerState(LatestChangeType latestChangeType, NodeContainer nodeContainer, string specName) : base(latestChangeType)
        {
            Id = nodeContainer.Id;
            RouteNodeId = nodeContainer.RouteNodeId;
            SpecificationId = nodeContainer.SpecificationId;
            SpecificationName = specName;
        }
    }
}

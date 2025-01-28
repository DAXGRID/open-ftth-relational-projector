using System;

namespace OpenFTTH.RelationalProjector.State
{
    /// <summary>
    /// Cable to route network information needed by projection logic
    /// </summary>
    public class CableToRouteElementState : ObjectState
    {
        public Guid CableId { get; set; }

        public Guid[] RouteNetworkElementIds { get; set; }
        public CableToRouteElementState(LatestChangeType latestChangeType, Guid cableId, Guid[] routeNetworkElementIds) : base(latestChangeType)
        {
           CableId = cableId;
           RouteNetworkElementIds = routeNetworkElementIds;
        }
    }
}

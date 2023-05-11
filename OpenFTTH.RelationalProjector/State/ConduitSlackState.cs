using System;

namespace OpenFTTH.RelationalProjector.State
{
    /// <summary>
    /// Conduit slack for fucture service connection
    /// </summary>
    public class ConduitSlackState : ObjectState
    {
        public Guid Id { get; set; }
        public Guid RouteNodeId { get; set; }
        public int NumberOfConduitEnds { get; set; }

        public ConduitSlackState(LatestChangeType latestChangeType) : base(latestChangeType)
        {
        }
    }
}

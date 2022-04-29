using OpenFTTH.UtilityGraphService.API.Model.UtilityNetwork;
using System;
using System.Linq;

namespace OpenFTTH.RelationalProjector.State
{
    /// <summary>
    /// Span equipment state needed by projection logic
    /// </summary>
    public class SpanEquipmentState
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
        public Guid WalkOfInterestId { get; set; }
        public Guid SpecificationId { get; set; }
        public Guid FromNodeId { get; set; }
        public Guid ToNodeId { get; set; }
        public bool IsCable { get; set; }
        public bool IsCustomerConduit { get; set; }
        public Guid RootSegmentId { get; set; }
        public bool RootSegmentHasFromConnection { get; set; }
        public bool RootSegmentHasToConnection { get; set; }
        public Guid RootSegmentFromTerminalId { get; set; }
        public Guid RootSegmentToTerminalId { get; set; }
        public bool HasChildSpanEquipments { get; set; }
        public Guid ChildSpanEquipmentId { get; internal set; }

        public static SpanEquipmentState Create(SpanEquipment spanEquipment, SpanEquipmentSpecification spanEquipmentSpecification)
        {
            var state = new SpanEquipmentState()
            {
                Id = spanEquipment.Id,
                Name = spanEquipment.Name,
                WalkOfInterestId = spanEquipment.WalkOfInterestId,
                SpecificationId = spanEquipment.SpecificationId,
                FromNodeId = spanEquipment.NodesOfInterestIds.First(),
                ToNodeId = spanEquipment.NodesOfInterestIds.Last(),
                IsCable = spanEquipment.IsCable,
                RootSegmentHasFromConnection = CheckIfAnyFromConnections(spanEquipment),
                RootSegmentHasToConnection = CheckIfAnyToConnections(spanEquipment),
                RootSegmentId = spanEquipment.SpanStructures.First().SpanSegments.First().Id,
                IsCustomerConduit = spanEquipmentSpecification.Name.ToLower().Contains("ø12") ? true : false
            };

            return state;
        }

        private static bool CheckIfAnyFromConnections(SpanEquipment spanEquipment)
        {
            foreach (var spanStructure in spanEquipment.SpanStructures)
            {
                foreach (var spanSegment in spanStructure.SpanSegments)
                {
                    if (spanSegment.FromNodeOfInterestIndex == 0 && spanSegment.FromTerminalId != Guid.Empty)
                        return true;
                }
            }

            return false;
        }

        private static bool CheckIfAnyToConnections(SpanEquipment spanEquipment)
        {
            foreach (var spanStructure in spanEquipment.SpanStructures)
            {
                foreach (var spanSegment in spanStructure.SpanSegments)
                {
                    if (spanSegment.ToNodeOfInterestIndex == (spanEquipment.NodesOfInterestIds.Length - 1) && spanSegment.ToTerminalId != Guid.Empty)
                        return true;
                }
            }

            return false;
        }
    }
}

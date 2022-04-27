using OpenFTTH.RouteNetwork.API.Model;
using OpenFTTH.UtilityGraphService.API.Model.UtilityNetwork;
using OpenFTTH.UtilityGraphService.Business.NodeContainers.Events;
using OpenFTTH.UtilityGraphService.Business.SpanEquipments.Events;
using OpenFTTH.UtilityGraphService.Business.TerminalEquipments.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OpenFTTH.RelationalProjector.State
{
    public class ProjektorState
    {
        #region Node Container Information
        private Dictionary<Guid, Guid> _nodeContainerToRouteNodeRelation = new();
        private Dictionary<Guid, Guid> _routeNodeToNodeContainerRelation = new();

        public void ProcessNodeContainerAdded(NodeContainerPlacedInRouteNetwork @event)
        {
            _nodeContainerToRouteNodeRelation[@event.Container.Id] = @event.Container.RouteNodeId;
            _routeNodeToNodeContainerRelation[@event.Container.RouteNodeId] = @event.Container.Id;
        }

        public void ProcessNodeContainerRemoved(Guid nodeContainerId)
        {
            if (_nodeContainerToRouteNodeRelation.ContainsKey(nodeContainerId))
            {
                var routeNodeId = _nodeContainerToRouteNodeRelation[nodeContainerId];

                _nodeContainerToRouteNodeRelation.Remove(nodeContainerId);

                if (_routeNodeToNodeContainerRelation.ContainsKey(routeNodeId))
                {
                    _routeNodeToNodeContainerRelation.Remove(routeNodeId);
                }
            }
        }

        #endregion

        #region Interest information

        private Dictionary<Guid, Guid[]> _walkOfInterestToRouteElementRelations = new();

        public Dictionary<Guid, Guid[]> WalkOfInterestToRouteElementRelations => _walkOfInterestToRouteElementRelations;

        public void ProcessWalkOfInterestAdded(RouteNetworkInterest interest)
        {
            _walkOfInterestToRouteElementRelations[interest.Id] = RemoveDublicatedIds(interest.RouteNetworkElementRefs).ToArray();
        }

        public void ProcessWalkOfInterestUpdated(Guid interestId, RouteNetworkElementIdList routeNetworkElementIds)
        {
            _walkOfInterestToRouteElementRelations[interestId] = RemoveDublicatedIds(routeNetworkElementIds).ToArray();
        }

        public void ProcessInterestRemoved(Guid interestId)
        {
            if (_walkOfInterestToRouteElementRelations.ContainsKey(interestId))
                _walkOfInterestToRouteElementRelations.Remove(interestId);
        }

        #endregion

        #region Span Equipment Information

        private Dictionary<Guid, SpanEquipmentSpecification> _spanEquipmentSpecificationById = new();
        private Dictionary<Guid, SpanStructureSpecification> _spanStructureSpecificationById = new();
        private Dictionary<Guid, SpanEquipmentState> _spanEquipmentStateById = new();
        private Dictionary<Guid, SpanEquipmentState> _spanEquipmentStateByRootSegmentId = new();
        private Dictionary<Guid, ConduitSlackState> _conduitSlackStateByRouteNodeId = new();

        public IEnumerable<SpanEquipmentState> SpanEquipmentStates => _spanEquipmentStateById.Values;
        public IEnumerable<ConduitSlackState> ConduitSlackStates => _conduitSlackStateByRouteNodeId.Values;
        public Dictionary<Guid, SpanEquipmentSpecification> SpanEquipmentSpecificationsById => _spanEquipmentSpecificationById;
        public Dictionary<Guid, SpanStructureSpecification> SpanStructureSpecificationsById => _spanStructureSpecificationById;


        public void ProcessSpanEquipmentAdded(SpanEquipment equipment)
        {
            var spanEquipmentState = SpanEquipmentState.Create(equipment, _spanEquipmentSpecificationById[equipment.SpecificationId]);

            _spanEquipmentStateById[equipment.Id] = spanEquipmentState;
            _spanEquipmentStateByRootSegmentId[spanEquipmentState.RootSegmentId] = spanEquipmentState;

            if (IsSpanEquipmentFromNodeSlack(spanEquipmentState))
            { 
                IncrementConduitSlackEndCount(spanEquipmentState.FromNodeId);
            }

            if (IsSpanEquipmentToNodeSlack(spanEquipmentState))
            {
                IncrementConduitSlackEndCount(spanEquipmentState.ToNodeId);
            }
        }
        public void ProcessSpanEquipmentMoved(SpanEquipmentMoved @event)
        {
            Guid newFromNodeId = @event.NodesOfInterestIds.First();
            Guid newToNodeId = @event.NodesOfInterestIds.Last();

            if (_spanEquipmentStateById.TryGetValue(@event.SpanEquipmentId, out var existingSpanEquipment))
            {
                // If from node moved
                if (existingSpanEquipment.FromNodeId != newFromNodeId)
                {
                    if (IsSpanEquipmentFromNodeSlack(existingSpanEquipment))
                    {
                        DecrementConduitSlackEndCount(existingSpanEquipment.FromNodeId);
                        IncrementConduitSlackEndCount(newFromNodeId);
                    }

                    existingSpanEquipment.FromNodeId = @event.NodesOfInterestIds.First();
                }

                // If to node moved
                if (existingSpanEquipment.ToNodeId != newToNodeId)
                {
                    if (IsSpanEquipmentToNodeSlack(existingSpanEquipment))
                    {
                        DecrementConduitSlackEndCount(existingSpanEquipment.ToNodeId);
                        IncrementConduitSlackEndCount(newToNodeId);
                    }

                    existingSpanEquipment.ToNodeId = @event.NodesOfInterestIds.Last();
                }
            }
        }

        public void ProcessSpanEquipmentRemoved(Guid spanEquipmentId)
        {
            var existingSpanEquipmentState = _spanEquipmentStateById[spanEquipmentId];

            if (IsSpanEquipmentFromNodeSlack(existingSpanEquipmentState))
            {
                DecrementConduitSlackEndCount(existingSpanEquipmentState.FromNodeId);
            }

            if (IsSpanEquipmentToNodeSlack(existingSpanEquipmentState))
            {
                DecrementConduitSlackEndCount(existingSpanEquipmentState.ToNodeId);
            }

            _spanEquipmentStateByRootSegmentId.Remove(existingSpanEquipmentState.RootSegmentId);
            _spanEquipmentStateById.Remove(spanEquipmentId);
        }

        public void ProcessSpanEquipmentChanged(SpanEquipmentSpecificationChanged @event)
        {
            _spanEquipmentStateById[@event.SpanEquipmentId].SpecificationId = @event.NewSpecificationId;
        }

        public void ProcessSpanEquipmentSpecificationAdded(SpanEquipmentSpecificationAdded @event)
        {
            _spanEquipmentSpecificationById[@event.Specification.Id] = @event.Specification;
        }

        public void ProcessSpanStructureSpecificationAdded(SpanStructureSpecificationAdded @event)
        {
            _spanStructureSpecificationById[@event.Specification.Id] = @event.Specification;
        }


        public bool TryGetSpanEquipmentState(Guid spanEquipmentId, out SpanEquipmentState spanEquipmentState)
        {
            if (_spanEquipmentStateById.TryGetValue(spanEquipmentId, out var spanEquipmentStateFound))
            {
                spanEquipmentState = spanEquipmentStateFound;
                return true;
            }
            else
            {
                spanEquipmentState = null;
                return false;
            }
        }

        public void ProcessSpanEquipmentConnects(SpanSegmentsConnectedToSimpleTerminals @event)
        {
            if (_spanEquipmentStateById.TryGetValue(@event.SpanEquipmentId, out var existingSpanEquipmentState))
            {
                foreach (var connect in @event.Connects)
                {
                    if (connect.SegmentId == existingSpanEquipmentState.RootSegmentId)
                    {
                        if (connect.ConnectionDirection == SpanSegmentToTerminalConnectionDirection.FromSpanSegmentToTerminal)
                        {
                            if (IsSpanEquipmentToNodeSlack(existingSpanEquipmentState))
                            {
                                DecrementConduitSlackEndCount(existingSpanEquipmentState.ToNodeId);
                            }

                            existingSpanEquipmentState.RootSegmentHasToConnection = true;
                            existingSpanEquipmentState.RootSegmentToTerminalId = connect.TerminalId;
                        }
                        else if (connect.ConnectionDirection == SpanSegmentToTerminalConnectionDirection.FromTerminalToSpanSegment)
                        {
                            if (IsSpanEquipmentFromNodeSlack(existingSpanEquipmentState))
                            {
                                DecrementConduitSlackEndCount(existingSpanEquipmentState.FromNodeId);
                            }

                            existingSpanEquipmentState.RootSegmentHasFromConnection = true;
                            existingSpanEquipmentState.RootSegmentFromTerminalId = connect.TerminalId;
                        }
                    }
                }
            }
        }

        public void ProcessSpanEquipmentDisconnects(SpanSegmentsDisconnectedFromTerminals @event)
        {
            if (_spanEquipmentStateById.TryGetValue(@event.SpanEquipmentId, out var existingSpanEquipmentState))
            {
                foreach (var disconnect in @event.Disconnects)
                {
                    if (disconnect.SegmentId == existingSpanEquipmentState.RootSegmentId)
                    {
                        if (existingSpanEquipmentState.RootSegmentFromTerminalId == disconnect.TerminalId)
                        {
                            existingSpanEquipmentState.RootSegmentFromTerminalId = Guid.Empty;
                            existingSpanEquipmentState.RootSegmentHasFromConnection = false;

                            if (IsSpanEquipmentToNodeSlack(existingSpanEquipmentState))
                            {
                                IncrementConduitSlackEndCount(existingSpanEquipmentState.ToNodeId);
                            }
                        }
                        else if (existingSpanEquipmentState.RootSegmentToTerminalId == disconnect.TerminalId)
                        {
                            existingSpanEquipmentState.RootSegmentToTerminalId = Guid.Empty;
                            existingSpanEquipmentState.RootSegmentHasToConnection = false;

                            if (IsSpanEquipmentFromNodeSlack(existingSpanEquipmentState))
                            {
                                IncrementConduitSlackEndCount(existingSpanEquipmentState.FromNodeId);
                            }
                        }
                    }
                }
            }
        }

        public void ProcessSpanEquipmentAffixedToParent(SpanEquipmentAffixedToParent @event)
        {
            foreach (var utilityNetworkHop in @event.NewUtilityHopList)
            {
                foreach (var parentAffix in utilityNetworkHop.ParentAffixes)
                {
                    if (_spanEquipmentStateByRootSegmentId.TryGetValue(parentAffix.SpanSegmentId, out var existingSpanEquipmentState))
                    {
                        if (IsSpanEquipmentToNodeSlack(existingSpanEquipmentState))
                        {
                            DecrementConduitSlackEndCount(existingSpanEquipmentState.ToNodeId);
                        }

                        if (IsSpanEquipmentFromNodeSlack(existingSpanEquipmentState))
                        {
                            DecrementConduitSlackEndCount(existingSpanEquipmentState.FromNodeId);
                        }

                        existingSpanEquipmentState.ChildSpanEquipmentId = @event.SpanEquipmentId;
                        existingSpanEquipmentState.HasChildSpanEquipments = true;
                    }
                }
            }
        }

        public void ProcessSpanEquipmentDetachedFromParent(SpanEquipmentDetachedFromParent @event)
        {
            foreach (var utilityNetworkHop in @event.NewUtilityHopList)
            {
                foreach (var parentAffix in utilityNetworkHop.ParentAffixes)
                {
                    if (_spanEquipmentStateByRootSegmentId.TryGetValue(parentAffix.SpanSegmentId, out var existingSpanEquipmentState))
                    {
                        if (@event.SpanEquipmentId == existingSpanEquipmentState.ChildSpanEquipmentId)
                        {
                            existingSpanEquipmentState.ChildSpanEquipmentId = Guid.Empty;
                            existingSpanEquipmentState.HasChildSpanEquipments = false;

                            if (IsSpanEquipmentToNodeSlack(existingSpanEquipmentState))
                            {
                                IncrementConduitSlackEndCount(existingSpanEquipmentState.ToNodeId);
                            }

                            if (IsSpanEquipmentFromNodeSlack(existingSpanEquipmentState))
                            {
                                IncrementConduitSlackEndCount(existingSpanEquipmentState.FromNodeId);
                            }
                        }
                    }
                }
            }
        }
      

        public SpanEquipmentSpecification GetSpanEquipmentSpecification(Guid specificationId)
        {
            return _spanEquipmentSpecificationById[specificationId];
        }

        public SpanStructureSpecification GetSpanStructureSpecification(Guid spanStructureSpecificationId)
        {
            return _spanStructureSpecificationById[spanStructureSpecificationId];
        }

        private bool IsSpanEquipmentFromNodeSlack(SpanEquipmentState spanEquipmentState)
        {
            if (spanEquipmentState.IsCustomerConduit && !_routeNodeToNodeContainerRelation.ContainsKey(spanEquipmentState.FromNodeId) && !spanEquipmentState.RootSegmentHasFromConnection && !spanEquipmentState.HasChildSpanEquipments)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        private bool IsSpanEquipmentToNodeSlack(SpanEquipmentState spanEquipmentState)
        {
            if (spanEquipmentState.IsCustomerConduit && !_routeNodeToNodeContainerRelation.ContainsKey(spanEquipmentState.ToNodeId) && !spanEquipmentState.RootSegmentHasToConnection && !spanEquipmentState.HasChildSpanEquipments)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        private void IncrementConduitSlackEndCount(Guid nodeId)
        {
            if (_conduitSlackStateByRouteNodeId.ContainsKey(nodeId))
            {
                _conduitSlackStateByRouteNodeId[nodeId].NumberOfConduitEnds++;
            }
            else
            {
                _conduitSlackStateByRouteNodeId.Add(nodeId, new ConduitSlackState() { Id = Guid.NewGuid(), RouteNodeId = nodeId, NumberOfConduitEnds = 1 });
            }
        }

        private void DecrementConduitSlackEndCount(Guid nodeId)
        {
            if (_conduitSlackStateByRouteNodeId.ContainsKey(nodeId))
            {
                _conduitSlackStateByRouteNodeId[nodeId].NumberOfConduitEnds--;

                if (_conduitSlackStateByRouteNodeId[nodeId].NumberOfConduitEnds == 0)
                {
                    _conduitSlackStateByRouteNodeId.Remove(nodeId);
                }
            }
        }


        #endregion

        #region Terminal Equipment Information

        private Dictionary<Guid, TerminalEquipmentSpecification> _terminalEquipmentSpecificationById = new();

        private Dictionary<Guid, ServiceTerminationState> _serviceTerminationStateByEquipmentId = new();
        public IEnumerable<ServiceTerminationState> ServiceTerminationStates => _serviceTerminationStateByEquipmentId.Values;

        public void ProcessTerminalEquipmentSpecificationAdded(TerminalEquipmentSpecificationAdded @event)
        {
            _terminalEquipmentSpecificationById[@event.Specification.Id] = @event.Specification;
        }

        public ServiceTerminationState? ProcessServiceTerminationstAdded(TerminalEquipmentPlacedInNodeContainer @event)
        {
            // If no terminal equipment specification found, give up
            if (!_terminalEquipmentSpecificationById.TryGetValue(@event.Equipment.SpecificationId, out var terminalEquipmentSpecification))
                return null;

            // If no relation from node container to route node found, give up
            if (!_nodeContainerToRouteNodeRelation.TryGetValue(@event.Equipment.NodeContainerId, out var routeNodeId))
                return null;

            // If not an customer/service termination, don't proceed
            if (!terminalEquipmentSpecification.IsCustomerTermination)
                return null;

            var serviceTerminationState = ServiceTerminationState.Create(@event.Equipment, routeNodeId);
            _serviceTerminationStateByEquipmentId[@event.Equipment.Id] = serviceTerminationState;
            
            return serviceTerminationState;
        }

        public void ProcessTerminalEquipmentRemoved(Guid terminalEquipmentId)
        {
            if (_serviceTerminationStateByEquipmentId.ContainsKey(terminalEquipmentId))
                _serviceTerminationStateByEquipmentId.Remove(terminalEquipmentId);
        }

        #endregion


        private IEnumerable<Guid> RemoveDublicatedIds(RouteNetworkElementIdList routeNetworkElementRefs)
        {
            RouteNetworkElementIdList result = new();

            HashSet<Guid> alreadyAdded = new();

            foreach (var id in routeNetworkElementRefs)
            {
                if (!alreadyAdded.Contains(id))
                {
                    alreadyAdded.Add(id);
                    result.Add(id);
                }
            }

            return result;
        }
    }
}

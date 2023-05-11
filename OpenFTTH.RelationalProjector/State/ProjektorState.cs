using OpenFTTH.RouteNetwork.API.Model;
using OpenFTTH.UtilityGraphService.API.Model.UtilityNetwork;
using OpenFTTH.UtilityGraphService.Business.NodeContainers.Events;
using OpenFTTH.UtilityGraphService.Business.SpanEquipments.Events;
using OpenFTTH.UtilityGraphService.Business.TerminalEquipments.Events;
using OpenFTTH.Work.Business.Events;
using System;
using System.Collections.Generic;
using System.Linq;

namespace OpenFTTH.RelationalProjector.State
{
    public class ProjektorState
    {
        #region Node Container Information
        private Dictionary<Guid, Guid> _nodeContainerToRouteNodeRelation = new();
        private Dictionary<Guid, Guid> _routeNodeToNodeContainerRelation = new();

        public void ProcessNodeContainerAdded(NodeContainerPlacedInRouteNetwork @event)
        {
            // Remove conduit slack if any, unless uknown conduit junction
            // FIX: Remove conduit slack stuff to customer specific relational projector service
            if (@event.Container.SpecificationId != Guid.Parse("c288e797-a65c-4cf6-b63d-5eda4b4a8a8c"))
            {
                _nodeContainerToRouteNodeRelation[@event.Container.Id] = @event.Container.RouteNodeId;
                _routeNodeToNodeContainerRelation[@event.Container.RouteNodeId] = @event.Container.Id;

                if (_conduitSlackStateByRouteNodeId.ContainsKey(@event.Container.RouteNodeId))
                    _conduitSlackStateByRouteNodeId.Remove(@event.Container.RouteNodeId);
            }
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


        public List<ObjectState> ProcessSpanEquipmentAdded(SpanEquipment equipment)
        {
            List<ObjectState> stateChanges = new List<ObjectState>();

            var spanEquipmentSpecification = _spanEquipmentSpecificationById[equipment.SpecificationId];

            var spanStructureSpecification = _spanStructureSpecificationById[spanEquipmentSpecification.RootTemplate.SpanStructureSpecificationId];

            var spanEquipmentState = SpanEquipmentState.Create(equipment, spanEquipmentSpecification, spanStructureSpecification);

            _spanEquipmentStateById[equipment.Id] = spanEquipmentState;
            _spanEquipmentStateByRootSegmentId[spanEquipmentState.RootSegmentId] = spanEquipmentState;

            if (IsSpanEquipmentFromNodeSlack(spanEquipmentState))
            {
                stateChanges.AddRange(IncrementConduitSlackEndCount(spanEquipmentState.FromNodeId));
            }

            if (IsSpanEquipmentToNodeSlack(spanEquipmentState))
            {
                stateChanges.AddRange(IncrementConduitSlackEndCount(spanEquipmentState.ToNodeId));
            }

            return stateChanges;
        }

        public List<ObjectState> ProcessSpanEquipmentMoved(SpanEquipmentMoved @event)
        {
            List<ObjectState> stateChanges = new List<ObjectState>();

            Guid newFromNodeId = @event.NodesOfInterestIds.First();
            Guid newToNodeId = @event.NodesOfInterestIds.Last();

            if (_spanEquipmentStateById.TryGetValue(@event.SpanEquipmentId, out var existingSpanEquipment))
            {
                // If from node moved
                if (existingSpanEquipment.FromNodeId != newFromNodeId)
                {
                    if (IsSpanEquipmentFromNodeSlack(existingSpanEquipment))
                    {
                        stateChanges.AddRange(DecrementConduitSlackEndCount(existingSpanEquipment.FromNodeId));
                        stateChanges.AddRange(IncrementConduitSlackEndCount(newFromNodeId));
                    }

                    existingSpanEquipment.FromNodeId = @event.NodesOfInterestIds.First();
                }

                // If to node moved
                if (existingSpanEquipment.ToNodeId != newToNodeId)
                {
                    if (IsSpanEquipmentToNodeSlack(existingSpanEquipment))
                    {
                        stateChanges.AddRange(DecrementConduitSlackEndCount(existingSpanEquipment.ToNodeId));
                        stateChanges.AddRange(IncrementConduitSlackEndCount(newToNodeId));
                    }

                    existingSpanEquipment.ToNodeId = @event.NodesOfInterestIds.Last();
                }
            }

            return stateChanges;
        }
        
        public List<ObjectState> ProcessSpanEquipmentRemoved(Guid spanEquipmentId)
        {
            List<ObjectState> stateChanges = new List<ObjectState>();

            var spanEquipmentState = _spanEquipmentStateById[spanEquipmentId];

            if (IsSpanEquipmentFromNodeSlack(spanEquipmentState))
            {
                stateChanges.AddRange(DecrementConduitSlackEndCount(spanEquipmentState.FromNodeId));
            }

            if (IsSpanEquipmentToNodeSlack(spanEquipmentState))
            {
                stateChanges.AddRange(DecrementConduitSlackEndCount(spanEquipmentState.ToNodeId));
            }

            spanEquipmentState.LatestChangeType = LatestChangeType.REMOVED;
            _spanEquipmentStateByRootSegmentId.Remove(spanEquipmentState.RootSegmentId);
            _spanEquipmentStateById.Remove(spanEquipmentId);

            stateChanges.Add(spanEquipmentState);

            return stateChanges;
        }

        public List<ObjectState> ProcessSpanEquipmentSpecificationChanged(SpanEquipmentSpecificationChanged @event)
        {
            var spanEquipmentSpecification = GetSpanEquipmentSpecification(@event.NewSpecificationId);

            var spanStructureSpecification = GetSpanStructureSpecification(spanEquipmentSpecification.RootTemplate.SpanStructureSpecificationId);

            var spanEquipmentState = _spanEquipmentStateById[@event.SpanEquipmentId];

            spanEquipmentState.LatestChangeType = LatestChangeType.UPDATED;

            spanEquipmentState.SpecificationId = @event.NewSpecificationId;

            spanEquipmentState.SpecificationName = spanEquipmentSpecification.Name;

            spanEquipmentState.OuterDiameter = spanStructureSpecification.OuterDiameter;

            return new List<ObjectState> { spanEquipmentState };
        }

        public List<ObjectState> ProcessSpanEquipmentAddressInfoChanged(SpanEquipmentAddressInfoChanged @event)
        {
            var spanEquipmentState = _spanEquipmentStateById[@event.SpanEquipmentId];

            spanEquipmentState.LatestChangeType = LatestChangeType.UPDATED;

            spanEquipmentState.AccessAddressId = @event.AddressInfo.AccessAddressId;

            spanEquipmentState.UnitAddressId = @event.AddressInfo.UnitAddressId;

            return new List<ObjectState> { spanEquipmentState };
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

        public List<ObjectState> ProcessSpanEquipmentConnects(SpanSegmentsConnectedToSimpleTerminals @event)
        {
            List<ObjectState> stateChanges = new List<ObjectState>();

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
                                stateChanges.AddRange(DecrementConduitSlackEndCount(existingSpanEquipmentState.ToNodeId));
                            }

                            existingSpanEquipmentState.RootSegmentHasToConnection = true;
                            existingSpanEquipmentState.RootSegmentToTerminalId = connect.TerminalId;

                        }
                        else if (connect.ConnectionDirection == SpanSegmentToTerminalConnectionDirection.FromTerminalToSpanSegment)
                        {
                            if (IsSpanEquipmentFromNodeSlack(existingSpanEquipmentState))
                            {
                                stateChanges.AddRange(DecrementConduitSlackEndCount(existingSpanEquipmentState.FromNodeId));
                            }

                            existingSpanEquipmentState.RootSegmentHasFromConnection = true;
                            existingSpanEquipmentState.RootSegmentFromTerminalId = connect.TerminalId;
                        }
                    }
                }
            }

            return stateChanges;
        }

        public List<ObjectState> ProcessSpanEquipmentDisconnects(SpanSegmentsDisconnectedFromTerminals @event)
        {
            List<ObjectState> stateChanges = new List<ObjectState>();

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
                                stateChanges.AddRange(IncrementConduitSlackEndCount(existingSpanEquipmentState.ToNodeId));
                            }
                        }
                        else if (existingSpanEquipmentState.RootSegmentToTerminalId == disconnect.TerminalId)
                        {
                            existingSpanEquipmentState.RootSegmentToTerminalId = Guid.Empty;
                            existingSpanEquipmentState.RootSegmentHasToConnection = false;

                            if (IsSpanEquipmentFromNodeSlack(existingSpanEquipmentState))
                            {
                                stateChanges.AddRange(IncrementConduitSlackEndCount(existingSpanEquipmentState.FromNodeId));
                            }
                        }
                    }
                }
            }

            return stateChanges;
        }

        public List<ObjectState> ProcessSpanEquipmentAffixedToParent(SpanEquipmentAffixedToParent @event)
        {
            List<ObjectState> stateChanges = new List<ObjectState>();

            foreach (var utilityNetworkHop in @event.NewUtilityHopList)
            {
                foreach (var parentAffix in utilityNetworkHop.ParentAffixes)
                {
                    if (_spanEquipmentStateByRootSegmentId.TryGetValue(parentAffix.SpanSegmentId, out var existingSpanEquipmentState))
                    {
                        if (IsSpanEquipmentToNodeSlack(existingSpanEquipmentState))
                        {
                            stateChanges.AddRange(DecrementConduitSlackEndCount(existingSpanEquipmentState.ToNodeId));
                        }

                        if (IsSpanEquipmentFromNodeSlack(existingSpanEquipmentState))
                        {
                            stateChanges.AddRange(DecrementConduitSlackEndCount(existingSpanEquipmentState.FromNodeId));
                        }

                        existingSpanEquipmentState.ChildSpanEquipmentId = @event.SpanEquipmentId;
                        existingSpanEquipmentState.HasChildSpanEquipments = true;
                    }
                }
            }

            return stateChanges;
        }

        public List<ObjectState> ProcessSpanEquipmentDetachedFromParent(SpanEquipmentDetachedFromParent @event)
        {
            List<ObjectState> stateChanges = new List<ObjectState>();

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
                                stateChanges.AddRange(IncrementConduitSlackEndCount(existingSpanEquipmentState.ToNodeId));
                            }

                            if (IsSpanEquipmentFromNodeSlack(existingSpanEquipmentState))
                            {
                                stateChanges.AddRange(IncrementConduitSlackEndCount(existingSpanEquipmentState.FromNodeId));
                            }
                        }
                    }
                }
            }

            return stateChanges;
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

        private List<ConduitSlackState> IncrementConduitSlackEndCount(Guid nodeId)
        {
            if (_conduitSlackStateByRouteNodeId.TryGetValue(nodeId, out var conduitSlackState))
            {
                conduitSlackState.NumberOfConduitEnds++;
                conduitSlackState.LatestChangeType = LatestChangeType.UPDATED;

                return new List<ConduitSlackState>() { conduitSlackState };
            }
            else
            {
                var newConduitSlackState = new ConduitSlackState(LatestChangeType.NEW) { Id = Guid.NewGuid(), RouteNodeId = nodeId, NumberOfConduitEnds = 1 };
                
                _conduitSlackStateByRouteNodeId.Add(nodeId, newConduitSlackState);

                return new List<ConduitSlackState>() { newConduitSlackState };
            }
        }

        private List<ConduitSlackState> DecrementConduitSlackEndCount(Guid nodeId)
        {
            if (_conduitSlackStateByRouteNodeId.TryGetValue(nodeId, out var conduitSlackState))
            {
                conduitSlackState.NumberOfConduitEnds--;
                conduitSlackState.LatestChangeType = LatestChangeType.UPDATED;

                if (conduitSlackState.NumberOfConduitEnds == 0)
                {
                    _conduitSlackStateByRouteNodeId.Remove(nodeId);
                }

                return new List<ConduitSlackState>() { conduitSlackState };
            }
            else
            {
                return new List<ConduitSlackState>();
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

        public ServiceTerminationState ProcessServiceTerminationAdded(TerminalEquipmentPlacedInNodeContainer @event)
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

            var serviceTerminationState = new ServiceTerminationState(LatestChangeType.NEW, @event.Equipment, routeNodeId);
            _serviceTerminationStateByEquipmentId[@event.Equipment.Id] = serviceTerminationState;
            
            return serviceTerminationState;
        }

        public ServiceTerminationState ProcessTerminalEquipmentNamingInfoChanged(TerminalEquipmentNamingInfoChanged @event)
        {
            if (_serviceTerminationStateByEquipmentId.ContainsKey(@event.TerminalEquipmentId))
            {
                var serviceTermination = _serviceTerminationStateByEquipmentId[@event.TerminalEquipmentId];
                serviceTermination.Name = @event.NamingInfo?.Name;

                return serviceTermination;
            }

            return null;
        }

        public void ProcessTerminalEquipmentRemoved(Guid terminalEquipmentId)
        {
            if (_serviceTerminationStateByEquipmentId.ContainsKey(terminalEquipmentId))
                _serviceTerminationStateByEquipmentId.Remove(terminalEquipmentId);
        }

        #endregion

        #region Work Task Information
        private Dictionary<Guid, WorkTaskState> _workTaskStateById = new();
        public IEnumerable<WorkTaskState> WorkTaskStates => _workTaskStateById.Values;

        public WorkTaskState ProcessWorkTaskCreated(WorkTaskCreated @event)
        {
            if (String.IsNullOrEmpty(@event.WorkTask.Status))
                return null;
                
            var workTaskState = WorkTaskState.Create(@event);
            _workTaskStateById[@event.WorkTaskId.Value] = workTaskState;

            return workTaskState;
        }

        public WorkTaskState ProcessWorkTaskStatusChanged(WorkTaskStatusChanged @event)
        {
            if (_workTaskStateById.ContainsKey(@event.WorkTaskId))
            {
                var workTask = _workTaskStateById[@event.WorkTaskId];
                workTask.Status = @event.Status;

                return workTask;
            }

            return null;
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

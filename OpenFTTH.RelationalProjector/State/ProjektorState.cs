using NetTopologySuite.Triangulate;
using OpenFTTH.RouteNetwork.API.Model;
using OpenFTTH.UtilityGraphService.API.Model.UtilityNetwork;
using OpenFTTH.UtilityGraphService.Business.NodeContainers.Events;
using OpenFTTH.UtilityGraphService.Business.SpanEquipments.Events;
using OpenFTTH.UtilityGraphService.Business.TerminalEquipments.Events;
using OpenFTTH.Work.Business.Events;
using Polly;
using System;
using System.Collections.Generic;
using System.Linq;

namespace OpenFTTH.RelationalProjector.State
{
    public class ProjektorState
    {
        #region Node Container Information
        private Dictionary<Guid, NodeContainerSpecification> _nodeContainerSpecificationById = new();
        private Dictionary<Guid, NodeContainerState> _nodeContainerStateById = new();
        private Dictionary<Guid, Guid> _nodeContainerToRouteNodeRelation = new();
        private Dictionary<Guid, Guid> _routeNodeToNodeContainerRelation = new();

        public IEnumerable<NodeContainerState> NodeContainerStates => _nodeContainerStateById.Values;

        public void ProcessNodeContainerSpecificationAdded(NodeContainerSpecificationAdded @event)
        {
            _nodeContainerSpecificationById[@event.Specification.Id] = @event.Specification;
        }
        public NodeContainerState ProcessNodeContainerSpecificationChanged(NodeContainerSpecificationChanged @event)
        {
            var nodeContainerSpecification = _nodeContainerSpecificationById[@event.NewSpecificationId];

            var nodeContainerState = _nodeContainerStateById[@event.NodeContainerId];

            nodeContainerState.LatestChangeType = LatestChangeType.UPDATED;

            nodeContainerState.SpecificationId = @event.NewSpecificationId;

            nodeContainerState.SpecificationName = nodeContainerSpecification.Name;

            nodeContainerState.SpecificationCategory = nodeContainerSpecification.Category;

            return nodeContainerState;
        }


        public NodeContainerState ProcessNodeContainerAdded(NodeContainerPlacedInRouteNetwork @event)
        {
            // Add state

            var nodeContainerSpecification = _nodeContainerSpecificationById[@event.Container.SpecificationId];

            var nodeContainerState = new NodeContainerState(LatestChangeType.NEW, @event.Container, nodeContainerSpecification.Name, nodeContainerSpecification.Category);
            _nodeContainerStateById[@event.Container.Id] = nodeContainerState;

            // Update relation dictionaries
            _nodeContainerToRouteNodeRelation[@event.Container.Id] = @event.Container.RouteNodeId;
            _routeNodeToNodeContainerRelation[@event.Container.RouteNodeId] = @event.Container.Id;

            // Remove conduit slack if any, unless uknown conduit junction
            if (@event.Container.SpecificationId != Guid.Parse("c288e797-a65c-4cf6-b63d-5eda4b4a8a8c"))
            {
                if (_conduitSlackStateByRouteNodeId.ContainsKey(@event.Container.RouteNodeId))
                    _conduitSlackStateByRouteNodeId.Remove(@event.Container.RouteNodeId);
            }

            return nodeContainerState;
        }

        public void ProcessNodeContainerRemoved(Guid nodeContainerId)
        {
            if (_nodeContainerStateById.ContainsKey(nodeContainerId))
                _nodeContainerStateById.Remove(nodeContainerId);

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
        private Dictionary<Guid, List<SpanEquipmentState>> _spanEquipmentParentsByChildId = new();
        private Dictionary<Guid, TerminalConnectivityState> _terminalConnectivityStateById = new();

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

            stateChanges.Add(spanEquipmentState);


            // process eventurally utility hops
            if (equipment.UtilityNetworkHops != null && (equipment.UtilityNetworkHops.Length > 0))
            {
                stateChanges.AddRange(ProcessSpanEquipmentAffixedToParent(
                    new SpanEquipmentAffixedToParent(equipment.Id, equipment.UtilityNetworkHops)
                ));
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

        public List<ObjectState> ProcessSpanEquipmentMerged(SpanEquipmentMerged @event)
        {
            // The logic should be applied as with move
            return ProcessSpanEquipmentMoved(
                new SpanEquipmentMoved(@event.SpanEquipmentId, @event.NodesOfInterestIds)
            );
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

        public List<ObjectState> ProcessSpanEquipmentAffixedToParent(SpanEquipmentAffixedToParent @event)
        {
            List<ObjectState> stateChanges = new List<ObjectState>();

            foreach (var utilityNetworkHop in @event.NewUtilityHopList)
            {
                foreach (var parentAffix in utilityNetworkHop.ParentAffixes)
                {
                    if (_spanEquipmentStateByRootSegmentId.TryGetValue(parentAffix.SpanSegmentId, out var parentSpanEquipmentState))
                    {
                        if (IsSpanEquipmentToNodeSlack(parentSpanEquipmentState))
                        {
                            stateChanges.AddRange(DecrementConduitSlackEndCount(parentSpanEquipmentState.ToNodeId));
                        }

                        if (IsSpanEquipmentFromNodeSlack(parentSpanEquipmentState))
                        {
                            stateChanges.AddRange(DecrementConduitSlackEndCount(parentSpanEquipmentState.FromNodeId));
                        }

                        parentSpanEquipmentState.ChildSpanEquipmentId = @event.SpanEquipmentId;
                        parentSpanEquipmentState.HasChildSpanEquipments = true;

                        // Update parent list
                        if (_spanEquipmentParentsByChildId.TryGetValue(@event.SpanEquipmentId, out var parents))
                        {
                            parents.Add(parentSpanEquipmentState);
                        }
                        else
                        {
                            _spanEquipmentParentsByChildId[@event.SpanEquipmentId] = new List<SpanEquipmentState>() { parentSpanEquipmentState };
                        }
                    }
                }
            }

            return stateChanges;
        }

        public List<ObjectState> ProcessSpanEquipmentDetachedFromParent(SpanEquipmentDetachedFromParent @event)
        {
            List<ObjectState> stateChanges = new List<ObjectState>();

            // Update parent list
            if (_spanEquipmentParentsByChildId.TryGetValue(@event.SpanEquipmentId, out var parents))
            {
                foreach (var parentSpanEquipment in parents)
                {
                    parentSpanEquipment.ChildSpanEquipmentId = Guid.Empty;
                    parentSpanEquipment.HasChildSpanEquipments = false;

                    if (IsSpanEquipmentToNodeSlack(parentSpanEquipment))
                    {
                        stateChanges.AddRange(IncrementConduitSlackEndCount(parentSpanEquipment.ToNodeId));
                    }

                    if (IsSpanEquipmentFromNodeSlack(parentSpanEquipment))
                    {
                        stateChanges.AddRange(IncrementConduitSlackEndCount(parentSpanEquipment.FromNodeId));
                    }
                }

                _spanEquipmentParentsByChildId.Remove(@event.SpanEquipmentId);
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

        private bool IsSpanEquipmentSlack(SpanEquipmentState spanEquipmentState, Guid routeNodeId)
        {
            if (spanEquipmentState.IsCustomerConduit && !_routeNodeToNodeContainerRelation.ContainsKey(routeNodeId) && !spanEquipmentState.HasChildSpanEquipments)
            {
                return true;
            }
            else
            {
                return false;
            }
        }


        private bool IsSpanEquipmentFromNodeSlack(SpanEquipmentState spanEquipmentState)
        {
            if (spanEquipmentState.IsCustomerConduit && !spanEquipmentState.RootSegmentHasFromConnection && !spanEquipmentState.HasChildSpanEquipments)
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
            if (spanEquipmentState.IsCustomerConduit && !spanEquipmentState.RootSegmentHasToConnection && !spanEquipmentState.HasChildSpanEquipments)
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
                    conduitSlackState.LatestChangeType = LatestChangeType.REMOVED;
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

        public List<ObjectState> ProcessTerminalEquipmentAddressInfoChanged(TerminalEquipmentAddressInfoChanged @event)
        {
            if (_serviceTerminationStateByEquipmentId.ContainsKey(@event.TerminalEquipmentId))
            {
                var serviceTerminationState = _serviceTerminationStateByEquipmentId[@event.TerminalEquipmentId];

                serviceTerminationState.LatestChangeType = LatestChangeType.UPDATED;

                serviceTerminationState.AccessAddressId = @event.AddressInfo.AccessAddressId;

                serviceTerminationState.UnitAddressId = @event.AddressInfo.UnitAddressId;

                return new List<ObjectState> { serviceTerminationState };
            }
            else
                return new List<ObjectState>();
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

    public class TerminalConnectivityState
    {
        public Guid TerminalId { get; set; }

        public SpanEquipmentState segment1 { get; set; }
        public SpanEquipmentState segment2 { get; set; }
        public Guid SharedNode { get; set; }

        public Guid GetSharedNodeId()
        {
            if (SharedNode != Guid.Empty)
                return SharedNode;

            if (segment1 == null || segment2 == null)
                throw new ApplicationException("GetSharedNodeId must not be called when segment variable are null");

            if (segment1.FromNodeId == segment2.FromNodeId)
                return segment1.FromNodeId;
            else if (segment1.FromNodeId == segment2.ToNodeId)
                return segment1.FromNodeId;
            else if (segment1.ToNodeId == segment2.ToNodeId)
                return segment1.ToNodeId;
            else if (segment1.ToNodeId == segment2.FromNodeId)
                return segment1.ToNodeId;
            else
                throw new ApplicationException($"Segments {segment1.Id} and {segment2.Id} is not connected");
        }

        public void RemoveSegment(Guid segmentId)
        {
            if (segment1 != null && segment1.Id == segmentId)
                segment1 = null;

            if (segment2 != null && segment2.Id == segmentId)
                segment2 = null;
        }

        public void AddSegment(SpanEquipmentState segmentState)
        {
            if (segment1 != null && segment1.Id == segmentState.Id)
                return;

            if (segment2 != null && segment2.Id == segmentState.Id)
                return;

            if (segment1 == null)
                segment1 = segmentState;
            else if (segment2 == null)
                segment2 = segmentState;

            if (IsFullyConnected())
                SharedNode = GetSharedNodeId();
        }

        public bool IsFullyConnected()
        {
            if (segment1 != null && segment2 != null)
                return true;
            else
                return false;
        }

        public bool IsFullyDisconnected()
        {
            if (segment1 == null && segment2 == null)
                return true;
            else
                return false;
        }
    }
}

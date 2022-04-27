using OpenFTTH.RouteNetwork.API.Model;
using OpenFTTH.UtilityGraphService.API.Model.UtilityNetwork;
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

        public void ProcessNodeContainerAdded(NodeContainerPlacedInRouteNetwork @event)
        {
            if (!_nodeContainerToRouteNodeRelation.ContainsKey(@event.Container.Id))
            {
                _nodeContainerToRouteNodeRelation.Add(@event.Container.Id, @event.Container.RouteNodeId);
            }
        }

        public void ProcessNodeContainerRemoved(Guid nodeContainerId)
        {
            if (_nodeContainerToRouteNodeRelation.ContainsKey(nodeContainerId))
            {
                _nodeContainerToRouteNodeRelation.Remove(nodeContainerId);
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

        public IEnumerable<SpanEquipmentState> SpanEquipmentStates => _spanEquipmentStateById.Values;
        public Dictionary<Guid, SpanEquipmentSpecification> SpanEquipmentSpecificationsById => _spanEquipmentSpecificationById;
        public Dictionary<Guid, SpanStructureSpecification> SpanStructureSpecificationsById => _spanStructureSpecificationById;

      

        public void ProcessSpanEquipmentAdded(SpanEquipment equipment)
        {
            _spanEquipmentStateById[equipment.Id] = SpanEquipmentState.Create(equipment);
        }

        public void ProcessSpanEquipmentMoved(SpanEquipmentMoved @event)
        {
            if (_spanEquipmentStateById.TryGetValue(@event.SpanEquipmentId, out var spanEquipment))
            {
                spanEquipment.FromNodeId = @event.NodesOfInterestIds.First();
                spanEquipment.ToNodeId = @event.NodesOfInterestIds.Last();
            }
        }

        public void ProcessSpanEquipmentRemoved(Guid spanEquipmentId)
        {
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

       

        public SpanEquipmentSpecification GetSpanEquipmentSpecification(Guid specificationId)
        {
            return _spanEquipmentSpecificationById[specificationId];
        }

        public SpanStructureSpecification GetSpanStructureSpecification(Guid spanStructureSpecificationId)
        {
            return _spanStructureSpecificationById[spanStructureSpecificationId];
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

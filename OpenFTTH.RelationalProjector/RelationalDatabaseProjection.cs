using Microsoft.Extensions.Logging;
using OpenFTTH.EventSourcing;
using OpenFTTH.RelationalProjector.Database;
using OpenFTTH.RelationalProjector.State;
using OpenFTTH.RouteNetwork.API.Model;
using OpenFTTH.RouteNetwork.Business.Interest.Events;
using OpenFTTH.UtilityGraphService.API.Model.UtilityNetwork;
using OpenFTTH.UtilityGraphService.Business.SpanEquipments.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace OpenFTTH.RelationalProjector
{
    public class RelationalDatabaseProjection : ProjectionBase
    {
        private readonly string _schemaName = "utility_network";

        private readonly ILogger<RelationalDatabaseProjection> _logger;
        private readonly PostgresWriter _dbWriter;

        private Dictionary<Guid, Guid[]> _interestToRouteElementRel = new();

        private Dictionary<Guid, SpanEquipmentSpecification> _spanEquipmentSpecificationById = new();

        private Dictionary<Guid, SpanStructureSpecification> _spanStructureSpecificationById = new();

        private Dictionary<Guid, SpanEquipmentState> _spanEquipmentStateById = new();

        private bool _bulkMode = true;

        public RelationalDatabaseProjection(ILogger<RelationalDatabaseProjection> logger, PostgresWriter dbWriter)
        {
            _logger = logger;
            _dbWriter = dbWriter;

            ProjectEvent<WalkOfInterestRegistered>(Project);
            ProjectEvent<WalkOfInterestRouteNetworkElementsModified>(Project);
            ProjectEvent<InterestUnregistered>(Project);

            ProjectEvent<SpanEquipmentPlacedInRouteNetwork>(Project);
            ProjectEvent<SpanEquipmentMoved>(Project);
            ProjectEvent<SpanEquipmentRemoved>(Project);

            ProjectEvent<SpanEquipmentSpecificationAdded>(Project);
            ProjectEvent<SpanStructureSpecificationAdded>(Project);
            ProjectEvent<SpanEquipmentSpecificationChanged>(Project);
            

            PrepareDatabase();
        }

        private void PrepareDatabase()
        {
            _dbWriter.CreateSchema(_schemaName);
            _dbWriter.CreateRouteElementToInterestTable(_schemaName);
            _dbWriter.CreateConduitTable(_schemaName);
            _dbWriter.CreateRouteSegmentLabelView(_schemaName);
        }

        private void Project(IEventEnvelope eventEnvelope)
        {
            switch (eventEnvelope.Data)
            {
                // Route network interest events
                case (WalkOfInterestRegistered @event):
                    Handle(@event);
                    break;

                case (WalkOfInterestRouteNetworkElementsModified @event):
                    Handle(@event);
                    break;

                case (InterestUnregistered @event):
                    Handle(@event);
                    break;


                // Span equipment events
                case (SpanEquipmentPlacedInRouteNetwork @event):
                    Handle(@event);
                    break;

                case (SpanEquipmentMoved @event):
                    Handle(@event);
                    break;

                case (SpanEquipmentRemoved @event):
                    Handle(@event);
                    break;


                // Span equipment specification events
                case (SpanEquipmentSpecificationAdded @event):
                    Handle(@event);
                    break;

                case (SpanStructureSpecificationAdded @event):
                    Handle(@event);
                    break;

                case (SpanEquipmentSpecificationChanged @event):
                    Handle(@event);
                    break;

            }
        }

        #region Interest events

        private void Handle(WalkOfInterestRegistered @event)
        {
            if (_bulkMode)
            {
                _interestToRouteElementRel[@event.Interest.Id] = RemoveDublicatedIds(@event.Interest.RouteNetworkElementRefs).ToArray();
            }
            else
            {
                _dbWriter.InsertGuidsIntoRouteElementToInterestTable(_schemaName, @event.Interest.Id, RemoveDublicatedIds(@event.Interest.RouteNetworkElementRefs));
            }
        }

        private void Handle(WalkOfInterestRouteNetworkElementsModified @event)
        {
            if (_bulkMode)
            {
                _interestToRouteElementRel[@event.InterestId] = RemoveDublicatedIds(@event.RouteNetworkElementIds).ToArray();
            }
            else
            {
                _dbWriter.DeleteGuidsFromRouteElementToInterestTable(_schemaName, @event.InterestId);
                _dbWriter.InsertGuidsIntoRouteElementToInterestTable(_schemaName, @event.InterestId, RemoveDublicatedIds(@event.RouteNetworkElementIds));
            }
        }

        private void Handle(InterestUnregistered @event)
        {
            if (_bulkMode)
            {
                if (_interestToRouteElementRel.ContainsKey(@event.InterestId))
                    _interestToRouteElementRel.Remove(@event.InterestId);
            }
            else
            {
                _dbWriter.DeleteGuidsFromRouteElementToInterestTable(_schemaName, @event.InterestId);
            }
        }

        #endregion

        #region Span Equipment Events
        private void Handle(SpanEquipmentPlacedInRouteNetwork @event)
        {
            _spanEquipmentStateById[@event.Equipment.Id] = SpanEquipmentState.Create(@event.Equipment);

            if (!_bulkMode)
            { 
                var spanEquipmentSpec = _spanEquipmentSpecificationById[@event.Equipment.SpecificationId];
                var structureSpec = _spanStructureSpecificationById[spanEquipmentSpec.RootTemplate.SpanStructureSpecificationId];

                if (!@event.Equipment.IsCable)
                    _dbWriter.InsertSpanEquipmentIntoConduitTable(_schemaName, @event.Equipment.Id, @event.Equipment.WalkOfInterestId, structureSpec.OuterDiameter.Value);
            }
        }

        private void Handle(SpanEquipmentMoved @event)
        {
            if (_spanEquipmentStateById.TryGetValue(@event.SpanEquipmentId, out var spanEquipment))
            {
                spanEquipment.FromNodeId = @event.NodesOfInterestIds.First();
                spanEquipment.FromNodeId = @event.NodesOfInterestIds.Last();
            }
        }

        private void Handle(SpanEquipmentRemoved @event)
        {
            if (_spanEquipmentStateById.TryGetValue(@event.SpanEquipmentId, out var spanEquipment))
            {
                if (!_bulkMode)
                {
                    if (!spanEquipment.IsCable)
                        _dbWriter.DeleteSpanEquipmentFromConduitTable(_schemaName, @event.SpanEquipmentId);
                }

                _spanEquipmentStateById.Remove(@event.SpanEquipmentId);
            }
        }

        #endregion

        #region Span Equipment Specification Events

        private void Handle(SpanEquipmentSpecificationChanged @event)
        {
            if (_bulkMode)
            {
                _spanEquipmentStateById[@event.SpanEquipmentId].SpecificationId = @event.NewSpecificationId;
            }
            else
            {
                _spanEquipmentStateById[@event.SpanEquipmentId].SpecificationId = @event.NewSpecificationId;

                var diameter = _spanStructureSpecificationById[_spanEquipmentSpecificationById[@event.NewSpecificationId].RootTemplate.SpanStructureSpecificationId].OuterDiameter;

                _dbWriter.UpdateSpanEquipmentDiameterInConduitTable(_schemaName, @event.SpanEquipmentId, diameter.Value);
            }
        }

        private void Handle(SpanEquipmentSpecificationAdded @event)
        {
            _spanEquipmentSpecificationById[@event.Specification.Id] = @event.Specification;
        }

        private void Handle(SpanStructureSpecificationAdded @event)
        {
            _spanStructureSpecificationById[@event.Specification.Id] = @event.Specification;
        }

        #endregion

        public override void DehydrationFinish()
        {
            _logger.LogInformation($"Bulk write to tables in schema: '{_schemaName}' started...");

            _dbWriter.BulkCopyGuidsToRouteElementToInterestTable(_schemaName, _interestToRouteElementRel);

            _dbWriter.BulkCopyIntoConduitTable(_schemaName, _spanEquipmentStateById.Values.ToList(), _spanEquipmentSpecificationById, _spanStructureSpecificationById);

            _interestToRouteElementRel = null;

            _bulkMode = false;

            _logger.LogInformation("Bulk write finish.");
        }

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

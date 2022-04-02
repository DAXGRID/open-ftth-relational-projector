using Microsoft.Extensions.Logging;
using OpenFTTH.EventSourcing;
using OpenFTTH.RelationalProjector.Database;
using OpenFTTH.RouteNetwork.API.Model;
using OpenFTTH.RouteNetwork.Business.Interest.Events;
using OpenFTTH.UtilityGraphService.API.Model.UtilityNetwork;
using OpenFTTH.UtilityGraphService.Business.Graph.Projections;
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

        private Dictionary<Guid, SpanEquipment> _spanEquipmentById = new();

        private bool _bulkMode = true;

        public RelationalDatabaseProjection(ILogger<RelationalDatabaseProjection> logger, PostgresWriter dbWriter)
        {
            _logger = logger;
            _dbWriter = dbWriter;

            ProjectEventAsync<WalkOfInterestRegistered>(Project);
            ProjectEventAsync<WalkOfInterestRouteNetworkElementsModified>(Project);
            ProjectEventAsync<InterestUnregistered>(Project);
            ProjectEventAsync<SpanEquipmentSpecificationAdded>(Project);
            ProjectEventAsync<SpanStructureSpecificationAdded>(Project);
            ProjectEventAsync<SpanEquipmentPlacedInRouteNetwork>(Project);
            ProjectEventAsync<SpanEquipmentSpecificationChanged>(Project);
            ProjectEventAsync<SpanEquipmentRemoved>(Project);

            PrepareDatabase();
        }

        private void PrepareDatabase()
        {
            _dbWriter.CreateSchema(_schemaName);
            _dbWriter.CreateRouteElementToInterestTable(_schemaName);
            _dbWriter.CreateConduitTable(_schemaName);
            _dbWriter.CreateRouteSegmentLabelView(_schemaName);
        }

        private async Task Project(IEventEnvelope eventEnvelope)
        {
            switch (eventEnvelope.Data)
            {
                case (WalkOfInterestRegistered @event):
                    Handle(@event);
                    break;

                case (WalkOfInterestRouteNetworkElementsModified @event):
                    Handle(@event);
                    break;

                case (InterestUnregistered @event):
                    Handle(@event);
                    break;

                case (SpanEquipmentSpecificationAdded @event):
                    Handle(@event);
                    break;

                case (SpanStructureSpecificationAdded @event):
                    Handle(@event);
                    break;

                case (SpanEquipmentPlacedInRouteNetwork @event):
                    Handle(@event);
                    break;

                case (SpanEquipmentRemoved @event):
                    Handle(@event);
                    break;

                case (SpanEquipmentSpecificationChanged @event):
                    Handle(@event);
                    break;
            }

            await Task.CompletedTask;
        }

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

        private void Handle(SpanEquipmentPlacedInRouteNetwork @event)
        {
            if (_bulkMode)
            {
                _spanEquipmentById[@event.Equipment.Id] = @event.Equipment;
            }
            else
            {
                var spanEquipmentSpec = _spanEquipmentSpecificationById[@event.Equipment.SpecificationId];
                var structureSpec = _spanStructureSpecificationById[spanEquipmentSpec.RootTemplate.SpanStructureSpecificationId];

                if (!spanEquipmentSpec.IsCable)
                    _dbWriter.InsertSpanEquipmentIntoConduitTable(_schemaName, @event.Equipment.Id, @event.Equipment.WalkOfInterestId, structureSpec.OuterDiameter.Value);
            }
        }

        private void Handle(SpanEquipmentRemoved @event)
        {
            if (_bulkMode)
            {
                if (_spanEquipmentById.ContainsKey(@event.SpanEquipmentId))
                    _spanEquipmentById.Remove(@event.SpanEquipmentId);
            }
            else
            {
                if (_spanEquipmentById.ContainsKey(@event.SpanEquipmentId))
                {
                    var spanEquipment = _spanEquipmentById[@event.SpanEquipmentId];

                    if (!spanEquipment.IsCable)
                        _dbWriter.DeleteSpanEquipmentFromConduitTable(_schemaName, @event.SpanEquipmentId);
                }
            }
        }

        private void Handle(SpanEquipmentSpecificationChanged @event)
        {
            if (_bulkMode)
            {
                _spanEquipmentById[@event.SpanEquipmentId] = SpanEquipmentProjectionFunctions.Apply(_spanEquipmentById[@event.SpanEquipmentId], @event);
            }
            else
            {
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

        public async override Task DehydrationFinishAsync()
        {
            _logger.LogInformation($"Bulk write to tables in schema: '{_schemaName}' started...");

            _dbWriter.BulkCopyGuidsToRouteElementToInterestTable(_schemaName, _interestToRouteElementRel);

            _dbWriter.BulkCopyIntoConduitTable(_schemaName, _spanEquipmentById.Values.ToList(), _spanEquipmentSpecificationById, _spanStructureSpecificationById);

            _interestToRouteElementRel = null;
            _spanEquipmentById = null;

            _bulkMode = false;

            _logger.LogInformation("Bulk write finish.");

            await Task.CompletedTask;
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

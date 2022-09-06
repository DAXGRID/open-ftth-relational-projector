using Microsoft.Extensions.Logging;
using OpenFTTH.EventSourcing;
using OpenFTTH.RelationalProjector.Database;
using OpenFTTH.RelationalProjector.State;
using OpenFTTH.RouteNetwork.API.Model;
using OpenFTTH.RouteNetwork.Business.Interest.Events;
using OpenFTTH.UtilityGraphService.Business.NodeContainers.Events;
using OpenFTTH.UtilityGraphService.Business.SpanEquipments.Events;
using OpenFTTH.UtilityGraphService.Business.TerminalEquipments.Events;
using OpenFTTH.Work.Business.Events;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace OpenFTTH.RelationalProjector
{
    public class RelationalDatabaseProjection : ProjectionBase
    {
        private readonly string _schemaName = "utility_network";

        private readonly ILogger<RelationalDatabaseProjection> _logger;
        private readonly PostgresWriter _dbWriter;

        private readonly ProjektorState _state = new();

        private bool _bulkMode = true;

        public RelationalDatabaseProjection(ILogger<RelationalDatabaseProjection> logger, PostgresWriter dbWriter)
        {
            _logger = logger;
            _dbWriter = dbWriter;

            // Node container events
            ProjectEventAsync<NodeContainerPlacedInRouteNetwork>(Project);
            ProjectEventAsync<NodeContainerRemovedFromRouteNetwork>(Project);

            // Interest events
            ProjectEventAsync<WalkOfInterestRegistered>(Project);
            ProjectEventAsync<WalkOfInterestRouteNetworkElementsModified>(Project);
            ProjectEventAsync<InterestUnregistered>(Project);

            // Span equipment events
            ProjectEventAsync<SpanEquipmentPlacedInRouteNetwork>(Project);
            ProjectEventAsync<SpanEquipmentMoved>(Project);
            ProjectEventAsync<SpanEquipmentRemoved>(Project);
            ProjectEventAsync<SpanSegmentsConnectedToSimpleTerminals>(Project);
            ProjectEventAsync<SpanSegmentsDisconnectedFromTerminals>(Project);
            ProjectEventAsync<SpanEquipmentAffixedToParent>(Project);
            ProjectEventAsync<SpanEquipmentDetachedFromParent>(Project);

            // Span equipment specification events
            ProjectEventAsync<SpanEquipmentSpecificationAdded>(Project);
            ProjectEventAsync<SpanStructureSpecificationAdded>(Project);
            ProjectEventAsync<SpanEquipmentSpecificationChanged>(Project);

            // Terminal equipment events
            ProjectEventAsync<TerminalEquipmentSpecificationAdded>(Project);
            ProjectEventAsync<TerminalEquipmentPlacedInNodeContainer>(Project);
            ProjectEventAsync<TerminalEquipmentRemoved>(Project);
            ProjectEventAsync<TerminalEquipmentNamingInfoChanged>(Project);

            // Work tasks
            ProjectEventAsync<WorkTaskCreated>(Project);
            ProjectEventAsync<WorkTaskStatusChanged>(Project);
        }

        private void PrepareDatabase()
        {
            _dbWriter.CreateSchema(_schemaName);
            _dbWriter.CreateRouteElementToInterestTable(_schemaName);
            _dbWriter.CreateSpanEquipmentTable(_schemaName);
            _dbWriter.CreateRouteSegmentLabelView(_schemaName);
            _dbWriter.CreateServiceTerminationTable(_schemaName);
            _dbWriter.CreateConduitSlackTable(_schemaName);
            _dbWriter.CreateWorkTaskTable(_schemaName);
            _dbWriter.CreateRouteNodeView(_schemaName);
            _dbWriter.CreateRouteSegmentView(_schemaName);
            _dbWriter.CreateRouteSegmentTaskStatusView(_schemaName);
            _dbWriter.CreateRouteNodeTaskStatusView(_schemaName);
        }

        private Task Project(IEventEnvelope eventEnvelope)
        {
            switch (eventEnvelope.Data)
            {
                // Node container events
                case (NodeContainerPlacedInRouteNetwork @event):
                    _state.ProcessNodeContainerAdded(@event);
                    break;

                case (NodeContainerRemovedFromRouteNetwork @event):
                    _state.ProcessNodeContainerRemoved(@event.NodeContainerId);
                    break;


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
                    _state.ProcessSpanEquipmentMoved(@event);
                    break;

                case (SpanEquipmentRemoved @event):
                    Handle(@event);
                    break;

                case (SpanSegmentsConnectedToSimpleTerminals @event):
                    _state.ProcessSpanEquipmentConnects(@event);
                    break;

                case (SpanSegmentsDisconnectedFromTerminals @event):
                    _state.ProcessSpanEquipmentDisconnects(@event);
                    break;

                case (SpanEquipmentAffixedToParent @event):
                    _state.ProcessSpanEquipmentAffixedToParent(@event);
                    break;

                case (SpanEquipmentDetachedFromParent @event):
                    _state.ProcessSpanEquipmentDetachedFromParent(@event);
                    break;


                // Span equipment specification events
                case (SpanEquipmentSpecificationAdded @event):
                    _state.ProcessSpanEquipmentSpecificationAdded(@event);
                    break;

                case (SpanStructureSpecificationAdded @event):
                    _state.ProcessSpanStructureSpecificationAdded(@event);
                    break;

                case (SpanEquipmentSpecificationChanged @event):
                    Handle(@event);
                    break;


                // Terminal equipment events
                case (TerminalEquipmentSpecificationAdded @event):
                    _state.ProcessTerminalEquipmentSpecificationAdded(@event);
                    break;

                case (TerminalEquipmentPlacedInNodeContainer @event):
                    Handle(@event);
                    break;

                case (TerminalEquipmentRemoved @event):
                    Handle(@event);
                    break;

                case (TerminalEquipmentNamingInfoChanged @event):
                    Handle(@event);
                    break;

                // Work tasks
                case (WorkTaskCreated @event):
                    Handle(@event);
                    break;

                case (WorkTaskStatusChanged @event):
                    Handle(@event);
                    break;
            }

            return Task.CompletedTask;
        }

        #region Interest events

        private void Handle(WalkOfInterestRegistered @event)
        {
            if (_bulkMode)
            {
                _state.ProcessWalkOfInterestAdded(@event.Interest);
            }
            else
            {
                _dbWriter.InsertGuidsIntoRouteElementToInterestTable(_schemaName, @event.Interest.Id, @event.Interest.RouteNetworkElementRefs);
            }
        }

        private void Handle(WalkOfInterestRouteNetworkElementsModified @event)
        {
            if (_bulkMode)
            {
                _state.ProcessWalkOfInterestUpdated(@event.InterestId, @event.RouteNetworkElementIds);
            }
            else
            {
                _dbWriter.DeleteGuidsFromRouteElementToInterestTable(_schemaName, @event.InterestId);
                _dbWriter.InsertGuidsIntoRouteElementToInterestTable(_schemaName, @event.InterestId, @event.RouteNetworkElementIds);
            }
        }

        private void Handle(InterestUnregistered @event)
        {
            if (_bulkMode)
            {
                _state.ProcessInterestRemoved(@event.InterestId);
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
            _state.ProcessSpanEquipmentAdded(@event.Equipment);

            if (!_bulkMode)
            {
                var spanEquipmentSpec = _state.GetSpanEquipmentSpecification(@event.Equipment.SpecificationId);
                var structureSpec = _state.GetSpanStructureSpecification(spanEquipmentSpec.RootTemplate.SpanStructureSpecificationId);

                _dbWriter.InsertSpanEquipment(_schemaName, @event.Equipment, spanEquipmentSpec, structureSpec.OuterDiameter.Value);
            }
        }

        private void Handle(SpanEquipmentRemoved @event)
        {
            if (_state.TryGetSpanEquipmentState(@event.SpanEquipmentId, out var spanEquipmentState))
            {
                if (!_bulkMode)
                {
                    if (!spanEquipmentState.IsCable)
                        _dbWriter.DeleteSpanEquipment(_schemaName, @event.SpanEquipmentId);
                }

                _state.ProcessSpanEquipmentRemoved(@event.SpanEquipmentId);
            }
        }

        #endregion

        #region Span Equipment Specification Events

        private void Handle(SpanEquipmentSpecificationChanged @event)
        {
            _state.ProcessSpanEquipmentChanged(@event);

            if (!_bulkMode)
            {
                var outerDiameter = _state.GetSpanStructureSpecification(_state.GetSpanEquipmentSpecification(@event.NewSpecificationId).RootTemplate.SpanStructureSpecificationId).OuterDiameter;

                _dbWriter.UpdateSpanEquipmentDiameter(_schemaName, @event.SpanEquipmentId, outerDiameter.Value);
            }
        }


        #endregion

        #region Terminal equipment events

        private void Handle(TerminalEquipmentPlacedInNodeContainer @event)
        {
            var serviceTerminationState = _state.ProcessServiceTerminationAdded(@event);

            if (serviceTerminationState != null && !_bulkMode)
            {
                _dbWriter.InsertIntoServiceTerminationTable(_schemaName, serviceTerminationState);
            }
        }

        private void Handle(TerminalEquipmentRemoved @event)
        {
            _state.ProcessTerminalEquipmentRemoved(@event.TerminalEquipmentId);

            if (!_bulkMode)
            {
                _dbWriter.DeleteServiceTermination(_schemaName, @event.TerminalEquipmentId);
            }
        }

        private void Handle(TerminalEquipmentNamingInfoChanged @event)
        {
            var serviceTerminationState = _state.ProcessTerminalEquipmentNamingInfoChanged(@event);

            if (serviceTerminationState != null && !_bulkMode)
            {
                _dbWriter.UpdateServiceTerminationName(_schemaName, @event.TerminalEquipmentId, @event.NamingInfo.Name);
            }
        }

        #endregion

        #region Work Task Events

        private void Handle(WorkTaskCreated @event)
        {
            var serviceTerminationState = _state.ProcessWorkTaskCreated(@event);

            if (serviceTerminationState != null && !_bulkMode)
            {
                _dbWriter.InsertIntoWorkTaskTable(_schemaName, serviceTerminationState);
            }
        }

        private void Handle(WorkTaskStatusChanged @event)
        {
            var workTasktate = _state.ProcessWorkTaskStatusChanged(@event);

            if (workTasktate != null && !_bulkMode)
            {
                _dbWriter.UpdateWorkTaskStatus(_schemaName, @event.WorkTaskId, @event.Status);
            }
        }


        #endregion

        public override Task DehydrationFinishAsync()
        {
            PrepareDatabase();

            _logger.LogInformation($"Bulk write to tables in schema: '{_schemaName}' started...");

            _logger.LogInformation($"Writing route element interest relations...");
            _dbWriter.BulkCopyGuidsToRouteElementToInterestTable(_schemaName, _state);

            _logger.LogInformation($"Writing service terminations...");
            _dbWriter.BulkCopyIntoServiceTerminationTable(_schemaName, _state);

            _logger.LogInformation($"Writing span equipments...");
            _dbWriter.BulkCopyIntoSpanEquipment(_schemaName, _state);

            _logger.LogInformation($"Writing conduit slacks...");
            _dbWriter.BulkCopyIntoConduitSlackTable(_schemaName, _state);

            _logger.LogInformation($"Writing work tasks...");
            _dbWriter.BulkCopyIntoWorkTaskTable(_schemaName, _state);

            _bulkMode = false;

            _logger.LogInformation("Bulk write finish.");

            return Task.CompletedTask;
        }

    }
}

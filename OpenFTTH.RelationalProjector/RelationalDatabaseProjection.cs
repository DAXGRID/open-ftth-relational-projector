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
            ProjectEventAsync<NodeContainerSpecificationAdded>(Project);
            ProjectEventAsync<NodeContainerPlacedInRouteNetwork>(Project);
            ProjectEventAsync<NodeContainerRemovedFromRouteNetwork>(Project);
            ProjectEventAsync<NodeContainerSpecificationChanged>(Project);

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
            ProjectEventAsync<SpanSegmentDisconnectedFromTerminal>(Project);
            ProjectEventAsync<SpanEquipmentAffixedToParent>(Project);
            ProjectEventAsync<SpanEquipmentDetachedFromParent>(Project);
            ProjectEventAsync<SpanEquipmentAddressInfoChanged>(Project);
            ProjectEventAsync<SpanEquipmentMerged>(Project);

            // Span equipment specification events
            ProjectEventAsync<SpanEquipmentSpecificationAdded>(Project);
            ProjectEventAsync<SpanStructureSpecificationAdded>(Project);
            ProjectEventAsync<SpanEquipmentSpecificationChanged>(Project);

            // Terminal equipment events
            ProjectEventAsync<TerminalEquipmentSpecificationAdded>(Project);
            ProjectEventAsync<TerminalEquipmentPlacedInNodeContainer>(Project);
            ProjectEventAsync<TerminalEquipmentRemoved>(Project);
            ProjectEventAsync<TerminalEquipmentNamingInfoChanged>(Project);
            ProjectEventAsync<TerminalEquipmentAddressInfoChanged>(Project);

            // Work tasks
            ProjectEventAsync<WorkTaskCreated>(Project);
            ProjectEventAsync<WorkTaskStatusChanged>(Project);
           
        }

        private void PrepareDatabase()
        {
            _dbWriter.CreateSchema(_schemaName);
            _dbWriter.CreateNodeContainerTable(_schemaName);
            _dbWriter.CreateRouteElementToInterestTable(_schemaName);
            _dbWriter.CreateSpanEquipmentTable(_schemaName);
            _dbWriter.CreateServiceTerminationTable(_schemaName);
            _dbWriter.CreateConduitSlackTable(_schemaName);
            _dbWriter.CreateWorkTaskTable(_schemaName);
        }

        private Task Project(IEventEnvelope eventEnvelope)
        {
            switch (eventEnvelope.Data)
            {
                // Node container events
                case (NodeContainerSpecificationAdded @event):
                    _state.ProcessNodeContainerSpecificationAdded(@event);
                    break;

                case (NodeContainerPlacedInRouteNetwork @event):
                    Handle(@event);
                    break;

                case (NodeContainerRemovedFromRouteNetwork @event):
                    Handle(@event);
                    break;

                case (NodeContainerSpecificationChanged @event):
                    Handle(@event);
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
                    ApplyStateChanges(_state.ProcessSpanEquipmentAdded(@event.Equipment));
                    break;

                case (SpanEquipmentMoved @event):
                    ApplyStateChanges(_state.ProcessSpanEquipmentMoved(@event));
                    break;

                case (SpanEquipmentRemoved @event):
                    ApplyStateChanges(_state.ProcessSpanEquipmentRemoved(@event.SpanEquipmentId));
                    break;

                case (SpanSegmentsConnectedToSimpleTerminals @event):
                    ApplyStateChanges(_state.ProcessSpanEquipmentConnects(@event));
                    break;

                case (SpanSegmentDisconnectedFromTerminal @event):
                    ApplyStateChanges(_state.ProcessSpanEquipmentDisconnects(@event));
                    break;

                case (SpanSegmentsDisconnectedFromTerminals @event):
                    ApplyStateChanges(_state.ProcessSpanEquipmentDisconnects(@event));
                    break;

                case (SpanEquipmentAffixedToParent @event):
                    ApplyStateChanges(_state.ProcessSpanEquipmentAffixedToParent(@event));
                    break;

                case (SpanEquipmentDetachedFromParent @event):
                    ApplyStateChanges(_state.ProcessSpanEquipmentDetachedFromParent(@event));
                    break;

                case (SpanEquipmentMerged @event):
                    ApplyStateChanges(_state.ProcessSpanEquipmentMerged(@event));
                    break;


                // Span equipment specification events
                case (SpanEquipmentSpecificationAdded @event):
                    _state.ProcessSpanEquipmentSpecificationAdded(@event);
                    break;

                case (SpanStructureSpecificationAdded @event):
                    _state.ProcessSpanStructureSpecificationAdded(@event);
                    break;

                case (SpanEquipmentSpecificationChanged @event):
                    ApplyStateChanges(_state.ProcessSpanEquipmentSpecificationChanged(@event));
                    break;

                case (SpanEquipmentAddressInfoChanged @event):
                    ApplyStateChanges(_state.ProcessSpanEquipmentAddressInfoChanged(@event));
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

                case (TerminalEquipmentAddressInfoChanged @event):
                    ApplyStateChanges(_state.ProcessTerminalEquipmentAddressInfoChanged(@event));
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

        #region Node Constainer events

        private void Handle(NodeContainerPlacedInRouteNetwork @event)
        {
            var nodeContainerState = _state.ProcessNodeContainerAdded(@event);

            if (nodeContainerState != null && !_bulkMode)
            {
                _dbWriter.InsertNodeContainer(_schemaName, nodeContainerState);
            }
        }

        private void Handle(NodeContainerRemovedFromRouteNetwork @event)
        {
            _state.ProcessNodeContainerRemoved(@event.NodeContainerId);

            if (!_bulkMode)
            {
                _dbWriter.DeleteNodeContainer(_schemaName, @event.NodeContainerId);
            }
        }

        private void Handle(NodeContainerSpecificationChanged @event)
        {
            var newState = _state.ProcessNodeContainerSpecificationChanged(@event);

            if (!_bulkMode)
            {
                _dbWriter.UpdateNodeContainer(_schemaName, newState);
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

        private void ApplyStateChanges(List<ObjectState> objectStates)
        {
            if (!_bulkMode)
            {
                foreach (var objectState in objectStates)
                {
                    switch (objectState)
                    {
                        case ConduitSlackState conduitSlackState:
                            if (conduitSlackState.LatestChangeType == LatestChangeType.NEW)
                                _dbWriter.InsertConduitSlack(_schemaName, conduitSlackState);
                            else if (conduitSlackState.LatestChangeType == LatestChangeType.UPDATED)
                                _dbWriter.UpdateConduitSlack(_schemaName, conduitSlackState);
                            else if (conduitSlackState.LatestChangeType == LatestChangeType.REMOVED)
                                _dbWriter.DeleteConduitSlack(_schemaName, conduitSlackState.RouteNodeId);
                            break;


                        case SpanEquipmentState spanEquipmentState:
                            if (spanEquipmentState.LatestChangeType == LatestChangeType.NEW)
                                _dbWriter.InsertSpanEquipment(_schemaName, spanEquipmentState);
                            else if (spanEquipmentState.LatestChangeType == LatestChangeType.UPDATED)
                                _dbWriter.UpdateSpanEquipment(_schemaName, spanEquipmentState);
                            if (spanEquipmentState.LatestChangeType == LatestChangeType.REMOVED)
                                _dbWriter.DeleteSpanEquipment(_schemaName, spanEquipmentState.Id);
                            break;

                        case ServiceTerminationState serviceTerminationState:
                            if (serviceTerminationState.LatestChangeType == LatestChangeType.UPDATED)
                                _dbWriter.UpdateServiceTermination(_schemaName, serviceTerminationState);
                            break;

                    }
                }
            }
        }

        public override Task DehydrationFinishAsync()
        {
            PrepareDatabase();

            _logger.LogInformation($"Bulk write to tables in schema: '{_schemaName}' started...");

            _logger.LogInformation($"Writing route element interest relations...");
            _dbWriter.BulkCopyGuidsToRouteElementToInterestTable(_schemaName, _state);

            _logger.LogInformation($"Writing node containers...");
            _dbWriter.BulkCopyIntoNodeContainerTable(_schemaName, _state);

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

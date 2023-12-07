using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using OpenFTTH.RelationalProjector.Settings;
using OpenFTTH.RelationalProjector.State;
using OpenFTTH.UtilityGraphService.API.Model.UtilityNetwork;
using System;
using System.Collections.Generic;
using System.Data;

namespace OpenFTTH.RelationalProjector.Database
{
    public class PostgresWriter
    {
        private readonly ILogger<PostgresWriter> _logger;
        private readonly GeoDatabaseSetting _databaseSetting;

        public PostgresWriter(ILogger<PostgresWriter> logger, IOptions<GeoDatabaseSetting> databaseSetting)
        {
            _logger = logger;
            _databaseSetting = databaseSetting.Value;
        }

        #region Route network element to interest relation table (rel_interest_to_route_element)
        public void CreateRouteElementToInterestTable(string schemaName, IDbTransaction transaction = null)
        {
            string createTableCmdText = $"CREATE TABLE IF NOT EXISTS {schemaName}.rel_interest_to_route_element (interest_id uuid, route_network_element_id uuid, seq_no integer, PRIMARY KEY(interest_id, route_network_element_id, seq_no));";
            _logger.LogDebug($"Execute SQL: {createTableCmdText}");

            RunDbCommand(transaction, createTableCmdText);

            // Create index on route_network_element_id column
            string createIndexCmdText1 = $"CREATE INDEX IF NOT EXISTS idx_rel_interest_to_route_element_route_network_element_id ON {schemaName}.rel_interest_to_route_element(route_network_element_id);";
            RunDbCommand(transaction, createIndexCmdText1);

            // Create index on interest_id column
            string createIndexCmdText2 = $"CREATE INDEX IF NOT EXISTS idx_rel_interest_to_route_element_interest_id ON {schemaName}.rel_interest_to_route_element(interest_id);";
            RunDbCommand(transaction, createIndexCmdText2);

        }

        public void BulkCopyGuidsToRouteElementToInterestTable(string schemaName, ProjektorState state)
        {
            using (var conn = GetConnection() as NpgsqlConnection)
            {
                conn.Open();

                // Truncate the table
                using (var truncateCmd = new NpgsqlCommand($"truncate table {schemaName}.rel_interest_to_route_element", conn))
                {
                    truncateCmd.ExecuteNonQuery();
                }

                using (var writer = conn.BeginBinaryImport($"copy {schemaName}.rel_interest_to_route_element (route_network_element_id, interest_id, seq_no) from STDIN (FORMAT BINARY)"))
                {
                    foreach (var walkOfInterestRouteElementRelations in state.WalkOfInterestToRouteElementRelations)
                    {
                        int seqNo = 1;
                        foreach (var routeElementId in walkOfInterestRouteElementRelations.Value)
                        {
                            writer.WriteRow(routeElementId, walkOfInterestRouteElementRelations.Key, seqNo);

                            seqNo++;
                        }
                    }

                    writer.Complete();
                }
            }
        }

        public void InsertGuidsIntoRouteElementToInterestTable(string schemaName, Guid interestId, IEnumerable<Guid> routeElementIds)
        {
            using (var conn = GetConnection() as NpgsqlConnection)
            {
                conn.Open();
                // Write guids to table
                using (var insertCmd = new NpgsqlCommand($"INSERT INTO {schemaName}.rel_interest_to_route_element (route_network_element_id, interest_id, seq_no) VALUES (@r, @i, @s)", conn))
                {
                    var routeNetworkElementIdparam = insertCmd.Parameters.Add("r", NpgsqlTypes.NpgsqlDbType.Uuid);
                    var interestIdparam = insertCmd.Parameters.Add("i", NpgsqlTypes.NpgsqlDbType.Uuid);
                    var seqNoparam = insertCmd.Parameters.Add("s", NpgsqlTypes.NpgsqlDbType.Integer);

                    var seqNo = 1;

                    foreach (var guid in routeElementIds)
                    {
                        interestIdparam.Value = interestId;
                        routeNetworkElementIdparam.Value = guid;
                        seqNoparam.Value = seqNo;

                        insertCmd.ExecuteNonQuery();

                        seqNo++;
                    }
                }
            }
        }

        public void DeleteGuidsFromRouteElementToInterestTable(string schemaName, Guid interestId)
        {
            using (var conn = GetConnection() as NpgsqlConnection)
            {
                conn.Open();
                using (var insertCmd = new NpgsqlCommand($"DELETE FROM {schemaName}.rel_interest_to_route_element WHERE interest_id = @i", conn))
                {
                    var interestIdparam = insertCmd.Parameters.Add("i", NpgsqlTypes.NpgsqlDbType.Uuid);
                    interestIdparam.Value = interestId;
                    insertCmd.ExecuteNonQuery();
                }
            }
        }
        #endregion


        #region Node Container
        public void CreateNodeContainerTable(string schemaName, IDbTransaction transaction = null)
        {
            // Create table
            string createTableCmdText = $"CREATE TABLE IF NOT EXISTS {schemaName}.node_container (id uuid, route_node_id uuid, spec_name character varying(255), spec_category character varying(255), PRIMARY KEY(id));";
            _logger.LogDebug($"Execute SQL: {createTableCmdText}");

            RunDbCommand(transaction, createTableCmdText);

            // Create index on route_node_id column
            string createIndexCmdText = $"CREATE INDEX IF NOT EXISTS idx_node_container_route_node_id ON {schemaName}.node_container(route_node_id);";
            RunDbCommand(transaction, createIndexCmdText);
        }

        public void InsertNodeContainer(string schemaName, NodeContainerState nodeContainerState)
        {
            using var conn = GetConnection() as NpgsqlConnection;

            conn.Open();

            using var insertCmd = conn.CreateCommand();

            insertCmd.CommandText = $"INSERT INTO {schemaName}.node_container (id, route_node_id, spec_name, spec_category) VALUES (@id, @route_node_id, @spec_name, @spec_category)";

            var idParam = insertCmd.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid);
            var routeNodeIdParam = insertCmd.Parameters.Add("route_node_id", NpgsqlTypes.NpgsqlDbType.Uuid);
            var specNameParam = insertCmd.Parameters.Add("spec_name", NpgsqlTypes.NpgsqlDbType.Varchar);
            var specCategoryParam = insertCmd.Parameters.Add("spec_category", NpgsqlTypes.NpgsqlDbType.Varchar);

            idParam.Value = nodeContainerState.Id;
            routeNodeIdParam.Value = nodeContainerState.RouteNodeId;
            specNameParam.Value = nodeContainerState.SpecificationName;
            specCategoryParam.Value = nodeContainerState.SpecificationCategory;

            insertCmd.ExecuteNonQuery();
        }

        public void UpdateNodeContainer(string schemaName, NodeContainerState nodeContainerState)
        {
            using (var conn = GetConnection() as NpgsqlConnection)
            {
                conn.Open();
                using (var updateCmd = new NpgsqlCommand($"UPDATE {schemaName}.node_container SET spec_name = @spec_name, spec_category = @spec_category  WHERE id = @id", conn))
                {
                    updateCmd.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid).Value = nodeContainerState.Id;

                    updateCmd.Parameters.Add("spec_name", NpgsqlTypes.NpgsqlDbType.Varchar).Value = nodeContainerState.SpecificationName;
                    updateCmd.Parameters.Add("spec_category", NpgsqlTypes.NpgsqlDbType.Varchar).Value = nodeContainerState.SpecificationCategory;

                    updateCmd.ExecuteNonQuery();
                }
            }
        }

        public void DeleteNodeContainer(string schemaName, Guid spanEquipmentId)
        {
            using (var conn = GetConnection() as NpgsqlConnection)
            {
                conn.Open();
                using (var deleteCmd = new NpgsqlCommand($"DELETE FROM {schemaName}.node_container WHERE id = @i", conn))
                {
                    var idparam = deleteCmd.Parameters.Add("i", NpgsqlTypes.NpgsqlDbType.Uuid);
                    idparam.Value = spanEquipmentId;
                    deleteCmd.ExecuteNonQuery();
                }
            }
        }

        public void BulkCopyIntoNodeContainerTable(string schemaName, ProjektorState state)
        {
            using (var conn = GetConnection() as NpgsqlConnection)
            {
                conn.Open();

                // Truncate the table
                using (var truncateCmd = new NpgsqlCommand($"truncate table {schemaName}.node_container", conn))
                {
                    truncateCmd.ExecuteNonQuery();
                }

                using (var writer = conn.BeginBinaryImport($"copy {schemaName}.node_container (id, route_node_id, spec_name, spec_category) from STDIN (FORMAT BINARY)"))
                {
                    foreach (var nodeContainer in state.NodeContainerStates)
                    {
                        writer.WriteRow(nodeContainer.Id, nodeContainer.RouteNodeId, nodeContainer.SpecificationName, nodeContainer.SpecificationCategory);
                    }

                    writer.Complete();
                }
            }

        }
        #endregion


        #region Span Equipment table
        public void CreateSpanEquipmentTable(string schemaName, IDbTransaction transaction = null)
        {
            // Create table
            string createTableCmdText = $"CREATE TABLE IF NOT EXISTS {schemaName}.span_equipment (id uuid, interest_id uuid, outer_diameter integer, is_cable boolean, name character varying(255), spec_name character varying(255), access_address_id uuid, unit_address_id uuid, PRIMARY KEY(id));";
            _logger.LogDebug($"Execute SQL: {createTableCmdText}");

            RunDbCommand(transaction, createTableCmdText);

            // Create index on interest_id column
            string createIndexCmdText = $"CREATE INDEX IF NOT EXISTS idx_span_equipment_interest_id ON {schemaName}.span_equipment(interest_id);";
            RunDbCommand(transaction, createIndexCmdText);
        }

        public void InsertSpanEquipment(string schemaName, SpanEquipmentState spanEquipmentState)
        {
            using var conn = GetConnection() as NpgsqlConnection;

            conn.Open();

            using var insertCmd = conn.CreateCommand();

            insertCmd.CommandText = $"INSERT INTO {schemaName}.span_equipment (id, interest_id, outer_diameter, is_cable, name, spec_name, access_address_id, unit_address_id) VALUES (@id, @interest_id, @outer_diameter, @is_cable, @name, @spec_name, @access_address_id, @unit_address_id)";

            insertCmd.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid).Value = spanEquipmentState.Id;

            insertCmd.Parameters.Add("interest_id", NpgsqlTypes.NpgsqlDbType.Uuid).Value = spanEquipmentState.WalkOfInterestId;

            insertCmd.Parameters.Add("outer_diameter", NpgsqlTypes.NpgsqlDbType.Integer).Value = spanEquipmentState.OuterDiameter;

            insertCmd.Parameters.Add("is_cable", NpgsqlTypes.NpgsqlDbType.Boolean).Value = spanEquipmentState.IsCable;

            insertCmd.Parameters.Add("name", NpgsqlTypes.NpgsqlDbType.Varchar).Value = spanEquipmentState.Name;

            insertCmd.Parameters.Add("spec_name", NpgsqlTypes.NpgsqlDbType.Varchar).Value = spanEquipmentState.SpecificationName;

            insertCmd.Parameters.Add("access_address_id", NpgsqlTypes.NpgsqlDbType.Uuid).Value = spanEquipmentState.AccessAddressId is null ? DBNull.Value : spanEquipmentState.AccessAddressId;

            insertCmd.Parameters.Add("unit_address_id", NpgsqlTypes.NpgsqlDbType.Uuid).Value = spanEquipmentState.UnitAddressId is null ? DBNull.Value : spanEquipmentState.UnitAddressId;

            insertCmd.ExecuteNonQuery();
        }

        public void UpdateSpanEquipment(string schemaName, SpanEquipmentState spanEquipmentState)
        {
            using (var conn = GetConnection() as NpgsqlConnection)
            {
                conn.Open();
                using (var updateCmd = new NpgsqlCommand($"UPDATE {schemaName}.span_equipment SET outer_diameter = @outer_diameter,  name = @name, spec_name = @spec_name, access_address_id = @access_address_id, unit_address_id = @unit_address_id WHERE id = @id", conn))
                {
                    updateCmd.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid).Value = spanEquipmentState.Id;

                    updateCmd.Parameters.Add("name", NpgsqlTypes.NpgsqlDbType.Varchar).Value = spanEquipmentState.Name;

                    updateCmd.Parameters.Add("spec_name", NpgsqlTypes.NpgsqlDbType.Varchar).Value = spanEquipmentState.SpecificationName;

                    updateCmd.Parameters.Add("outer_diameter", NpgsqlTypes.NpgsqlDbType.Integer).Value = spanEquipmentState.OuterDiameter;

                    updateCmd.Parameters.Add("access_address_id", NpgsqlTypes.NpgsqlDbType.Uuid).Value = spanEquipmentState.AccessAddressId is null ? DBNull.Value : spanEquipmentState.AccessAddressId;

                    updateCmd.Parameters.Add("unit_address_id", NpgsqlTypes.NpgsqlDbType.Uuid).Value = spanEquipmentState.UnitAddressId is null ? DBNull.Value : spanEquipmentState.UnitAddressId;

                    updateCmd.ExecuteNonQuery();
                }
            }
        }

        public void DeleteSpanEquipment(string schemaName, Guid spanEquipmentId)
        {
            using (var conn = GetConnection() as NpgsqlConnection)
            {
                conn.Open();
                using (var deleteCmd = new NpgsqlCommand($"DELETE FROM {schemaName}.span_equipment WHERE id = @i", conn))
                {
                    deleteCmd.Parameters.Add("i", NpgsqlTypes.NpgsqlDbType.Uuid).Value = spanEquipmentId;
                    deleteCmd.ExecuteNonQuery();
                }
            }
        }

        public void BulkCopyIntoSpanEquipment(string schemaName, ProjektorState state)
        {
            using (var conn = GetConnection() as NpgsqlConnection)
            {
                conn.Open();

                // Truncate the table
                using (var truncateCmd = new NpgsqlCommand($"truncate table {schemaName}.span_equipment", conn))
                {
                    truncateCmd.ExecuteNonQuery();
                }

                using (var writer = conn.BeginBinaryImport($"copy {schemaName}.span_equipment (id, interest_id, outer_diameter, is_cable, name, spec_name, access_address_id, unit_address_id) from STDIN (FORMAT BINARY)"))
                {
                    foreach (var spanEquipment in state.SpanEquipmentStates)
                    {
              
                        writer.WriteRow(spanEquipment.Id, spanEquipment.WalkOfInterestId, spanEquipment.OuterDiameter, spanEquipment.IsCable, spanEquipment.Name, spanEquipment.SpecificationName, spanEquipment.AccessAddressId is null ? DBNull.Value : spanEquipment.AccessAddressId, spanEquipment.UnitAddressId is null ? DBNull.Value : spanEquipment.UnitAddressId);
                    }

                    writer.Complete();
                }
            }

        }

        #endregion


        #region Service Termination Point
        public void CreateServiceTerminationTable(string schemaName, IDbTransaction transaction = null)
        {
            // Create table
            string createTableCmdText = $"CREATE TABLE IF NOT EXISTS {schemaName}.service_termination (id uuid, route_node_id uuid, name character varying(255), access_address_id uuid, unit_address_id uuid, PRIMARY KEY(id));";
            _logger.LogDebug($"Execute SQL: {createTableCmdText}");

            RunDbCommand(transaction, createTableCmdText);

            // Create index on route_node_id column
            string createIndexCmdText = $"CREATE INDEX IF NOT EXISTS idx_service_termination_route_node_id ON {schemaName}.service_termination(route_node_id);";
            RunDbCommand(transaction, createIndexCmdText);
        }

        public void InsertIntoServiceTerminationTable(string schemaName, ServiceTerminationState serviceTerminationState)
        {
            using var conn = GetConnection() as NpgsqlConnection;

            conn.Open();

            using var insertCmd = conn.CreateCommand();

            insertCmd.CommandText = $"INSERT INTO {schemaName}.service_termination (id, route_node_id, name, access_address_id, unit_address_id) VALUES (@id, @route_node_id, @name, @access_address_id, @unit_address_id)";

            var idParam = insertCmd.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid);
            var routeNodeIdParam = insertCmd.Parameters.Add("route_node_id", NpgsqlTypes.NpgsqlDbType.Uuid);
            var nameParam = insertCmd.Parameters.Add("name", NpgsqlTypes.NpgsqlDbType.Varchar);

            idParam.Value = serviceTerminationState.Id;
            routeNodeIdParam.Value = serviceTerminationState.RouteNodeId;
            nameParam.Value = serviceTerminationState.Name;

            insertCmd.Parameters.Add("access_address_id", NpgsqlTypes.NpgsqlDbType.Uuid).Value = serviceTerminationState.AccessAddressId is null ? DBNull.Value : serviceTerminationState.AccessAddressId;

            insertCmd.Parameters.Add("unit_address_id", NpgsqlTypes.NpgsqlDbType.Uuid).Value = serviceTerminationState.UnitAddressId is null ? DBNull.Value : serviceTerminationState.UnitAddressId;

            insertCmd.ExecuteNonQuery();
        }

        public void DeleteServiceTermination(string schemaName, Guid spanEquipmentId)
        {
            using (var conn = GetConnection() as NpgsqlConnection)
            {
                conn.Open();
                using (var deleteCmd = new NpgsqlCommand($"DELETE FROM {schemaName}.service_termination WHERE id = @i", conn))
                {
                    var idparam = deleteCmd.Parameters.Add("i", NpgsqlTypes.NpgsqlDbType.Uuid);
                    idparam.Value = spanEquipmentId;
                    deleteCmd.ExecuteNonQuery();
                }
            }
        }

        public void UpdateServiceTerminationName(string schemaName, Guid terminalEquipmentId, string name)
        {
            using (var conn = GetConnection() as NpgsqlConnection)
            {
                conn.Open();
                using (var updateCmd = new NpgsqlCommand($"UPDATE {schemaName}.service_termination SET name = @n WHERE id = @i", conn))
                {
                    var idParam = updateCmd.Parameters.Add("i", NpgsqlTypes.NpgsqlDbType.Uuid);
                    idParam.Value = terminalEquipmentId;

                    var nameParam = updateCmd.Parameters.Add("n", NpgsqlTypes.NpgsqlDbType.Varchar);
                    nameParam.Value = name;

                    updateCmd.ExecuteNonQuery();
                }
            }
        }

        public void UpdateServiceTermination(string schemaName, ServiceTerminationState serviceTerminationState)
        {
            using (var conn = GetConnection() as NpgsqlConnection)
            {
                conn.Open();
                using (var updateCmd = new NpgsqlCommand($"UPDATE {schemaName}.service_termination SET name = @name, access_address_id = @access_address_id, unit_address_id = @unit_address_id WHERE id = @id", conn))
                {
                    updateCmd.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid).Value = serviceTerminationState.Id;

                    updateCmd.Parameters.Add("name", NpgsqlTypes.NpgsqlDbType.Varchar).Value = serviceTerminationState.Name;

                    updateCmd.Parameters.Add("access_address_id", NpgsqlTypes.NpgsqlDbType.Uuid).Value = serviceTerminationState.AccessAddressId is null ? DBNull.Value : serviceTerminationState.AccessAddressId;

                    updateCmd.Parameters.Add("unit_address_id", NpgsqlTypes.NpgsqlDbType.Uuid).Value = serviceTerminationState.UnitAddressId is null ? DBNull.Value : serviceTerminationState.UnitAddressId;

                    updateCmd.ExecuteNonQuery();
                }
            }
        }


        public void BulkCopyIntoServiceTerminationTable(string schemaName, ProjektorState state)
        {
            using (var conn = GetConnection() as NpgsqlConnection)
            {
                conn.Open();

                // Truncate the table
                using (var truncateCmd = new NpgsqlCommand($"truncate table {schemaName}.service_termination", conn))
                {
                    truncateCmd.ExecuteNonQuery();
                }

                using (var writer = conn.BeginBinaryImport($"copy {schemaName}.service_termination (id, route_node_id, name, access_address_id, unit_address_id) from STDIN (FORMAT BINARY)"))
                {
                    foreach (var serviceTermination in state.ServiceTerminationStates)
                    {
                        writer.WriteRow(serviceTermination.Id, serviceTermination.RouteNodeId, serviceTermination.Name, serviceTermination.AccessAddressId is null ? DBNull.Value : serviceTermination.AccessAddressId, serviceTermination.UnitAddressId is null ? DBNull.Value : serviceTermination.UnitAddressId);
                    }

                    writer.Complete();
                }
            }

        }
        #endregion


        #region Conduit Slack
        public void CreateConduitSlackTable(string schemaName, IDbTransaction transaction = null)
        {
            // Create table
            string createTableCmdText = $"CREATE TABLE IF NOT EXISTS {schemaName}.conduit_slack (id uuid, route_node_id uuid, number_of_ends integer, PRIMARY KEY(id));";
            _logger.LogDebug($"Execute SQL: {createTableCmdText}");

            RunDbCommand(transaction, createTableCmdText);

            // Create index on route_node_id column
            string createIndexCmdText = $"CREATE INDEX IF NOT EXISTS idx_conduit_slack_route_node_id ON {schemaName}.conduit_slack(route_node_id);";
            RunDbCommand(transaction, createIndexCmdText);
        }

        public void BulkCopyIntoConduitSlackTable(string schemaName, ProjektorState state)
        {
            using (var conn = GetConnection() as NpgsqlConnection)
            {
                conn.Open();

                // Truncate the table
                using (var truncateCmd = new NpgsqlCommand($"truncate table {schemaName}.conduit_slack", conn))
                {
                    truncateCmd.ExecuteNonQuery();
                }

                using (var writer = conn.BeginBinaryImport($"copy {schemaName}.conduit_slack (id, route_node_id, number_of_ends) from STDIN (FORMAT BINARY)"))
                {
                    foreach (var conduitSlack in state.ConduitSlackStates)
                    {
                        writer.WriteRow(conduitSlack.Id, conduitSlack.RouteNodeId, conduitSlack.NumberOfConduitEnds);
                    }

                    writer.Complete();
                }
            }
        }

        public void InsertConduitSlack(string schemaName, ConduitSlackState state)
        {
            using (var conn = GetConnection() as NpgsqlConnection)
            {
                conn.Open();
                using (var updateCmd = new NpgsqlCommand($"INSERT INTO {schemaName}.conduit_slack (id, route_node_id, number_of_ends) VALUES (@id, @route_node_id, @number_of_ends)", conn))
                {
                    updateCmd.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid).Value = state.Id;

                    updateCmd.Parameters.Add("route_node_id", NpgsqlTypes.NpgsqlDbType.Uuid).Value = state.RouteNodeId;

                    updateCmd.Parameters.Add("number_of_ends", NpgsqlTypes.NpgsqlDbType.Integer).Value = state.NumberOfConduitEnds;

                    updateCmd.ExecuteNonQuery();
                }
            }
        }

        public void UpdateConduitSlack(string schemaName,ConduitSlackState state)
        {
            using (var conn = GetConnection() as NpgsqlConnection)
            {
                conn.Open();
                using (var updateCmd = new NpgsqlCommand($"UPDATE {schemaName}.conduit_slack SET number_of_ends = @number_of_ends WHERE route_node_id = @route_node_id", conn))
                {
                    updateCmd.Parameters.Add("route_node_id", NpgsqlTypes.NpgsqlDbType.Uuid).Value = state.RouteNodeId;

                    updateCmd.Parameters.Add("number_of_ends", NpgsqlTypes.NpgsqlDbType.Integer).Value = state.NumberOfConduitEnds;

                    updateCmd.ExecuteNonQuery();
                }
            }
        }

        public void DeleteConduitSlack(string schemaName, Guid routeNodeId)
        {
            using (var conn = GetConnection() as NpgsqlConnection)
            {
                conn.Open();
                using (var deleteCmd = new NpgsqlCommand($"DELETE FROM {schemaName}.conduit_slack WHERE route_node_id = @route_node_id", conn))
                {
                    deleteCmd.Parameters.Add("route_node_id", NpgsqlTypes.NpgsqlDbType.Uuid).Value = routeNodeId;
                    deleteCmd.ExecuteNonQuery();
                }
            }
        }

        #endregion


        #region Work Task
        public void CreateWorkTaskTable(string schemaName, IDbTransaction transaction = null)
        {
            // Create table
            string createTableCmdText = $"CREATE TABLE IF NOT EXISTS {schemaName}.work_task (id uuid, number character varying(255), status character varying(255), PRIMARY KEY(id));";
            _logger.LogDebug($"Execute SQL: {createTableCmdText}");

            RunDbCommand(transaction, createTableCmdText);
        }

        public void InsertIntoWorkTaskTable(string schemaName, WorkTaskState workTaskState)
        {
            using var conn = GetConnection() as NpgsqlConnection;

            conn.Open();

            using var insertCmd = conn.CreateCommand();

            insertCmd.CommandText = $"INSERT INTO {schemaName}.work_task (id, number, status) VALUES (@id, @number, @status)";

            var idParam = insertCmd.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid);
            var numberParam = insertCmd.Parameters.Add("number", NpgsqlTypes.NpgsqlDbType.Varchar);
            var statusParam = insertCmd.Parameters.Add("status", NpgsqlTypes.NpgsqlDbType.Varchar);

            idParam.Value = workTaskState.Id;
            numberParam.Value = workTaskState.Number;
            statusParam.Value = workTaskState.Status;

            insertCmd.ExecuteNonQuery();
        }

        public void UpdateWorkTaskStatus(string schemaName, Guid workTaskId, string status)
        {
            using (var conn = GetConnection() as NpgsqlConnection)
            {
                conn.Open();
                using (var updateCmd = new NpgsqlCommand($"UPDATE {schemaName}.work_task SET status = @n WHERE id = @i", conn))
                {
                    var idParam = updateCmd.Parameters.Add("i", NpgsqlTypes.NpgsqlDbType.Uuid);
                    idParam.Value = workTaskId;

                    var statusParam = updateCmd.Parameters.Add("n", NpgsqlTypes.NpgsqlDbType.Varchar);
                    statusParam.Value = status;

                    updateCmd.ExecuteNonQuery();
                }
            }
        }

        public void BulkCopyIntoWorkTaskTable(string schemaName, ProjektorState state)
        {
            using (var conn = GetConnection() as NpgsqlConnection)
            {
                conn.Open();

                // Truncate the table
                using (var truncateCmd = new NpgsqlCommand($"truncate table {schemaName}.work_task", conn))
                {
                    truncateCmd.ExecuteNonQuery();
                }

                using (var writer = conn.BeginBinaryImport($"copy {schemaName}.work_task (id, number, status) from STDIN (FORMAT BINARY)"))
                {
                    foreach (var workTask in state.WorkTaskStates)
                    {
                        writer.WriteRow(workTask.Id, workTask.Number, workTask.Status);
                    }

                    writer.Complete();
                }
            }

        }

        #endregion


        #region Generel database commands
        public IDbConnection GetConnection()
        {
            return new NpgsqlConnection(_databaseSetting.PostgresConnectionString);
        }

        public void CreateSchema(string schemaName, IDbTransaction transaction = null)
        {
            string createSchemaCmdText = $"CREATE SCHEMA IF NOT EXISTS {schemaName};";

            _logger.LogDebug($"Execute SQL: {createSchemaCmdText}");

            RunDbCommand(transaction, createSchemaCmdText);
        }

        public void DropSchema(string schemaName, IDbTransaction transaction = null)
        {
            string deleteSchemaCmdText = $"DROP SCHEMA IF EXISTS {schemaName} CASCADE;";

            _logger.LogDebug($"Execute SQL: {deleteSchemaCmdText}");

            RunDbCommand(transaction, deleteSchemaCmdText);
        }

        public void TruncateTable(string schemaName, string tableName, IDbTransaction trans = null)
        {
            if (trans != null)
            {
                // Truncate the table
                using (var truncateCmd = new NpgsqlCommand($"truncate table {schemaName}.{tableName}", (NpgsqlConnection)trans.Connection, (NpgsqlTransaction)trans))
                {
                    truncateCmd.ExecuteNonQuery();
                }
            }
            else
            {
                using (var conn = GetConnection() as NpgsqlConnection)
                {
                    conn.Open();

                    // Truncate the table
                    using (var truncateCmd = new NpgsqlCommand($"truncate table {schemaName}.{tableName}", conn))
                    {
                        truncateCmd.ExecuteNonQuery();
                    }
                }
            }
        }

        private void RunDbCommand(IDbTransaction transaction, string createTableCmdText)
        {
            _logger.LogDebug($"Execute SQL: {createTableCmdText}");

            if (transaction == null)
            {
                using (var conn = GetConnection())
                {
                    conn.Open();

                    using (var createSchemaCmd = new NpgsqlCommand(createTableCmdText, (NpgsqlConnection)conn))
                    {
                        createSchemaCmd.ExecuteNonQuery();
                    }
                }
            }
            else
            {
                using (var createSchemaCmd = new NpgsqlCommand(createTableCmdText, (NpgsqlConnection)transaction.Connection, (NpgsqlTransaction)transaction))
                {
                    createSchemaCmd.ExecuteNonQuery();
                }
            }
        }

        private void RunDbCommandIfNotExists(IDbTransaction transaction, string createTableCmdText)
        {
            _logger.LogDebug($"Execute SQL: {createTableCmdText}");

            try
            {
                if (transaction == null)
                {
                    using (var conn = GetConnection())
                    {
                        conn.Open();

                        using (var createSchemaCmd = new NpgsqlCommand(createTableCmdText, (NpgsqlConnection)conn))
                        {
                            createSchemaCmd.ExecuteNonQuery();
                        }
                    }
                }
                else
                {
                    using (var createSchemaCmd = new NpgsqlCommand(createTableCmdText, (NpgsqlConnection)transaction.Connection, (NpgsqlTransaction)transaction))
                    {
                        createSchemaCmd.ExecuteNonQuery();
                    }
                }
            }
            catch (PostgresException ex)
            {
                if (ex.Message.ToLower().Contains("already exists"))
                {
                    _logger.LogDebug(ex.Message);
                    return;
                }
                else
                {
                    throw;
                }
            }
        }

        #endregion
    }
}

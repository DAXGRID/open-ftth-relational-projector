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
            string createTableCmdText = $"CREATE TABLE IF NOT EXISTS {schemaName}.span_equipment (id uuid, interest_id uuid, outer_diameter integer, is_cable boolean, name character varying(255), spec_name character varying(255), PRIMARY KEY(id));";
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

            insertCmd.CommandText = $"INSERT INTO {schemaName}.span_equipment (id, interest_id, outer_diameter, is_cable, name, spec_name) VALUES (@id, @interest_id, @outer_diameter, @is_cable, @name, @spec_name)";

            insertCmd.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid).Value = spanEquipmentState.Id;

            insertCmd.Parameters.Add("interest_id", NpgsqlTypes.NpgsqlDbType.Uuid).Value = spanEquipmentState.WalkOfInterestId;

            insertCmd.Parameters.Add("outer_diameter", NpgsqlTypes.NpgsqlDbType.Integer).Value = spanEquipmentState.OuterDiameter;

            insertCmd.Parameters.Add("is_cable", NpgsqlTypes.NpgsqlDbType.Boolean).Value = spanEquipmentState.IsCable;

            insertCmd.Parameters.Add("name", NpgsqlTypes.NpgsqlDbType.Varchar).Value = spanEquipmentState.Name;

            insertCmd.Parameters.Add("spec_name", NpgsqlTypes.NpgsqlDbType.Varchar).Value = spanEquipmentState.SpecificationName;

            insertCmd.ExecuteNonQuery();
        }

        public void UpdateSpanEquipment(string schemaName, SpanEquipmentState spanEquipmentState)
        {
            using (var conn = GetConnection() as NpgsqlConnection)
            {
                conn.Open();
                using (var updateCmd = new NpgsqlCommand($"UPDATE {schemaName}.span_equipment SET outer_diameter = @outer_diameter,  name = @name, spec_name = @spec_name WHERE id = @id", conn))
                {
                    updateCmd.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid).Value = spanEquipmentState.Id;

                    updateCmd.Parameters.Add("name", NpgsqlTypes.NpgsqlDbType.Varchar).Value = spanEquipmentState.Name;

                    updateCmd.Parameters.Add("spec_name", NpgsqlTypes.NpgsqlDbType.Varchar).Value = spanEquipmentState.SpecificationName;

                    updateCmd.Parameters.Add("outer_diameter", NpgsqlTypes.NpgsqlDbType.Integer).Value = spanEquipmentState.OuterDiameter;

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

                using (var writer = conn.BeginBinaryImport($"copy {schemaName}.span_equipment (id, interest_id, outer_diameter, is_cable, name, spec_name) from STDIN (FORMAT BINARY)"))
                {
                    foreach (var spanEquipment in state.SpanEquipmentStates)
                    {
              
                        writer.WriteRow(spanEquipment.Id, spanEquipment.WalkOfInterestId, spanEquipment.OuterDiameter, spanEquipment.IsCable, spanEquipment.Name, spanEquipment.SpecificationName);
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
            string createTableCmdText = $"CREATE TABLE IF NOT EXISTS {schemaName}.service_termination (id uuid, route_node_id uuid, name character varying(255), PRIMARY KEY(id));";
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

            insertCmd.CommandText = $"INSERT INTO {schemaName}.service_termination (id, route_node_id, name) VALUES (@id, @route_node_id, @name)";

            var idParam = insertCmd.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid);
            var routeNodeIdParam = insertCmd.Parameters.Add("route_node_id", NpgsqlTypes.NpgsqlDbType.Uuid);
            var nameParam = insertCmd.Parameters.Add("name", NpgsqlTypes.NpgsqlDbType.Varchar);

            idParam.Value = serviceTerminationState.Id;
            routeNodeIdParam.Value = serviceTerminationState.RouteNodeId;
            nameParam.Value = serviceTerminationState.Name;

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

                using (var writer = conn.BeginBinaryImport($"copy {schemaName}.service_termination (id, route_node_id, name) from STDIN (FORMAT BINARY)"))
                {
                    foreach (var serviceTermination in state.ServiceTerminationStates)
                    {
                        writer.WriteRow(serviceTermination.Id, serviceTermination.RouteNodeId, serviceTermination.Name);
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
                    throw (ex);
            }
        }


        #endregion


        #region Views used for GIS map visualisation

        public void CreateRouteSegmentLabelView(string schemaName, IDbTransaction transaction = null)
        {
            // Create view
            string createViewCmdText = @"
                CREATE OR REPLACE VIEW " + schemaName + @".route_segment_label AS 
                select 
                    mrid,
	                coord,
	                (
	                select 
	                   string_agg(
						  case 
						    when outer_diameter = 0 then cast(n_conduit as text) || ' stk kabel'
						    else cast(n_conduit as text) || ' stk Ø' || cast(outer_diameter as text)
						  end
					  ,', ')
	                from
	                (
		                select i2r.route_network_element_id, outer_diameter, count(*) as n_conduit
		                from utility_network.rel_interest_to_route_element i2r
		                inner join utility_network.span_equipment on span_equipment.interest_id = i2r.interest_id
		                group by i2r.route_network_element_id, span_equipment.outer_diameter
		                order by i2r.route_network_element_id, span_equipment.outer_diameter
	                ) coduit_label 
	                where 
	                  route_network_element_id = mrid
	                ) as label
                from
                  route_network.route_segment
                where exists (
	                select null from utility_network.rel_interest_to_route_element i2r2 where i2r2.route_network_element_id = route_segment.mrid
                )";

            RunDbCommand(transaction, createViewCmdText);
        }

        public void CreateRouteNodeView(string schemaName, IDbTransaction transaction = null)
        {
            // Create view
            string createViewCmdText = @"
                CREATE OR REPLACE VIEW " + schemaName + @".route_node AS 
                    select 
		                    mrid, 
	                    ST_AsGeoJSON(ST_Transform(coord,4326)) as coord, 
	                    case 
	                      when inst.id is not null then 'SDU'
                          when slack.id IS NOT NULL AND (route_node.routenode_kind is null or route_node.routenode_kind not in ('HandHole','CabinetSmall','CabinetBig')) then 'ConduitSlack'
						  when node_container.id IS NOT NULL AND node_container.spec_category in ('SpliceClosure') then 'SpliceClosure'
	                      else routenode_kind
	                    end as kind,
	                    routenode_function as function, 
	                    case 
	                      when inst.id is not null then inst.name
                          when slack.id IS NOT NULL AND (route_node.routenode_kind is null or route_node.routenode_kind not in ('HandHole','CabinetSmall','CabinetBig')) then cast(cast(slack.number_of_ends as character varying) || ' stk' as character varying(255))
	                      else naming_name
	                    end as name, 
	                    mapping_method as method,
     				    case
							WHEN work_task.status IS NULL THEN 'InService'::text
							WHEN work_task.status::text = 'Udført'::text THEN 'InService'::text
							ELSE 'Planned'::text
						end as state
                      from 
	                    route_network.route_node 
                      left outer join
                        utility_network.service_termination inst on inst.route_node_id = route_node.mrid
                      left outer join
                        utility_network.conduit_slack slack on slack.route_node_id = route_node.mrid
   			     	  left outer join
					    utility_network.work_task ON work_task.id = route_node.work_task_mrid				
				      left outer join
					    utility_network.node_container ON node_container.route_node_id = route_node.mrid
                      where
	                    coord is not null and
	                    marked_to_be_deleted = false
                      order by mrid;     
                ";

            RunDbCommand(transaction, createViewCmdText);
        }

        public void CreateRouteSegmentView(string schemaName, IDbTransaction transaction = null)
        {
            // Create view
            string createViewCmdText = @"
               CREATE OR REPLACE VIEW " + schemaName + @".route_segment AS 
                 select 
	                route_segment.mrid, 
	                ST_AsGeoJSON(ST_Transform(route_segment.coord,4326)) as coord, 
	                routesegment_kind as kind, 
	                mapping_method as method,
	                slabel.label as name,
					case
							WHEN work_task.status IS NULL THEN 'InService'::text
							WHEN work_task.status::text = 'Udført'::text THEN 'InService'::text
							ELSE 'Planned'::text
					end as state
                  from 
	                route_network.route_segment 
                  left outer join
                    utility_network.route_segment_label slabel on slabel.mrid = route_segment.mrid
  				  left outer join
					utility_network.work_task ON work_task.id = route_segment.work_task_mrid				
                  where
	                route_segment.coord is not null and
	                route_segment.marked_to_be_deleted = false
                  order by
	                route_segment.mrid
                ";

            RunDbCommand(transaction, createViewCmdText);
        }

        public void CreateRouteNodeWithTaskInfoView(string schemaName, IDbTransaction transaction = null)
        {
            // Create view
            string createViewCmdText = @"
                CREATE OR REPLACE VIEW " + schemaName + @".route_node_with_work_task_info
                AS
                SELECT route_node.mrid,
                    route_node.coord,
                    route_node.marked_to_be_deleted,
                    route_node.delete_me,
                    route_node.work_task_mrid,
                    route_node.user_name,
                    route_node.application_name,
                    route_node.application_info,
                    route_node.lifecycle_deployment_state,
                    route_node.lifecycle_installation_date,
                    route_node.lifecycle_removal_date,
                    route_node.mapping_method,
                    route_node.mapping_vertical_accuracy,
                    route_node.mapping_horizontal_accuracy,
                    route_node.mapping_source_info,
                    route_node.mapping_survey_date,
                    route_node.safety_classification,
                    route_node.safety_remark,
                    route_node.routenode_kind,
                    route_node.routenode_function,
                    route_node.naming_name,
                    route_node.naming_description,
                    route_node.lifecycle_documentation_state,
                        CASE
                            WHEN work_task.status IS NULL THEN 'InService'::text
                            WHEN work_task.status::text = 'Udført'::text THEN 'InService'::text
                            ELSE 'Planned'::text
                        END AS work_task_deployment_state,
		                 work_task.number as work_task_number,
		                 user_edit_history.created_username,
		                 timezone('Europe/Copenhagen', user_edit_history.created_timestamp) as created_timestamp,
		                 user_edit_history.edited_username,
		                 timezone('Europe/Copenhagen', user_edit_history.edited_timestamp) as edited_timestamp
                   FROM route_network.route_node
                     LEFT JOIN utility_network.work_task ON work_task.id = route_node.work_task_mrid
	                 LEFT JOIN route_network.user_edit_history ON user_edit_history.route_network_element_id = route_node.mrid;
            ";

            RunDbCommand(transaction, createViewCmdText);

            // Create insert trigger function
            string insertTriggerfunctionCmdText = @"
                CREATE OR REPLACE FUNCTION " + schemaName + @".route_node_with_work_task_info_on_insert()
                RETURNS trigger
                LANGUAGE 'plpgsql'
                COST 100
                VOLATILE NOT LEAKPROOF
                AS $BODY$	
                  BEGIN	
                      INSERT INTO route_network.route_node (coord, marked_to_be_deleted, delete_me, work_task_mrid, user_name, application_name, application_info, lifecycle_deployment_state, lifecycle_installation_date, lifecycle_removal_date, mapping_method, mapping_vertical_accuracy, mapping_horizontal_accuracy, mapping_source_info, mapping_survey_date, safety_classification, safety_remark, routenode_kind, routenode_function, naming_name, naming_description) VALUES 
                        ( NEW.coord
                        , NEW.marked_to_be_deleted
                        , NEW.delete_me
                        , NEW.work_task_mrid
                        , NEW.user_name
                        , NEW.application_name
                        , NEW.application_info
                        , NEW.lifecycle_deployment_state
                        , NEW.lifecycle_installation_date
                        , NEW.lifecycle_removal_date
                        , NEW.mapping_method
                        , NEW.mapping_vertical_accuracy
                        , NEW.mapping_horizontal_accuracy
                        , NEW.mapping_source_info
                        , NEW.mapping_survey_date
                        , NEW.safety_classification
                        , NEW.safety_remark
		                , NEW.routenode_kind
                        , NEW.routenode_function
                        , NEW.naming_name
                        , NEW.naming_description
                        );
                      RETURN NEW;
                  END $BODY$;
             ";

            RunDbCommand(transaction, insertTriggerfunctionCmdText);


            // Create insert trigger
            string insertTriggerCmdText = @"
                 CREATE TRIGGER route_node_with_work_task_info_on_insert_trigger
                 INSTEAD OF INSERT
                 ON utility_network.route_node_with_work_task_info
                 FOR EACH ROW
                 EXECUTE PROCEDURE utility_network.route_node_with_work_task_info_on_insert();
            ";

            RunDbCommandIfNotExists(transaction, insertTriggerCmdText);


            // Create update trigger function
            string updateTriggerfunctionCmdText = @"
                CREATE OR REPLACE FUNCTION " + schemaName + @".route_node_with_work_task_info_on_update()
                RETURNS trigger
                LANGUAGE 'plpgsql'
                COST 100
                VOLATILE NOT LEAKPROOF
                AS $BODY$	
                  BEGIN	
                      UPDATE route_network.route_node SET
	                    coord = NEW.coord
                        , marked_to_be_deleted = NEW.marked_to_be_deleted
                        , delete_me = NEW.delete_me
                        , user_name = NEW.user_name
                        , application_name = NEW.application_name
                        , application_info = NEW.application_info
                        , lifecycle_deployment_state = NEW.lifecycle_deployment_state
                        , lifecycle_installation_date = NEW.lifecycle_installation_date
                        , lifecycle_removal_date = NEW.lifecycle_removal_date
                        , mapping_method = NEW.mapping_method
                        , mapping_vertical_accuracy = NEW.mapping_vertical_accuracy
                        , mapping_horizontal_accuracy = NEW.mapping_horizontal_accuracy
                        , mapping_source_info = NEW.mapping_source_info
                        , mapping_survey_date = NEW.mapping_survey_date
                        , safety_classification = NEW.safety_classification
                        , safety_remark = NEW.safety_remark
		                , routenode_kind = NEW.routenode_kind
                        , routenode_function = NEW.routenode_function
                        , naming_name = NEW.naming_name
                        , naming_description = NEW.naming_description
                       WHERE mrid = OLD.mrid
		                ;
                      RETURN NEW;
                  END $BODY$;
             ";

            RunDbCommand(transaction, updateTriggerfunctionCmdText);


            // Create update trigger
            string updateTriggerCmdText = @"
                CREATE TRIGGER route_node_with_work_task_info_on_update_trigger
                INSTEAD OF UPDATE 
                ON utility_network.route_node_with_work_task_info
                FOR EACH ROW
                EXECUTE PROCEDURE utility_network.route_node_with_work_task_info_on_update();
            ";

            RunDbCommandIfNotExists(transaction, updateTriggerCmdText);
        }


        public void CreateRouteSegmentWithTaskInfoView(string schemaName, IDbTransaction transaction = null)
        {
            // Create view
            string createViewCmdText = @"
             CREATE OR REPLACE VIEW " + schemaName + @".route_segment_with_work_task_info
                 AS
               SELECT route_segment.mrid,
                    route_segment.coord,
                    route_segment.marked_to_be_deleted,
                    route_segment.delete_me,
                    route_segment.work_task_mrid,
                    route_segment.user_name,
                    route_segment.application_name,
                    route_segment.application_info,
                    route_segment.lifecycle_deployment_state,
                    route_segment.lifecycle_installation_date,
                    route_segment.lifecycle_removal_date,
                    route_segment.mapping_method,
                    route_segment.mapping_vertical_accuracy,
                    route_segment.mapping_horizontal_accuracy,
                    route_segment.mapping_source_info,
                    route_segment.mapping_survey_date,
                    route_segment.safety_classification,
                    route_segment.safety_remark,
                    route_segment.routesegment_kind,
                    route_segment.routesegment_width,
                    route_segment.routesegment_height,
                    route_segment.naming_name,
                    route_segment.naming_description,
                    route_segment.lifecycle_documentation_state,
                        CASE
                            WHEN work_task.status IS NULL THEN 'InService'::text
                            WHEN work_task.status::text = 'Udført'::text THEN 'InService'::text
                            ELSE 'Planned'::text
                        END AS work_task_deployment_state,
					 work_task.number as work_task_number,
		             user_edit_history.created_username,
		             timezone('Europe/Copenhagen', user_edit_history.created_timestamp) as created_timestamp,
		             user_edit_history.edited_username,
		             timezone('Europe/Copenhagen', user_edit_history.edited_timestamp) as edited_timestamp
                   FROM route_network.route_segment
                     LEFT JOIN utility_network.work_task ON work_task.id = route_segment.work_task_mrid
                     LEFT JOIN route_network.user_edit_history ON user_edit_history.route_network_element_id = route_segment.mrid;
             ";

            RunDbCommand(transaction, createViewCmdText);

            // Create insert trigger function
            string insertTriggerfunctionCmdText = @"
                CREATE OR REPLACE FUNCTION " + schemaName + @".route_segment_with_work_task_info_on_insert()
                RETURNS trigger
                LANGUAGE 'plpgsql'
                COST 100
                VOLATILE NOT LEAKPROOF
                AS $BODY$	
                  BEGIN	
                      INSERT INTO route_network.route_segment (coord, marked_to_be_deleted, delete_me, work_task_mrid, user_name, application_name, application_info, lifecycle_deployment_state, lifecycle_installation_date, lifecycle_removal_date, mapping_method, mapping_vertical_accuracy, mapping_horizontal_accuracy, mapping_source_info, mapping_survey_date, safety_classification, safety_remark, routesegment_kind, routesegment_width, routesegment_height, naming_name, naming_description) VALUES 
                        ( NEW.coord
                        , NEW.marked_to_be_deleted
                        , NEW.delete_me
                        , NEW.work_task_mrid
                        , NEW.user_name
                        , NEW.application_name
                        , NEW.application_info
                        , NEW.lifecycle_deployment_state
                        , NEW.lifecycle_installation_date
                        , NEW.lifecycle_removal_date
                        , NEW.mapping_method
                        , NEW.mapping_vertical_accuracy
                        , NEW.mapping_horizontal_accuracy
                        , NEW.mapping_source_info
                        , NEW.mapping_survey_date
                        , NEW.safety_classification
                        , NEW.safety_remark
		                , NEW.routesegment_kind
                        , NEW.routesegment_width
                        , NEW.routesegment_height
                        , NEW.naming_name
                        , NEW.naming_description
                        );
                      RETURN NEW;
                  END $BODY$;
             ";

            RunDbCommand(transaction, insertTriggerfunctionCmdText);


            // Create insert trigger
            string insertTriggerCmdText = @"
              CREATE TRIGGER route_segment_with_work_task_info_on_insert_trigger
                 INSTEAD OF INSERT
                 ON utility_network.route_segment_with_work_task_info
                 FOR EACH ROW
                 EXECUTE PROCEDURE utility_network.route_segment_with_work_task_info_on_insert();
            ";

            RunDbCommandIfNotExists(transaction, insertTriggerCmdText);


            // Create update trigger function
            string updateTriggerfunctionCmdText = @"
             CREATE OR REPLACE FUNCTION " + schemaName + @".route_segment_with_work_task_info_on_update()
                RETURNS trigger
                LANGUAGE 'plpgsql'
                COST 100
                VOLATILE NOT LEAKPROOF
                AS $BODY$	
                  BEGIN	
                      UPDATE route_network.route_segment SET
	                    coord = NEW.coord
                        , marked_to_be_deleted = NEW.marked_to_be_deleted
                        , delete_me = NEW.delete_me
                        , user_name = NEW.user_name
                        , application_name = NEW.application_name
                        , application_info = NEW.application_info
                        , lifecycle_deployment_state = NEW.lifecycle_deployment_state
                        , lifecycle_installation_date = NEW.lifecycle_installation_date
                        , lifecycle_removal_date = NEW.lifecycle_removal_date
                        , mapping_method = NEW.mapping_method
                        , mapping_vertical_accuracy = NEW.mapping_vertical_accuracy
                        , mapping_horizontal_accuracy = NEW.mapping_horizontal_accuracy
                        , mapping_source_info = NEW.mapping_source_info
                        , mapping_survey_date = NEW.mapping_survey_date
                        , safety_classification = NEW.safety_classification
                        , safety_remark = NEW.safety_remark
		                , routesegment_kind = NEW.routesegment_kind
                        , routesegment_width = NEW.routesegment_width
                        , routesegment_height = NEW.routesegment_height
                        , naming_name = NEW.naming_name
                        , naming_description = NEW.naming_description
                       WHERE mrid = OLD.mrid
		                ;
                      RETURN NEW;
                  END $BODY$;
             ";

            RunDbCommand(transaction, updateTriggerfunctionCmdText);


            // Create update trigger
            string updateTriggerCmdText = @"
             CREATE TRIGGER route_segment_with_work_task_info_on_update_trigger
                INSTEAD OF UPDATE 
                ON utility_network.route_segment_with_work_task_info
                FOR EACH ROW
                EXECUTE PROCEDURE utility_network.route_segment_with_work_task_info_on_update();
            ";

            RunDbCommandIfNotExists(transaction, updateTriggerCmdText);
        }

        

        public void CreateServiceTerminationView(string schemaName, IDbTransaction transaction = null)
        {
            string createViewCmdText = @"
             CREATE OR REPLACE VIEW " + schemaName + @".service_termination_view
                 AS
                 SELECT inst.id,
                    route_node.coord,
                    'SDU'::text AS kind,
                    inst.name,
                    route_node.mapping_method AS method,
                    route_node.lifecycle_deployment_state,
                        CASE
                            WHEN work_task.status IS NULL THEN 'InService'::text
                            WHEN work_task.status::text = 'Udført'::text THEN 'InService'::text
                            ELSE 'Planned'::text
                        END AS work_task_deployment_state
                   FROM route_network.route_node
                     JOIN utility_network.service_termination inst ON inst.route_node_id = route_node.mrid
                     LEFT JOIN utility_network.work_task ON work_task.id = route_node.work_task_mrid
                  WHERE route_node.coord IS NOT NULL AND route_node.marked_to_be_deleted = false;
             ";

            RunDbCommand(transaction, createViewCmdText);
        }

        public void CreateStandAloneSpliceClosureView(string schemaName, IDbTransaction transaction = null)
        {
            string createViewCmdText = @"
            CREATE OR REPLACE VIEW " + schemaName + @".stand_alone_splice_closure_view
                 AS
                 SELECT container.id,
                    route_node.coord,
                    route_node.naming_name AS name,
                    container.spec_name AS kind,
                    route_node.mapping_method AS method,
                    route_node.lifecycle_deployment_state,
                        CASE
                            WHEN work_task.status IS NULL THEN 'InService'::text
                            WHEN work_task.status::text = 'Udført'::text THEN 'InService'::text
                            ELSE 'Planned'::text
                        END AS work_task_deployment_state
                   FROM route_network.route_node
                     JOIN utility_network.node_container container ON container.route_node_id = route_node.mrid
                     LEFT JOIN utility_network.work_task ON work_task.id = route_node.work_task_mrid
                  WHERE route_node.coord IS NOT NULL AND route_node.marked_to_be_deleted = false AND (container.spec_category = 'SpliceClosure');
             ";

            RunDbCommand(transaction, createViewCmdText);
        }

        #endregion
    }
}

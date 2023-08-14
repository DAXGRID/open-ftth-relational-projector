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
                using (var updateCmd = new NpgsqlCommand($"UPDATE {schemaName}.conduit_slack SET number_of_ends = @n WHERE route_node_id = @i", conn))
                {
                    updateCmd.Parameters.Add("i", NpgsqlTypes.NpgsqlDbType.Uuid).Value = state.RouteNodeId;

                    updateCmd.Parameters.Add("n", NpgsqlTypes.NpgsqlDbType.Integer).Value = state.NumberOfConduitEnds;

                    updateCmd.ExecuteNonQuery();
                }
            }
        }

        public void DeleteConduitSlack(string schemaName, Guid routeNodeId)
        {
            using (var conn = GetConnection() as NpgsqlConnection)
            {
                conn.Open();
                using (var deleteCmd = new NpgsqlCommand($"DELETE FROM {schemaName}.conduit_slack WHERE id = @i", conn))
                {
                    deleteCmd.Parameters.Add("i", NpgsqlTypes.NpgsqlDbType.Uuid).Value = routeNodeId;
                    deleteCmd.ExecuteNonQuery();
                }
            }
        }


        #endregion


       


        #region Work Task
        public void CreateWorkTaskTable(string schemaName, IDbTransaction transaction = null)
        {
            // Create table
            string createTableCmdText = $"CREATE TABLE IF NOT EXISTS {schemaName}.work_task (id uuid, status character varying(255), PRIMARY KEY(id));";
            _logger.LogDebug($"Execute SQL: {createTableCmdText}");

            RunDbCommand(transaction, createTableCmdText);
        }

        public void InsertIntoWorkTaskTable(string schemaName, WorkTaskState workTaskState)
        {
            using var conn = GetConnection() as NpgsqlConnection;

            conn.Open();

            using var insertCmd = conn.CreateCommand();

            insertCmd.CommandText = $"INSERT INTO {schemaName}.work_task (id, status) VALUES (@id, @status)";

            var idParam = insertCmd.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid);
            var statusParam = insertCmd.Parameters.Add("status", NpgsqlTypes.NpgsqlDbType.Varchar);

            idParam.Value = workTaskState.Id;
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

                using (var writer = conn.BeginBinaryImport($"copy {schemaName}.work_task (id, status) from STDIN (FORMAT BINARY)"))
                {
                    foreach (var workTask in state.WorkTaskStates)
                    {
                        writer.WriteRow(workTask.Id, workTask.Status);
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
            _logger.LogDebug($"Execute SQL: {createViewCmdText}");

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
                      where
	                    coord is not null and
	                    marked_to_be_deleted = false
                      order by mrid;
            ";

            _logger.LogDebug($"Execute SQL: {createViewCmdText}");

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
            _logger.LogDebug($"Execute SQL: {createViewCmdText}");

            RunDbCommand(transaction, createViewCmdText);
        }

        public void CreateRouteSegmentTaskStatusView(string schemaName, IDbTransaction transaction = null)
        {
            // Create view
            string createViewCmdText = @"
                CREATE OR REPLACE VIEW " + schemaName + @".route_segment_with_task_status AS 
                select 
                  *,
                  work_task.status as work_task_status
                from 
                  route_network.route_segment
                left outer join utility_network.work_task on work_task.id = route_segment.work_task_mrid
             ";

            _logger.LogDebug($"Execute SQL: {createViewCmdText}");

            RunDbCommand(transaction, createViewCmdText);
        }

        public void CreateRouteNodeTaskStatusView(string schemaName, IDbTransaction transaction = null)
        {
            // Create view
            string createViewCmdText = @"
               CREATE OR REPLACE VIEW " + schemaName + @".route_node_with_task_status
                 AS
                select 
                  *,
                  work_task.status as work_task_status
                from 
                  route_network.route_node
                left outer join utility_network.work_task on work_task.id = route_node.work_task_mrid     
             ";

            _logger.LogDebug($"Execute SQL: {createViewCmdText}");

            RunDbCommand(transaction, createViewCmdText);
        }

        #endregion
    }
}

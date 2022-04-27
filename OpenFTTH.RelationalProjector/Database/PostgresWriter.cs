using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using OpenFTTH.RelationalProjector.Settings;
using OpenFTTH.RelationalProjector.State;
using OpenFTTH.UtilityGraphService.API.Model.UtilityNetwork;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
            string createTableCmdText = $"CREATE UNLOGGED TABLE IF NOT EXISTS {schemaName}.rel_interest_to_route_element (interest_id uuid, route_network_element_id uuid, PRIMARY KEY(interest_id, route_network_element_id));";
            _logger.LogDebug($"Execute SQL: {createTableCmdText}");

            RunDbCommand(transaction, createTableCmdText);

            // Create index on route_network_element_id column
            string createIndexCmdText = $"CREATE INDEX IF NOT EXISTS idx_rel_interest_to_route_element_route_network_element_id ON {schemaName}.rel_interest_to_route_element(route_network_element_id);";
            RunDbCommand(transaction, createIndexCmdText);
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

                using (var writer = conn.BeginBinaryImport($"copy {schemaName}.rel_interest_to_route_element (route_network_element_id, interest_id) from STDIN (FORMAT BINARY)"))
                {
                    foreach (var walkOfInterestRouteElementRelations in state.WalkOfInterestToRouteElementRelations)
                    {
                        foreach (var routeElementId in walkOfInterestRouteElementRelations.Value)
                        {
                            writer.WriteRow(routeElementId, walkOfInterestRouteElementRelations.Key);
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
                using (var insertCmd = new NpgsqlCommand($"INSERT INTO {schemaName}.rel_interest_to_route_element (route_network_element_id, interest_id) VALUES (@r, @i)", conn))
                {
                    var routeNetworkElementIdparam = insertCmd.Parameters.Add("r", NpgsqlTypes.NpgsqlDbType.Uuid);
                    var interestIdparam = insertCmd.Parameters.Add("i", NpgsqlTypes.NpgsqlDbType.Uuid);

                    foreach (var guid in routeElementIds)
                    {
                        interestIdparam.Value = interestId;
                        routeNetworkElementIdparam.Value = guid;
                        insertCmd.ExecuteNonQuery();
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

        #region Conduit table
        public void CreateConduitTable(string schemaName, IDbTransaction transaction = null)
        {
            // Create table
            string createTableCmdText = $"CREATE UNLOGGED TABLE IF NOT EXISTS {schemaName}.conduit (id uuid, interest_id uuid, outer_diameter integer, PRIMARY KEY(id));";
            _logger.LogDebug($"Execute SQL: {createTableCmdText}");
            
            RunDbCommand(transaction, createTableCmdText);

            // Create index on interest_id column
           string createIndexCmdText = $"CREATE INDEX IF NOT EXISTS idx_conduit_interest_id ON {schemaName}.conduit(interest_id);";
           RunDbCommand(transaction, createIndexCmdText);
        }

        public void InsertSpanEquipmentIntoConduitTable(string schemaName, Guid id, Guid interestId, int outerDiameter)
        {
            using var conn = GetConnection() as NpgsqlConnection;

            conn.Open();

            using var insertCmd = conn.CreateCommand();

            insertCmd.CommandText = $"INSERT INTO {schemaName}.conduit (id, interest_id, outer_diameter) VALUES (@id, @interest_id, @outer_diameter)";

            var idParam = insertCmd.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid);
            var interestIdparam = insertCmd.Parameters.Add("interest_id", NpgsqlTypes.NpgsqlDbType.Uuid);
            var outerDiameterParam = insertCmd.Parameters.Add("outer_diameter", NpgsqlTypes.NpgsqlDbType.Integer);

            idParam.Value = id;
            interestIdparam.Value = interestId;
            outerDiameterParam.Value = outerDiameter;

            insertCmd.ExecuteNonQuery();
        }

        public void DeleteSpanEquipmentFromConduitTable(string schemaName, Guid spanEquipmentId)
        {
            using (var conn = GetConnection() as NpgsqlConnection)
            {
                conn.Open();
                using (var deleteCmd = new NpgsqlCommand($"DELETE FROM {schemaName}.conduit WHERE id = @i", conn))
                {
                    var idparam = deleteCmd.Parameters.Add("i", NpgsqlTypes.NpgsqlDbType.Uuid);
                    idparam.Value = spanEquipmentId;
                    deleteCmd.ExecuteNonQuery();
                }
            }
        }

        public void BulkCopyIntoConduitTable(string schemaName, ProjektorState state)
        {
            using (var conn = GetConnection() as NpgsqlConnection)
            {
                conn.Open();

                // Truncate the table
                using (var truncateCmd = new NpgsqlCommand($"truncate table {schemaName}.conduit", conn))
                {
                    truncateCmd.ExecuteNonQuery();
                }

                using (var writer = conn.BeginBinaryImport($"copy {schemaName}.conduit (id, interest_id, outer_diameter) from STDIN (FORMAT BINARY)"))
                {
                    foreach (var spanEquipment in state.SpanEquipmentStates.Where(s => !s.IsCable))
                    {
                        var spanEquipmentSpecification = state.GetSpanEquipmentSpecification(spanEquipment.SpecificationId);
                        var spanStructureSpecification = state.GetSpanStructureSpecification(spanEquipmentSpecification.RootTemplate.SpanStructureSpecificationId);

                        writer.WriteRow(spanEquipment.Id, spanEquipment.WalkOfInterestId, spanStructureSpecification.OuterDiameter.Value);
                    }

                    writer.Complete();
                }
            }

        }


        public void UpdateSpanEquipmentDiameterInConduitTable(string schemaName, Guid spanEquipmentId, int diameter)
        {
            using (var conn = GetConnection() as NpgsqlConnection)
            {
                conn.Open();
                using (var updateCmd = new NpgsqlCommand($"UPDATE {schemaName}.conduit SET outer_diameter = @d WHERE id = @i", conn))
                {
                    var idParam = updateCmd.Parameters.Add("i", NpgsqlTypes.NpgsqlDbType.Uuid);
                    idParam.Value = spanEquipmentId;

                    var diameterParam = updateCmd.Parameters.Add("d", NpgsqlTypes.NpgsqlDbType.Integer);
                    diameterParam.Value = diameter;

                    updateCmd.ExecuteNonQuery();
                }
            }
        }

      

        #endregion

        #region Service Termination Point
        public void CreateServiceTerminationTable(string schemaName, IDbTransaction transaction = null)
        {
            // Create table
            string createTableCmdText = $"CREATE UNLOGGED TABLE IF NOT EXISTS {schemaName}.service_termination (id uuid, route_node_id uuid, name character varying(255), PRIMARY KEY(id));";
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
	                  string_agg(cast(n_conduit as text) || ' stk Ø' || cast(outer_diameter as text),', ')
	                from
	                (
		                select i2r.route_network_element_id, outer_diameter, count(*) as n_conduit
		                from utility_network.rel_interest_to_route_element i2r
		                inner join utility_network.conduit on conduit.interest_id = i2r.interest_id
		                group by i2r.route_network_element_id, conduit.outer_diameter
		                order by i2r.route_network_element_id, conduit.outer_diameter
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
	                      else routenode_kind
	                    end as kind,
	                    routenode_function as function, 
	                    case 
	                      when inst.id is not null then inst.name
	                      else naming_name
	                    end as name, 
	                    mapping_method as method,
	                    lifecycle_deployment_state
                      from 
	                    route_network.route_node 
                      left outer join
                        utility_network.service_termination inst on inst.route_node_id = route_node.mrid
                      where
	                    coord is not null and
	                    marked_to_be_deleted = false
                      order by mrid
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
	                lifecycle_deployment_state,
	                slabel.label as name
                  from 
	                route_network.route_segment 
                  left outer join
                    utility_network.route_segment_label slabel on slabel.mrid = route_segment.mrid
                  where
	                route_segment.coord is not null and
	                route_segment.marked_to_be_deleted = false
                  order by
	                route_segment.mrid
                ";
            _logger.LogDebug($"Execute SQL: {createViewCmdText}");

            RunDbCommand(transaction, createViewCmdText);
        }
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.standard;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.JdbcCommon;
import org.apache.nifi.util.LongHolder;
import org.apache.nifi.util.StopWatch;

@EventDriven
@Tags({ "sql", "select", "jdbc", "query", "database" })
@CapabilityDescription("Execute provided SQL select query. Query result will be converted to Avro format."
    + " Streaming is used so arbitrarily large result sets are supported.")
public class ExecuteSQL extends AbstractProcessor {

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Successfully created FlowFile from SQL query result set.")
        .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("SQL query execution failed. Incoming FlowFile will be penalized and routed to this relationship")
        .build();
    private final Set<Relationship> relationships;

    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
        .name("Database Connection Pooling Service")
        .description("The Controller Service that is used to obtain connection to database")
        .required(true)
        .identifiesControllerService(DBCPService.class)
        .build();

    public static final PropertyDescriptor SQL_SELECT_QUERY = new PropertyDescriptor.Builder()
        .name("SQL select query")
        .description("SQL select query")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();

    public static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
        .name("Max Wait Time")
        .description("The maximum amount of time allowed for a running SQL select query "
            + " , zero means there is no limit. Max time less than 1 second will be equal to zero.")
        .defaultValue("0 seconds")
        .required(true)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .sensitive(false)
        .build();

    private final List<PropertyDescriptor> propDescriptors;

    public ExecuteSQL() {
        final Set<Relationship> r = new HashSet<>();
        r.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(r);

        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(DBCP_SERVICE);
        pds.add(SQL_SELECT_QUERY);
        pds.add(QUERY_TIMEOUT);
        propDescriptors = Collections.unmodifiableList(pds);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propDescriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile incoming = session.get();
        if (incoming == null) {
            return;
        }

        final ProcessorLog logger = getLogger();

        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        final String selectQuery = context.getProperty(SQL_SELECT_QUERY).evaluateAttributeExpressions(incoming).getValue();
        final Integer queryTimeout = context.getProperty(QUERY_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();

        final StopWatch stopWatch = new StopWatch(true);

        try (final Connection con = dbcpService.getConnection();
            final Statement st = con.createStatement()) {
            st.setQueryTimeout(queryTimeout); // timeout in seconds
            final LongHolder nrOfRows = new LongHolder(0L);
            final FlowFile outgoing = session.write(incoming, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    try {
                        logger.debug("Executing query {}", new Object[] { selectQuery });
                        final ResultSet resultSet = st.executeQuery(selectQuery);
                        nrOfRows.set(JdbcCommon.convertToAvroStream(resultSet, out));
                    } catch (final SQLException e) {
                        throw new ProcessException(e);
                    }
                }
            });

            logger.info("{} contains {} Avro records", new Object[] { nrOfRows.get() });
            logger.info("Transferred {} to 'success'", new Object[] { outgoing });
            session.getProvenanceReporter().modifyContent(outgoing, "Retrieved " + nrOfRows.get() + " rows", stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(outgoing, REL_SUCCESS);
        } catch (final ProcessException | SQLException e) {
            logger.error("Unable to execute SQL select query {} for {} due to {}; routing to failure", new Object[] { selectQuery, incoming, e });
            session.transfer(incoming, REL_FAILURE);
        }
    }
}

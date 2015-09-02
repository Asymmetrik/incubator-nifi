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

import com.datasift.client.DataSiftClient;
import com.datasift.client.DataSiftConfig;
import com.datasift.client.core.Stream;
import com.datasift.client.stream.*;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

@SupportsBatching
@Tags({"get", "fetch", "datasift", "ingest", "source", "input"})
@CapabilityDescription("Fetches a stream from DataSift")
//@WritesAttributes({
//        @WritesAttribute(attribute = "filename", description = "The filename is set to the name of the file on the remote server"),
//        @WritesAttribute(attribute = "mime.type", description = "The MIME Type of the FlowFile, as reported by the HTTP Content-Type header")
//})
public class GetDataSift extends AbstractProcessor {

    public static final PropertyDescriptor WEB_SERVICE_URL = new PropertyDescriptor.Builder()
            .name("Web Service URL")
            .description("The URL to pull from")
            .required(false)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("https?\\://.*")))
            .build();
    public static final PropertyDescriptor RULES_URL = new PropertyDescriptor.Builder()
            .name("Rules URL")
            .description("The URL to pull from")
            .required(false)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("https?\\://.*")))
            .build();
    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("Query")
            .description("DataSift query")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username required to access the URL")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor API_KEY = new PropertyDescriptor.Builder()
            .name("API Key")
            .description("API Key required to access the URL")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All files are transferred to the success relationship")
            .build();

    public static final Relationship REL_DELETE = new Relationship.Builder()
            .name("delete")
            .description("delete requests are transferred to this relationship")
            .build();

    public static final Relationship REL_ERROR = new Relationship.Builder()
            .name("error")
            .description("api errors are transferred to this relationship")
            .build();

    private final BlockingQueue<JsonNode> messages = new LinkedBlockingQueue<>();
    private DataSiftClient datasift;
    private Stream stream;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_DELETE);
        relationships.add(REL_ERROR);

        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(WEB_SERVICE_URL);
        properties.add(RULES_URL);
        properties.add(QUERY);
        properties.add(USERNAME);
        properties.add(API_KEY);
        this.properties = Collections.unmodifiableList(properties);

    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        // get the URL
        final String url = context.getProperty(WEB_SERVICE_URL).isSet() ? context.getProperty(WEB_SERVICE_URL).getValue() : null;

        final String username = context.getProperty(USERNAME).getValue();
        final String password = context.getProperty(API_KEY).getValue();
        final String query = context.getProperty(QUERY).getValue();

        DataSiftConfig config = new DataSiftConfig(username, password);

        if(url != null) config.wsHost(url);

        this.datasift = new DataSiftClient(config);
        stream = datasift.compile(query).sync();

        callDataSift();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }


    @OnUnscheduled
    @OnStopped
    public void onStopped() {
        datasift.liveStream().unsubscribe(stream);
    }


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {


        final JsonNode jsonNode = messages.poll();

        FlowFile flowFile = session.create();
        flowFile = session.putAttribute(flowFile, "datasift_hash", stream.hash());
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(final OutputStream out) throws IOException {
                if(jsonNode != null) {
                    out.write(jsonNode.toString().getBytes());
                }
            }
        });
        session.transfer(flowFile, REL_SUCCESS);
        session.commit();
    }

    private void callDataSift() {

//        String csdl = "bitly.url regex_partial \"youtube.*\"";

        // TODO: Update me later

        try {

            datasift.liveStream().onError(new ErrorListener() {
                @Override
                public void exceptionCaught(Throwable throwable) {
                    getLogger().error("data sift error", throwable);
                }
            });
            datasift.liveStream().onStreamEvent(new StreamEventListener() {
                @Override
                public void onDelete(DeletedInteraction deletedInteraction) {
                    //todo - route in nifi
                    getLogger().info("on-delete::{}", new Object[]{deletedInteraction});
                }
            });

            // Subscribe to the stream
            datasift.liveStream().subscribe(new StreamSubscription(stream) {
                @Override
                public void onDataSiftLogMessage(DataSiftMessage di) {
                    getLogger().warn((di.isError() ? "Error" : di.isInfo() ? "Info" : "Warning") + ":\n" + di);
                }

                @Override
                public void onMessage(Interaction interaction) {
                    if(interaction.getData() != null) {
                        messages.add(interaction.getData());
                    }
                }
            });

        } catch (Exception ex) {
            // TODO: Your exception handling here
        }

        if (getLogger().isDebugEnabled()) {
            getLogger().debug("DataSift Stream Reading Thread successfully launched");
        }

    }

}


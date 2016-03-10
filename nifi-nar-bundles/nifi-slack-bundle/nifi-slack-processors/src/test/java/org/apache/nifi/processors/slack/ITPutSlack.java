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
package org.apache.nifi.processors.slack;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


public class ITPutSlack {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(PutSlack.class);
    }

    @Test
    public void testSimplePut() {
        testRunner.setProperty(PutSlack.WEBHOOK_URL, PutSlackTest.WEBHOOK_TEST_URL);
        testRunner.setProperty(PutSlack.WEBHOOK_TEXT, PutSlackTest.WEBHOOK_TEST_TEXT);

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);
        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_SUCCESS, 1);
    }

    @Test
    public void testSimplePutWithAttributes() {
        testRunner.setProperty(PutSlack.WEBHOOK_URL, PutSlackTest.WEBHOOK_TEST_URL);
        testRunner.setProperty(PutSlack.WEBHOOK_TEXT, PutSlackTest.WEBHOOK_TEST_TEXT);
        testRunner.setProperty(PutSlack.CHANNEL, "#test-attributes");
        testRunner.setProperty(PutSlack.USERNAME, "integration-test-webhook");
        testRunner.setProperty(PutSlack.ICON_EMOJI, ":smile:");

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);
        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_SUCCESS, 1);
    }

    @Test
    public void testSimplePutWithAttributesIconURL() {
        testRunner.setProperty(PutSlack.WEBHOOK_URL, PutSlackTest.WEBHOOK_TEST_URL);
        testRunner.setProperty(PutSlack.WEBHOOK_TEXT, PutSlackTest.WEBHOOK_TEST_TEXT);
        testRunner.setProperty(PutSlack.CHANNEL, "#test-attributes-url");
        testRunner.setProperty(PutSlack.USERNAME, "integration-test-webhook");
        testRunner.setProperty(PutSlack.ICON_URL, "http://lorempixel.com/48/48/");

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);
        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_SUCCESS, 1);
    }
}

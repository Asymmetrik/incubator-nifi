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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class PutSlackTest {

    private TestRunner testRunner;
    public static String WEBHOOK_TEST_URL = "https://hooks.slack.com/services/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";
    public static String WEBHOOK_TEST_TEXT = "Hello From Apache NiFi";

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(PutSlack.class);
    }

    @Test(expected = AssertionError.class)
    public void testBlankText() {
        testRunner.setProperty(PutSlack.WEBHOOK_URL, WEBHOOK_TEST_URL);
        testRunner.setProperty(PutSlack.WEBHOOK_TEXT, "");

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);
    }

    @Test(expected = AssertionError.class)
    public void testBlankTextViaExpression() {
        testRunner.setProperty(PutSlack.WEBHOOK_URL, WEBHOOK_TEST_URL);
        testRunner.setProperty(PutSlack.WEBHOOK_TEXT, "${invalid-attr}"); // Create a blank webhook text

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);
    }

    @Test(expected = AssertionError.class)
    public void testInvalidChannel() {
        testRunner.setProperty(PutSlack.WEBHOOK_URL, WEBHOOK_TEST_URL);
        testRunner.setProperty(PutSlack.WEBHOOK_TEXT, WEBHOOK_TEST_TEXT);
        testRunner.setProperty(PutSlack.CHANNEL, "invalid");

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);
    }

    @Test(expected = AssertionError.class)
    public void testInvalidIconUrl() {
        testRunner.setProperty(PutSlack.WEBHOOK_URL, WEBHOOK_TEST_URL);
        testRunner.setProperty(PutSlack.WEBHOOK_TEXT, WEBHOOK_TEST_TEXT);
        testRunner.setProperty(PutSlack.ICON_URL, "invalid");

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);
    }

    @Test(expected = AssertionError.class)
    public void testInvalidIconEmoji() {
        testRunner.setProperty(PutSlack.WEBHOOK_URL, WEBHOOK_TEST_URL);
        testRunner.setProperty(PutSlack.WEBHOOK_TEXT, WEBHOOK_TEST_TEXT);
        testRunner.setProperty(PutSlack.ICON_EMOJI, "invalid");

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);
    }

    @Test
    public void testGetPropertyDescriptors() throws Exception {
        PutSlack processor = new PutSlack();
        List<PropertyDescriptor> pd = processor.getSupportedPropertyDescriptors();
        assertEquals("size should be eq", 6, pd.size());
        assertTrue(pd.contains(PutSlack.WEBHOOK_TEXT));
        assertTrue(pd.contains(PutSlack.WEBHOOK_URL));
        assertTrue(pd.contains(PutSlack.CHANNEL));
        assertTrue(pd.contains(PutSlack.USERNAME));
        assertTrue(pd.contains(PutSlack.ICON_URL));
        assertTrue(pd.contains(PutSlack.ICON_EMOJI));
    }
}

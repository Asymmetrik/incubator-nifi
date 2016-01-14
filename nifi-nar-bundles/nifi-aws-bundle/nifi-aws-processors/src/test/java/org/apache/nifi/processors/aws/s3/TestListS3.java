package org.apache.nifi.processors.aws.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Ignore;
import org.junit.Test;

import com.amazonaws.regions.Regions;

@Ignore("For local testing only - interacts with S3 so the credentials file must be configured and all necessary buckets created")
public class TestListS3 {

    private static final String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";
    private static final String BUCKET = "wildfire-data";
    private static final String BASE_PREFIX = "sandbox/twitter/";
    private static final String REGION = Regions.US_EAST_1.getName();
    
    private static DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("yyyy/MM/dd").withZoneUTC();
    private static DateTime NOW = new DateTime(DateTimeZone.UTC);
    
    private TestRunner buildTestRunner() {
        TestRunner runner = TestRunners.newTestRunner(new ListS3());
        runner.setProperty(ListS3.BUCKET, BUCKET);
        runner.setProperty(ListS3.PREFIXES, BASE_PREFIX + FORMATTER.print(NOW));
        runner.setProperty(ListS3.REGION, REGION);
        runner.setProperty(ListS3.CREDENTIALS_FILE, CREDENTIALS_FILE);
        return runner;
    }
    
    private void assertFlowfiles(List<MockFlowFile> flowFiles) {
        System.out.println("Flow files: " + flowFiles.size());
        for (MockFlowFile f : flowFiles) {
            assertNotNull(f.getAttribute("s3.bucket"));
            assertNotNull(f.getAttribute(CoreAttributes.FILENAME.key()));
            assertNotNull(f.getAttribute("s3.owner"));
            assertNotNull(f.getAttribute("s3.lastModified"));
            assertNotNull(f.getAttribute("s3.length"));
            assertNotNull(f.getAttribute("s3.storageClass"));
            
            System.out.println(f.getAttribute("s3.bucket") + ":" + f.getAttribute(CoreAttributes.FILENAME.key())
            + ":" + f.getAttribute("s3.owner") + ":" + f.getAttribute("s3.lastModified")
            + ":" + f.getAttribute("s3.length")  + ":" + f.getAttribute("s3.storageClass"));
        }
    }
    
    @Test
    public void testList() throws IOException {
        TestRunner runner = buildTestRunner();

        runner.enqueue(new byte[0]);
        runner.run(1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        assertFalse(flowFiles.isEmpty());
        assertFlowfiles(flowFiles);
    }
    
    @Test
    public void testListWithMax() throws IOException {
        TestRunner runner = buildTestRunner();
        runner.setProperty(ListS3.MAX_OBJECTS, "3");

        runner.enqueue(new byte[0]);
        runner.run(1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        assertEquals(3, flowFiles.size());
    }
    
    @Test
    public void testListDateRange() throws IOException {
        TestRunner runner = buildTestRunner();
        runner.setProperty(ListS3.MIN_AGE, "4 hr");
        runner.setProperty(ListS3.MAX_AGE, "5 hr");

        runner.enqueue(new byte[0]);
        runner.run(1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        assertFalse(flowFiles.isEmpty());
    }
    
    @Test
    public void testInvalidDateRange() throws IOException {
        TestRunner runner = buildTestRunner();
        runner.setProperty(ListS3.MIN_AGE, "4 hr");
        runner.setProperty(ListS3.MAX_AGE, "1 hr");
        
        Collection<ValidationResult> results = new HashSet<>();
        runner.enqueue(new byte[0]);
        ProcessContext pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            assertTrue(vr.toString().contains("is invalid because Minimum Age cannot be greater than Maximum Age"));
        }
    }
    
    @Test
    public void testListEmpty() throws IOException {
        TestRunner runner = buildTestRunner();
        runner.setProperty(ListS3.PREFIXES, "foo");
        
        runner.enqueue(new byte[0]);
        runner.run(1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        assertTrue(flowFiles.isEmpty());
    }
    
    @Test
    public void testAlphabeticalOrder() throws IOException {
        TestRunner runner = buildTestRunner();
        runner.setProperty(ListS3.MAX_OBJECTS, "3");

        runner.enqueue(new byte[0]);
        runner.run(1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        for (int i=1; i < flowFiles.size(); i++) {
            assertTrue(flowFiles.get(i-1).getAttribute(CoreAttributes.FILENAME.key()).compareTo(flowFiles.get(i).getAttribute(CoreAttributes.FILENAME.key())) < 0);
        }
    }
    
    @Test
    public void testReverseAlphabeticalOrder() throws IOException {
        TestRunner runner = buildTestRunner();
        runner.setProperty(ListS3.MAX_OBJECTS, "3");
        runner.setProperty(ListS3.ORDER, ListS3.ORDER_REVERSE_ALPHABETICAL.getValue());

        runner.enqueue(new byte[0]);
        runner.run(1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        for (int i=1; i < flowFiles.size(); i++) {
            assertTrue(flowFiles.get(i-1).getAttribute(CoreAttributes.FILENAME.key()).compareTo(flowFiles.get(i).getAttribute(CoreAttributes.FILENAME.key())) > 0);
        }
    }
    
    @Test
    public void testChronologicalOrder() throws IOException {
        TestRunner runner = buildTestRunner();
        runner.setProperty(ListS3.MAX_OBJECTS, "3");
        runner.setProperty(ListS3.ORDER, ListS3.ORDER_CHRONOLOGICAL.getValue());

        runner.enqueue(new byte[0]);
        runner.run(1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        for (int i=1; i < flowFiles.size(); i++) {
            assertTrue(flowFiles.get(i-1).getAttribute("s3.lastModified").compareTo(flowFiles.get(i).getAttribute("s3.lastModified")) < 0);
        }
    }
    
    @Test
    public void testReverseChronologicalOrder() throws IOException {
        TestRunner runner = buildTestRunner();
        runner.setProperty(ListS3.MAX_OBJECTS, "3");
        runner.setProperty(ListS3.ORDER, ListS3.ORDER_REVERSE_CHRONOLOGICAL.getValue());

        runner.enqueue(new byte[0]);
        runner.run(1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        for (int i=1; i < flowFiles.size(); i++) {
            assertTrue(flowFiles.get(i-1).getAttribute("s3.lastModified").compareTo(flowFiles.get(i).getAttribute("s3.lastModified")) > 0);
        }
    }
    
    @Test
    public void testMultiplePrefixes() throws IOException {
        DateTimeFormatter hourFormatter = DateTimeFormat.forPattern("yyyy/MM/dd/HH").withZoneUTC();
        
        DateTime lastHour = NOW.minusHours(1);
        String[] prefixes = {BASE_PREFIX + hourFormatter.print(NOW), BASE_PREFIX + hourFormatter.print(lastHour)};

        TestRunner runner = buildTestRunner();
        runner.setProperty(ListS3.PREFIXES, StringUtils.join(prefixes, '\n'));
        
        runner.enqueue(new byte[0]);
        runner.run(1);
        
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        assertFalse(flowFiles.isEmpty());
    }
    
    @Test
    public void testMultiplePrefixesWithMax() throws IOException {
        DateTimeFormatter hourFormatter = DateTimeFormat.forPattern("yyyy/MM/dd/HH").withZoneUTC();
        
        DateTime oneHourAgo = NOW.minusHours(1);
        DateTime twoHoursAgo = NOW.minusHours(2);
        String oneHourAgoPrefix = BASE_PREFIX + hourFormatter.print(oneHourAgo);
        String twoHoursAgoPrefix = BASE_PREFIX + hourFormatter.print(twoHoursAgo);
        String[] prefixes = {oneHourAgoPrefix, twoHoursAgoPrefix};

        TestRunner runner = buildTestRunner();
        runner.setProperty(ListS3.PREFIXES, StringUtils.join(prefixes, ','));
        runner.setProperty(ListS3.MAX_OBJECTS, "3");
        
        runner.enqueue(new byte[0]);
        runner.run(1);
        
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        assertEquals(3, flowFiles.size());
        
        // assert that all files are from the first prefix
        for (MockFlowFile f : flowFiles) {
            assertTrue(f.getAttribute(CoreAttributes.FILENAME.key()).contains(oneHourAgoPrefix));
        }
    }
    
    @Test
    public void testMultiplePrefixesWithSort() throws IOException {
        DateTimeFormatter hourFormatter = DateTimeFormat.forPattern("yyyy/MM/dd/HH").withZoneUTC();
        
        DateTime lastHour = NOW.minusHours(1);
        String[] prefixes = {BASE_PREFIX + hourFormatter.print(lastHour), BASE_PREFIX + hourFormatter.print(NOW)};

        TestRunner runner = buildTestRunner();
        runner.setProperty(ListS3.PREFIXES, StringUtils.join(prefixes, '\n'));
        runner.setProperty(ListS3.ORDER, ListS3.ORDER_REVERSE_CHRONOLOGICAL);
        
        runner.enqueue(new byte[0]);
        runner.run(1);
        
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        assertFalse(flowFiles.isEmpty());
        assertFlowfiles(flowFiles); // manually verify that files are sorted within prefixes, in order that prefixes were specified
    }
}

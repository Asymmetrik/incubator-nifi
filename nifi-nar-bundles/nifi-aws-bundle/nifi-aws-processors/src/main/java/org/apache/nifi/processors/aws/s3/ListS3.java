package org.apache.nifi.processors.aws.s3;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.S3ObjectSummary;

@TriggerSerially
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"Amazon", "S3", "AWS", "Get", "List"})
@CapabilityDescription("Retrieves a listing of objects from S3. For each object that is listed in S3, creates a FlowFile that represents "
        + "the S3 object so that it can be fetched in conjunction with FetchS3Object. This Processor does not delete any data from S3.")
@SeeAlso({FetchS3Object.class, PutS3Object.class})
@WritesAttributes({
    @WritesAttribute(attribute = "s3.bucket", description = "The name of the S3 bucket"),
    @WritesAttribute(attribute = "filename", description = "The key under which the object is stored"),
    @WritesAttribute(attribute = "s3.owner", description = "Owner of the object"),
    @WritesAttribute(attribute = "s3.lastModified", description = "Date (millis) when object was last modified"),
    @WritesAttribute(attribute = "s3.length", description = "Object length"),
    @WritesAttribute(attribute = "s3.storageClass", description = "Object storage class"),    
})
public class ListS3 extends AbstractS3Processor {
    
    public static final PropertyDescriptor PREFIXES = new PropertyDescriptor.Builder()
        .name("Object Key Prefixes")
        .description("Optional parameter restricting the response to keys which begin with the specified prefixes, comma or newline separated. One S3 request is made per prefix in the order specified, and returned objects are grouped by prefix in the order specified. If unspecified, the entire bucket is listed.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();
    public static final PropertyDescriptor MAX_OBJECTS = new PropertyDescriptor.Builder()
        .name("Max Objects")
        .description("Maximum number of objects to output.")
        .required(false)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();
    public static final PropertyDescriptor MIN_AGE = new PropertyDescriptor.Builder()
        .name("Minimum Age")
        .description("The minimum age that an object must be in order to be included in the listing; any object younger than this amount of time (based on last modification date) will be ignored")
        .required(true)
        .addValidator(StandardValidators.createTimePeriodValidator(0, TimeUnit.MILLISECONDS, Long.MAX_VALUE, TimeUnit.NANOSECONDS))
        .defaultValue("0 sec")
        .build();
    public static final PropertyDescriptor MAX_AGE = new PropertyDescriptor.Builder()
        .name("Maximum Age")
        .description("The maximum age that a object must be in order to be included in the listing; any object older than this amount of time (based on last modification date) will be ignored")
        .required(false)
        .addValidator(StandardValidators.createTimePeriodValidator(100, TimeUnit.MILLISECONDS, Long.MAX_VALUE, TimeUnit.NANOSECONDS))
        .build();
    public static final PropertyDescriptor CHRONOLOGICAL_ORDER = new PropertyDescriptor.Builder()
        .name("Chronological Order")
        .description("Return objects within each prefix in chronological order; if false objects are returned in lexicographic (alphabetical) order. When multiple prefixes are used, all objects for the first prefix are returned before objects for the next prefix.")
        .required(false)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .defaultValue("false")
        .build();
    public static final PropertyDescriptor REVERSE_SORT = new PropertyDescriptor.Builder()
        .name("Reverse Sort")
        .description("Reverse sort the listing of objects within each prefix.")
        .required(false)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .defaultValue("false")
        .build();
    public static final PropertyDescriptor POLLING_INTERVAL = new PropertyDescriptor.Builder()
        .name("Polling Interval")
        .description("Indicates how long to wait before performing a listing")
        .required(true)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("60 min")
        .build();
    
    private static final String PREFIX_DELIMITERS = ",\n";
    
    private int maxObjects = 0;
    private long minAge = 0L, maxAge = 0L;
    private String bucket;
    private String[] prefixes;
    private boolean chronologicalOrder;
    private boolean reverseSort;
    
    private final AtomicLong lastRunTime = new AtomicLong(0L);
    
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(POLLING_INTERVAL);
        props.add(BUCKET);
        props.add(PREFIXES);
        props.add(REGION);
        props.add(ACCESS_KEY);
        props.add(SECRET_KEY);
        props.add(CREDENTIALS_FILE);
        props.add(TIMEOUT);
        props.add(MAX_OBJECTS);
        props.add(MIN_AGE);
        props.add(MAX_AGE);
        props.add(CHRONOLOGICAL_ORDER);
        props.add(REVERSE_SORT);
        return Collections.unmodifiableList(props);
    }
    
    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        return Collections.unmodifiableSet(relationships);
    }
    
    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> problems = new ArrayList<>(super.customValidate(context));

        final Long minAgeProp = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final Long maxAgeProp = context.getProperty(MAX_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final long minAge = (minAgeProp == null) ? 0L : minAgeProp;
        final long maxAge = (maxAgeProp == null) ? Long.MAX_VALUE : maxAgeProp;
        if (minAge > maxAge) {
            problems.add(new ValidationResult.Builder().valid(false).subject(getClass().getSimpleName() + " Configuration")
                    .explanation(MIN_AGE.getName() + " cannot be greater than " + MAX_AGE.getName()).build());
        }

        return problems;
    }
    
    @Override
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        super.onScheduled(context);
        
        bucket = context.getProperty(BUCKET).evaluateAttributeExpressions().getValue();
        prefixes = context.getProperty(PREFIXES).isSet() ? 
                StringUtils.split(context.getProperty(PREFIXES).evaluateAttributeExpressions().getValue(), PREFIX_DELIMITERS)
                : new String[] {null}; // a single null prefix to denote listing the entire bucket
        
        final Integer maxObjectsProp = context.getProperty(MAX_OBJECTS).asInteger();
        maxObjects = (maxObjectsProp == null) ? Integer.MAX_VALUE : maxObjectsProp;
        
        final Long minAgeProp = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        minAge = (minAgeProp == null) ? 0L : minAgeProp;
        final Long maxAgeProp = context.getProperty(MAX_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        maxAge = (maxAgeProp == null) ? Long.MAX_VALUE : maxAgeProp;
        
        chronologicalOrder = context.getProperty(CHRONOLOGICAL_ORDER).asBoolean();
        reverseSort = context.getProperty(REVERSE_SORT).asBoolean();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final long pollingMillis = context.getProperty(POLLING_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);
        if ((lastRunTime.get() >= System.currentTimeMillis() - pollingMillis)) {
            return;
        }
        
        int transferCount = 0;
        
        for (String prefix : prefixes) {
            transferCount += performListingForPrefix(session, prefix, transferCount);
        }
        
        if ( transferCount > 0 ) {
            getLogger().info("Successfully retrieved S3 listing from bucket {} with {} objects",
                    new Object[] {bucket, transferCount});
            session.commit();
            
        } else {
            getLogger().info("There is no data to list from bucket {} with prefixes {}. Yielding.",
                    new Object[] {bucket, StringUtils.join(prefixes, ',')});
            context.yield();
            return;
        }
        
        lastRunTime.set(System.currentTimeMillis());
    }
    
    private int performListingForPrefix(final ProcessSession session, final String prefix, int transferCount) {
        if (transferCount >= maxObjects) {
            return 0;
        }
        
        List<S3ObjectSummary> objectList = getObjectsForPrefix(prefix);
        
        for (S3ObjectSummary s3ObjectSummary : objectList) {
            if (transferCount >= maxObjects) {
                getLogger().debug("Reached max objects, no more objects will be sent");
                break;
            }
            transfer(session, s3ObjectSummary);
            transferCount++;
        }
        
        return transferCount;
    }
    
    /**
     * Retrieve all objects for the given prefix, omitting those that do not pass min/max age properties, and sort the returned List based on processor properties.
     * 
     * @param prefix optional prefix, may be null/empty to pull all objects in the bucket
     * @return sorted List of S3ObjectSummary objects
     */
    private List<S3ObjectSummary> getObjectsForPrefix(final String prefix) {
        // build request
        final ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucket);
        if (StringUtils.isNotEmpty(prefix)) {
            request.setPrefix(prefix);
        }
        
        List<S3ObjectSummary> objectList = new ArrayList<>();
        
        // get listing
        ObjectListing listing = getClient().listObjects(request);
        
        // process first batch
        filterListing(listing, objectList);
        
        // retrieve and process any remaining batches
        while (listing.isTruncated()) {
            listing = getClient().listNextBatchOfObjects(listing);
            filterListing(listing, objectList);
        }
        
        if (chronologicalOrder) {
            Collections.sort(objectList, new Comparator<S3ObjectSummary>() {
                @Override
                public int compare(final S3ObjectSummary o1, final S3ObjectSummary o2) {
                    return o1.getLastModified().compareTo(o2.getLastModified());
                }
            });
        }
        if (reverseSort) {
            Collections.reverse(objectList);
        }
        
        String listingBase = bucket + '/' + (StringUtils.isNotEmpty(prefix) ? prefix : "");
        getLogger().info("Successfully retrieved S3 listing for {} with {} objects",
                new Object[] {listingBase, objectList.size()});
        
        return objectList;
    }
    
    /**
     * Add the S3ObjectSummary objects in the ObjectListing to the given List, omitting those that do not pass min/max age properties.
     * 
     * @param listing
     * @param objectList
     */
    private void filterListing(final ObjectListing listing, final List<S3ObjectSummary> objectList) {
        if (listing.getObjectSummaries() != null) {
            for (S3ObjectSummary s3ObjectSummary : listing.getObjectSummaries()) {
                final long fileAge = System.currentTimeMillis() - s3ObjectSummary.getLastModified().getTime();
                if (minAge < fileAge && fileAge < maxAge) {
                    objectList.add(s3ObjectSummary);
                }
            }
        }
    }
    
    /**
     * Create a new FlowFile for the given S3ObjectSummary and transfer to the success relationship.
     * 
     * @param session
     * @param s3ObjectSummary
     */
    private void transfer(final ProcessSession session, final S3ObjectSummary s3ObjectSummary) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("s3.bucket", s3ObjectSummary.getBucketName());
        attributes.put(CoreAttributes.FILENAME.key(), s3ObjectSummary.getKey());

        final Owner owner = s3ObjectSummary.getOwner();
        attributes.put("s3.owner", owner != null ? owner.getDisplayName() : "");
        attributes.put("s3.lastModified", String.valueOf(s3ObjectSummary.getLastModified().getTime()));
        attributes.put("s3.length", String.valueOf(s3ObjectSummary.getSize()));
        attributes.put("s3.storageClass", s3ObjectSummary.getStorageClass());

        FlowFile flowFile = session.create();
        flowFile = session.putAllAttributes(flowFile, attributes);
        session.transfer(flowFile, REL_SUCCESS);
    }
}

package org.apache.nifi.processors.aws.cloudwatch;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StatisticSet;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;

import java.util.*;


@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"amazon", "aws", "cloudwatch", "metrics", "put", "publish"})
@CapabilityDescription("Publishes metrics to Amazon CloudWatch")
public class PutCloudWatchMetric extends AbstractAWSCredentialsProviderProcessor<AmazonCloudWatchClient> {

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("FlowFiles are routed to success after metric is sent to Amazon CloudWatch").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("FlowFiles are routed to failure if unable send metric to Amazon CloudWatch").build();

    public static final Set<Relationship> relationships = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    private static final Validator DOUBLE_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return (new ValidationResult.Builder()).subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
            } else {
                String reason = null;

                try {
                    Double.parseDouble(input);
                } catch (NumberFormatException var6) {
                    reason = "not a valid Double";
                }

                return (new ValidationResult.Builder()).subject(subject).input(input).explanation(reason).valid(reason == null).build();
            }
        }
    };


    public static final PropertyDescriptor NAMESPACE = new PropertyDescriptor.Builder()
            .name("Namespace")
            .description("The namespace for the metric data for CloudWatch")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor METRIC_NAME = new PropertyDescriptor.Builder()
            .name("MetricName")
            .description("The name of the metric")
            .expressionLanguageSupported(true)
            .required(true)
            .addValidator(new StandardValidators.StringLengthValidator(1, 255))
            .build();

    public static final PropertyDescriptor TIMESTAMP = new PropertyDescriptor.Builder()
            .name("Timestamp")
            .description("A point in time expressed as the number of milliseconds since Jan 1, 1970 00:00:00 UTC. If not specified, the default value is set to the time the metric data was received")
            .expressionLanguageSupported(true)
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();

    public static final PropertyDescriptor UNIT = new PropertyDescriptor.Builder()
            .name("Unit")
            .description("The unit of the metric. (e.g Seconds, Bytes, Megabytes, Percent, Count,  Kilobytes/Second, Terabits/Second, Count/Second) For details see http://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_MetricDatum.html")
            .expressionLanguageSupported(true)
            .required(false)
            //.allowableValues("Seconds", "Microseconds", "Milliseconds", "Bytes", "Kilobytes", "Megabytes", "Gigabytes", "Terabytes", "Bits", "Kilobits", "Megabits", "Gigabits", "Terabits", "Percent", "Count", "Bytes/Second", "Kilobytes/Second", "Megabytes/Second", "Gigabytes/Second", "Terabytes/Second", "Bits/Second", "Kilobits/Second", "Megabits/Second", "Gigabits/Second", "Terabits/Second", "Count/Second", "None")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final PropertyDescriptor VALUE = new PropertyDescriptor.Builder()
            .name("Value")
            .description("The value for the metric. Must be a double")
            .expressionLanguageSupported(true)
            .required(false)
            .addValidator(DOUBLE_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAXIMUM = new PropertyDescriptor.Builder()
            .name("Maximum")
            .description("The maximum value of the sample set. Must be a double")
            .expressionLanguageSupported(true)
            .required(false)
            .addValidator(DOUBLE_VALIDATOR)
            .build();


    public static final PropertyDescriptor MINIMUM = new PropertyDescriptor.Builder()
            .name("Minimum")
            .description("The minimum value of the sample set. Must be a double")
            .expressionLanguageSupported(true)
            .required(false)
            .addValidator(DOUBLE_VALIDATOR)
            .build();


    public static final PropertyDescriptor SAMPLECOUNT = new PropertyDescriptor.Builder()
            .name("SampleCount")
            .description("The number of samples used for the statistic set. Must be a double")
            .expressionLanguageSupported(true)
            .required(false)
            .addValidator(DOUBLE_VALIDATOR)
            .build();


    public static final PropertyDescriptor SUM = new PropertyDescriptor.Builder()
            .name("Sum")
            .description("The sum of values for the sample set. Must be a double")
            .expressionLanguageSupported(true)
            .required(false)
            .addValidator(DOUBLE_VALIDATOR)
            .build();


    //Dimensions.member.N

    public static final List<PropertyDescriptor> properties =
            Collections.unmodifiableList(
                    Arrays.asList(NAMESPACE, METRIC_NAME, VALUE, MAXIMUM, MINIMUM, SAMPLECOUNT, SUM, TIMESTAMP, UNIT, REGION, ACCESS_KEY, SECRET_KEY, CREDENTIALS_FILE, AWS_CREDENTIALS_PROVIDER_SERVICE, TIMEOUT, SSL_CONTEXT_SERVICE, ENDPOINT_OVERRIDE,
                            PROXY_HOST, PROXY_HOST_PORT)
            );


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        Collection<ValidationResult> problems = super.customValidate(validationContext);

        final boolean valueSet = validationContext.getProperty(VALUE).isSet();
        final boolean maxSet = validationContext.getProperty(MAXIMUM).isSet();
        final boolean minSet = validationContext.getProperty(MINIMUM).isSet();
        final boolean sampleCountSet = validationContext.getProperty(SAMPLECOUNT).isSet();
        final boolean sumSet = validationContext.getProperty(SUM).isSet();


        if (valueSet && (maxSet || minSet || sampleCountSet || sumSet)) {
            problems.add(new ValidationResult.Builder().subject("Metric").valid(false).explanation("Cannot set both Value or StatisticSet(Maximum, Minimum, SampleCount, Sum)").build());
        }

        return problems;
    }

    /**
     * Create client using aws credentials provider. This is the preferred way for creating clients
     */

    @Override
    protected AmazonCloudWatchClient createClient(ProcessContext processContext, AWSCredentialsProvider awsCredentialsProvider, ClientConfiguration clientConfiguration) {
        getLogger().info("Creating client using aws credentials provider");
        return new AmazonCloudWatchClient(awsCredentialsProvider, clientConfiguration);
    }

    /**
     * Create client using AWSCredentials
     *
     * @deprecated use {@link #createClient(ProcessContext, AWSCredentialsProvider, ClientConfiguration)} instead
     */
    @Override
    protected AmazonCloudWatchClient createClient(ProcessContext processContext, AWSCredentials awsCredentials, ClientConfiguration clientConfiguration) {
        getLogger().debug("Creating client with aws credentials");
        return new AmazonCloudWatchClient(awsCredentials, clientConfiguration);
    }


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final AmazonCloudWatchClient client = getClient();

        MetricDatum dataum = new MetricDatum();

        dataum.setMetricName(context.getProperty(METRIC_NAME).evaluateAttributeExpressions(flowFile).getValue());

        final String valueString = context.getProperty(VALUE).evaluateAttributeExpressions(flowFile).getValue();
        if (valueString != null) {
            dataum.setValue(Double.parseDouble(valueString));
        } else {
            StatisticSet statisticSet = new StatisticSet();
            statisticSet.setMaximum(Double.parseDouble(context.getProperty(MAXIMUM).evaluateAttributeExpressions(flowFile).getValue()));
            statisticSet.setMinimum(Double.parseDouble(context.getProperty(MINIMUM).evaluateAttributeExpressions(flowFile).getValue()));
            statisticSet.setSampleCount(Double.parseDouble(context.getProperty(SAMPLECOUNT).evaluateAttributeExpressions(flowFile).getValue()));
            statisticSet.setSum(Double.parseDouble(context.getProperty(SUM).evaluateAttributeExpressions(flowFile).getValue()));

            dataum.setStatisticValues(statisticSet);
        }


        final String timestamp = context.getProperty(TIMESTAMP).evaluateAttributeExpressions(flowFile).getValue();
        if (timestamp != null) {
            dataum.setTimestamp(new Date(Long.parseLong(timestamp)));
        }

        final String unit = context.getProperty(UNIT).evaluateAttributeExpressions(flowFile).getValue();
        if (unit != null) {
            dataum.setUnit(unit);
        }

        final PutMetricDataRequest metricDataRequest = new PutMetricDataRequest()
                .withNamespace(context.getProperty(NAMESPACE).evaluateAttributeExpressions(flowFile).getValue())
                .withMetricData(dataum);

        try {
            client.putMetricData(metricDataRequest);
            session.transfer(flowFile, REL_SUCCESS);
            getLogger().info("Successfully published cloudwatch metric for {}", new Object[]{flowFile});
        } catch (final Exception e) {
            getLogger().error("Failed to publish cloudwatch metric for {} due to {}", new Object[]{flowFile, e});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }

    }

}

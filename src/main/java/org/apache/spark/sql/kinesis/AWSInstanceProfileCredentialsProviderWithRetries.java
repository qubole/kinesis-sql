package org.apache.spark.sql.kinesis;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AWSInstanceProfileCredentialsProviderWithRetries
        extends InstanceProfileCredentialsProvider {

    private static final Log LOG =
            LogFactory.getLog(AWSInstanceProfileCredentialsProviderWithRetries.class);

    public AWSCredentials getCredentials() {
        int retries = 10;
        int sleep = 500;
        while(retries > 0) {
            try {
                return super.getCredentials();
            }
            catch (RuntimeException re) {
                LOG.error("Got an exception while fetching credentials " + re);
                --retries;
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException ie) {
                    // Do Nothing here
                }
                if (sleep < 10000) {
                    sleep *= 2;
                }
            }
            catch (Error error) {
                LOG.error("Got an exception while fetching credentials " + error);
                --retries;
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException ie) {
                    // Do Nothing here
                }
                if (sleep < 10000) {
                    sleep *= 2;
                }
            }
        }
        throw new AmazonClientException("Unable to load credentials.");
    }
}

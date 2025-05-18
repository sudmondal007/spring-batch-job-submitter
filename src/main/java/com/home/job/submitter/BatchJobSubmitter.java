package com.home.job.submitter;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.batch.BatchClient;
import software.amazon.awssdk.services.batch.model.ContainerOverrides;
import software.amazon.awssdk.services.batch.model.KeyValuePair;
import software.amazon.awssdk.services.batch.model.SubmitJobRequest;
import software.amazon.awssdk.services.batch.model.SubmitJobResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

public class BatchJobSubmitter implements RequestHandler<S3Event, Boolean> {
	
	
	@Override
    public Boolean handleRequest(S3Event s3Event, Context context) {
		LambdaLogger log = context.getLogger();
		System.out.println("accept called.................");
        if(s3Event.getRecords().isEmpty()) {
        	System.out.println("No records found");
        	return false;
        }
        
        for(S3EventNotification.S3EventNotificationRecord s3record: s3Event.getRecords()) {
        	String bucketName = s3record.getS3().getBucket().getName();
        	String objectKey = s3record.getS3().getObject().getKey();
        	
        	try {
        		/*S3Client s3client = S3Client.builder().region(Region.US_EAST_2).build();
        	
        		GetObjectRequest objectRequest = GetObjectRequest.builder().key(objectKey).bucket(bucketName).build();
        	
        		ResponseBytes<GetObjectResponse> objectBytes = s3client.getObjectAsBytes(objectRequest);
        		
        		if(objectBytes != null) {
        			BufferedReader reader = new BufferedReader(new InputStreamReader(objectBytes.asInputStream()));
        			
        			try {
        				String line;
        				while((line = reader.readLine()) != null) {
        					System.out.println("line: " + line);
        				}
        				
        			} catch (Exception ex) {
        				System.err.println();
        			}
        		}*/
        		
        		
        		try {
        		
	        		BatchClient batchClient = BatchClient.builder()
	                        .region(Region.of(System.getenv("MEM_AWS_REGION")))
	                        .build();
	        		
	        		String jobName = "fargate-job-" + System.currentTimeMillis();
	        		
					// Define environment variables to pass to the container
					List<KeyValuePair> environmentVariables = Arrays.asList(
							KeyValuePair.builder().name("bucket_name").value(bucketName).build(),
							KeyValuePair.builder().name("object_key").value(objectKey).build()
					);
	                
	                // Override container settings to include environment variables and resource requirements
	                ContainerOverrides containerOverrides = ContainerOverrides.builder()
	                        .environment(environmentVariables)
	                        .build();
	        		
	        		SubmitJobRequest submitJobRequest = SubmitJobRequest.builder()
	                        .jobName(jobName)
	                        .jobQueue(System.getenv("MEM_JOB_QUEUE_NM"))
	                        .jobDefinition(System.getenv("MEM_JOB_DEF_NM"))
	                        .containerOverrides(containerOverrides)
	                        .build();
	        		
	        		SubmitJobResponse submitJobResponse = batchClient.submitJob(submitJobRequest);
	        		
	        		System.out.println("Job " + jobName + " submitted with ID: " + submitJobResponse.jobId());
        		} catch (Exception ex) {
        			System.err.println("error submitting batch JOB: " + ex.getMessage());
        		}
        		
        	} catch (Exception ex) {
        		System.err.println("error: " + ex.getMessage());
        		return false;
        	}
        }
        return true;
    }
}

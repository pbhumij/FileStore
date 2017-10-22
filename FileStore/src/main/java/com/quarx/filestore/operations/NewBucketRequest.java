package com.quarx.filestore.operations;

import java.util.concurrent.Callable;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;

public class NewBucketRequest implements Callable<String> {
	private AmazonS3 s3Client;
	private String bucketName;

	public NewBucketRequest(AmazonS3 s3Client, String bucketName) {
		this.s3Client = s3Client;
		this.bucketName = bucketName;
	}

	@Override
	public String call() throws  AmazonServiceException, AmazonClientException{
		Bucket bucket = new Bucket();
		if(s3Client.doesBucketExistV2(bucketName)) {
			return "Bucket already exists.. try a different name";
		}
		else {
			bucket.setName(bucketName);
			s3Client.createBucket(bucket.getName());
			if(s3Client.doesBucketExistV2(bucket.getName())){
				return "bucket "+bucket.getName()+" was created";
			}
			else return "bucket was not created";
		}
	}
}

package com.quarx.filestore.operations;

import java.util.Iterator;
import java.util.concurrent.Callable;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class DeleteBucketRequest implements Callable<String>{
	private AmazonS3 s3Client;
	private String bucketName;
	
	public DeleteBucketRequest(AmazonS3 s3Client, String bucketName) {
		this.s3Client = s3Client;
		this.bucketName = bucketName;
	}

	@Override
	public String call() throws Exception {
		return delete(bucketName);
	}
	
	private String delete(String bucketName) {
		try {
			System.out.println("Deleting S3 bucket: " + bucketName);
			System.out.println(" - removing objects from bucket");
			ObjectListing object_listing = s3Client.listObjects(bucketName);
			while (true) {
				for (Iterator<?> iterator = object_listing.getObjectSummaries().iterator(); iterator.hasNext();) {
					S3ObjectSummary summary = (S3ObjectSummary) iterator.next();
					s3Client.deleteObject(bucketName, summary.getKey());
				}
				// more object_listing to retrieve?
				if (object_listing.isTruncated()) {
					object_listing = s3Client.listNextBatchOfObjects(object_listing);
				} else {
					break;
				}
			}
			System.out.println(" OK, bucket ready to delete!");
			s3Client.deleteBucket(bucketName);
			return "Bucket "+bucketName+" was deleted";
		} catch (AmazonServiceException e) {
			return e.getErrorMessage();
		} catch (SdkClientException e) {
			return e.getMessage();
		} catch(Exception e) {
			return e.getMessage();
		}
	}
}

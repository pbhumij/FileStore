package com.quarx.filestore.operations;

import java.util.List;
import java.util.concurrent.Callable;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;

public class ListBuckets implements Callable<List<Bucket>> {
	AmazonS3 s3Client;
	public ListBuckets(AmazonS3 s3Client) {
		this.s3Client = s3Client;
	}

	@Override
	public List<Bucket> call() throws Exception {
		List<Bucket> list = s3Client.listBuckets();
		return list;
	}	
}
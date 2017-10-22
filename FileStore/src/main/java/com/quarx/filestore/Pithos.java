
package com.quarx.filestore;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.quarx.filestore.chunk.FileChunkListContainer;
import com.quarx.filestore.chunk.Transfer;
import com.quarx.filestore.operations.ChunkDownload;
import com.quarx.filestore.operations.ChunkUpload;
import com.quarx.filestore.operations.DeleteBucketRequest;
import com.quarx.filestore.operations.ListBuckets;
import com.quarx.filestore.operations.NewBucketRequest;
import com.quarx.filestore.operations.ResumableDownload;
import com.quarx.filestore.operations.ResumableUpload;

public class Pithos {

	private String endPoint; 
	private String region; 
	private AmazonS3 s3Client;
	Logger logger;
	FileHandler handler;
	private static final String ACTION_1 = "Closing thread!"; 
	private static final String ACTION_2 = "Interrupted!"; 
	
	public Pithos() {
		this.endPoint = "http://127.0.0.1:8080";
		this.region = "CH-GV1";
	}
	
	public void initialize(){
		try {
			ClientConfiguration config = new ClientConfiguration();
			config.setSignerOverride("S3SignerType");
			EndpointConfiguration endpointConfiguration = new EndpointConfiguration(endPoint, region);
			BasicAWSCredentials awsCreds = new BasicAWSCredentials("AKIAIOSFODNN7EXAMPLE",
					"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
			this.s3Client = AmazonS3ClientBuilder.standard().withRegion(region).withClientConfiguration(config)
					.withPathStyleAccessEnabled(true).withCredentials(new AWSStaticCredentialsProvider(awsCreds))
					.withEndpointConfiguration(endpointConfiguration).disableChunkedEncoding().build();
			logger = Logger.getLogger(Pithos.class.getName());
			handler = new FileHandler("myapp-log.txt");
			handler.setFormatter(new SimpleFormatter());
			logger.addHandler(handler);
		} catch (IOException|AmazonClientException e) {
			logger.info(e.getMessage());
		} 
	}

	public void newBucket(String bucketName) {
		try {
			NewBucketRequest request = new NewBucketRequest(s3Client, bucketName);
			FutureTask<String> task = new FutureTask<>(request);
			ExecutorService executor = Executors.newFixedThreadPool(1);
			executor.execute(task);
			while (true) {
				if (task.isDone()) {
					executor.shutdown();
					return;
				}
				if (!task.isDone()) {
					logger.info(task.get());
				}
				if (!executor.awaitTermination(100, TimeUnit.MICROSECONDS)) {
					logger.info(ACTION_1);
				}
			}
		} catch (AmazonClientException|ExecutionException e) {
			logger.info(e.getMessage());
		} catch (InterruptedException e) {
			logger.info(ACTION_2+ e);
		    // clean up state...
		    Thread.currentThread().interrupt();		}
	}

	public void listBuckets() {
		try {
			ListBuckets list = new ListBuckets(s3Client);
			FutureTask<List<Bucket>> task = new FutureTask<>(list);
			ExecutorService executor = Executors.newFixedThreadPool(1);
			executor.execute(task);
			while (true) {
				if (task.isDone()) {
					executor.shutdown();
					return;
				}
				if (!task.isDone()) {
					List<Bucket> buckets = task.get();
					for (Bucket l : buckets) {
						logger.info(l.getName());
					}
				}
				if (!executor.awaitTermination(100, TimeUnit.MICROSECONDS)) {
					logger.info(ACTION_1);
				}
			}
		} catch (AmazonClientException|ExecutionException e) {
			logger.info(e.getMessage());
		} catch (InterruptedException e) {
			logger.info(ACTION_2+ e);
		    // clean up state...
		    Thread.currentThread().interrupt();
		} 
	}

	public void deleteBucket(String bucketName) {
		try {
			DeleteBucketRequest toDelBucket = new DeleteBucketRequest(s3Client, bucketName);
			FutureTask<String> task = new FutureTask<>(toDelBucket);
			ExecutorService executor = Executors.newFixedThreadPool(1);
			executor.execute(task);
			while (true) {
				if (task.isDone()) {
					executor.shutdown();
					return;
				}
				if (!task.isDone()) {
					logger.info(task.get());
				}
				if (!executor.awaitTermination(100, TimeUnit.MICROSECONDS)) {
					logger.info(ACTION_1);
				}
			}
		} catch (AmazonClientException|ExecutionException e) {
			logger.info(e.getMessage());
		} catch (InterruptedException e) {
			logger.info(ACTION_2+ e);
		    // clean up state...
		    Thread.currentThread().interrupt();
		} 
	}

	public FileChunkListContainer uploadFile(String filePath) throws IOException {
		Transfer transfer = new Transfer();
		FileChunkListContainer chunkList = new FileChunkListContainer();
		try {
			File file = new File(filePath);
			ChunkUpload toUpload = new ChunkUpload(s3Client, file);
			FutureTask<FileChunkListContainer> uploadTask = new FutureTask<>(toUpload);
			ExecutorService executor = Executors.newFixedThreadPool(1);
			executor.execute(uploadTask);
			while (true) {
				if (uploadTask.isDone()) {
					executor.shutdown();
					return chunkList;
				}
				if (!uploadTask.isDone()) {
					logger.info("uploading..");
					chunkList = uploadTask.get();
					logger.info("uploaded size: "+Integer.toString(chunkList.FileSize));
					transfer.listContainer = chunkList;
					transfer.status = transfer.UpdateStatus();
					transfer.file = new File(filePath);
					while (!transfer.status) {
						logger.info("resuming upload...");
						ResumableUpload resume = new ResumableUpload(s3Client, transfer);
						transfer = resume.ResumeUpload();
						transfer.status = transfer.UpdateStatus();
					}
					logger.info("File uploaded completely: " + transfer.status + " size: "
							+ transfer.listContainer.FileSize);
				}
				if (!executor.awaitTermination(100, TimeUnit.MICROSECONDS)) {
					logger.info(ACTION_1);
				}
			}

		} catch (AmazonClientException|IOException|ExecutionException e) {
			logger.info(e.getMessage());
		} catch (InterruptedException e) {
			logger.info(ACTION_2+ e);
		    // clean up state...
		    Thread.currentThread().interrupt();		} 
		return chunkList;
	}

	public void downloadFile(FileChunkListContainer listContainer){
		try {
			Transfer transfer = new Transfer();
			FileChunkListContainer downloadListContainer = listContainer;
			ChunkDownload toDownload = new ChunkDownload(s3Client, downloadListContainer);
			FutureTask<FileChunkListContainer> downloadTask = new FutureTask<>(toDownload);
			ExecutorService executor = Executors.newFixedThreadPool(1);
			executor.execute(downloadTask);
			while (true) {
				if (downloadTask.isDone()) {
					executor.shutdown();
					return;
				}
				if (!downloadTask.isDone()) {
					// wait for downloading task to complete.
					downloadListContainer = downloadTask.get();
					transfer.listContainer = downloadListContainer;
					transfer.status = transfer.UpdateStatus();
					while (!transfer.status) {
						ResumableDownload resume = new ResumableDownload(s3Client, transfer);
						transfer = resume.ResumeDownload();
						transfer.status = transfer.UpdateStatus();
					}
					logger.info("File downloaded completely: " + transfer.status + " size: "
							+ transfer.listContainer.FileSize);
				}
				if (!executor.awaitTermination(100, TimeUnit.MICROSECONDS)) {
					logger.info(ACTION_1);
				}
			}
		} catch (ExecutionException|IOException|AmazonClientException e) {
			logger.info(e.getMessage());
		} catch (InterruptedException e) {
			logger.info(ACTION_2+ e);
		    // clean up state...
		    Thread.currentThread().interrupt();
		}
	}

}

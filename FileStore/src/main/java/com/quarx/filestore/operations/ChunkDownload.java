package com.quarx.filestore.operations;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.Callable;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.quarx.filestore.chunk.FileChunkListContainer;
import com.quarx.filestore.chunk.FileChunkObject;

public class ChunkDownload implements Callable<FileChunkListContainer> {
	private AmazonS3 s3Client;
	private FileChunkListContainer downloadListContainer;
	private RandomAccessFile randomFile; // file where downloaded bytes will be written into.
	private FileChannel fileChannel;
	private ByteBuffer buffer;
	private S3ObjectInputStream stream;
	private BufferedInputStream bis;
	public Boolean status;
	int chunkSize = 1024*1024*1;
	int checkSize = 0;
	int count = 1;

	public ChunkDownload(AmazonS3 s3Client, FileChunkListContainer list_container) throws FileNotFoundException {
		super();
		this.s3Client = s3Client;
		this.downloadListContainer = list_container;
		randomFile = new RandomAccessFile("//home//quarx//Downloads//"+downloadListContainer.CloudFileName, "rw");
		fileChannel = randomFile.getChannel();
		buffer = ByteBuffer.allocate(chunkSize);
	}

	private void download() throws IOException, AmazonClientException {
		for(FileChunkObject c: downloadListContainer.chunkList) {
			c.ChunkStatus = false;
		}
		// download chunks
		for (FileChunkObject chunk_object : downloadListContainer.chunkList) {
			if(count<3) {
				try {
					S3Object object = s3Client.getObject("ziroh", chunk_object.FileName); // gets the object
					stream = object.getObjectContent();
					bis = new BufferedInputStream(stream);
					byte[] temp = new byte[chunk_object.size];
					int readBytes = bis.read(temp);
					buffer.put(temp);
					buffer.flip();
					fileChannel.position(chunk_object.offset);
					fileChannel.write(buffer);
					fileChannel.force(true);
					buffer.flip();
					chunk_object.ChunkStatus = true;
					System.out.println("chunk key:"+object.getKey()+" offset:"+chunk_object.offset+" status:"+chunk_object.ChunkStatus+" size:"+readBytes);		
				} finally {
					bis.close();
					stream.close();
				}
			}
			count++;
		}
		buffer.clear();
		fileChannel.close();
		randomFile.close();
		downloadListContainer.status = updateStatus(downloadListContainer.chunkList);
		downloadListContainer.FileSize = (int) downloadedSize(downloadListContainer);
	}

	@Override
	public FileChunkListContainer call() throws Exception {
		download();
		return downloadListContainer;
	}

	private Boolean updateStatus(List<FileChunkObject> list) {
		Boolean status = true;
		for (FileChunkObject c : list) {
			if (!c.ChunkStatus)
				status = false;
		}
		return status;
	}

	private long downloadedSize(FileChunkListContainer list) {
		long size = 0;
		for (FileChunkObject c : list.chunkList) {
			if (c.ChunkStatus)
				size = size + c.size;
		}
		return size;
	}
}

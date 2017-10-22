package com.quarx.filestore.operations;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.quarx.filestore.chunk.FileChunkListContainer;
import com.quarx.filestore.chunk.FileChunkObject;
import com.quarx.filestore.chunk.Transfer;

public class ResumableDownload{
	private AmazonS3 s3Client;
	Transfer transfer;
	private RandomAccessFile randomFile; // file where downloaded bytes will be written into.
	private FileChannel fileChannel;
	private ByteBuffer buffer;
	private S3ObjectInputStream stream;
	private BufferedInputStream bis;
	private int chunkSize = 1024*1025*1;

	public ResumableDownload(AmazonS3 s3Client, Transfer transfer) throws FileNotFoundException {
		this.s3Client = s3Client;
		this.transfer = transfer;
		randomFile = new RandomAccessFile("//home//quarx//Downloads//"+transfer.listContainer.CloudFileName, "rw");
		fileChannel = randomFile.getChannel();
		buffer = ByteBuffer.allocate(chunkSize);
	}
	
	public Transfer ResumeDownload() throws IOException, AmazonClientException {
		for(FileChunkObject c: transfer.listContainer.chunkList) {
			if(!c.ChunkStatus) {
				try {
					S3Object object = s3Client.getObject("ziroh", c.FileName); // gets the object
				    stream = object.getObjectContent();
				    bis = new BufferedInputStream(stream);
					byte[] temp = new byte[c.size];
					int readBytes = bis.read(temp);
					if(readBytes>0) {
						buffer.put(temp);
						buffer.flip();
						fileChannel.position(c.offset);
						fileChannel.write(buffer);
						fileChannel.force(true);
						c.ChunkStatus = true;
						buffer.flip();
						System.out.println("chunk key: " + c.FileName + " offset:" + c.offset+ "status: "+c.ChunkStatus);
					}	
				} finally {
					bis.close();
					stream.close();
				}				
			}
		}
		buffer.clear();
		fileChannel.close();
		randomFile.close();
		transfer.listContainer.FileSize = (int)downloadedSize(transfer.listContainer);
		return transfer;
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

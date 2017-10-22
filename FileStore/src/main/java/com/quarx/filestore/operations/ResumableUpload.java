package com.quarx.filestore.operations;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.quarx.filestore.chunk.FileChunkListContainer;
import com.quarx.filestore.chunk.FileChunkObject;
import com.quarx.filestore.chunk.Transfer;

public class ResumableUpload{
	AmazonS3 s3Client;
	private int chunkSize = 1024 * 1024 * 1;
	private Transfer transfer;
	private File file;
	private Path filePath;
	private SeekableByteChannel channel;

	private ByteBuffer buffer;

	public ResumableUpload(AmazonS3 s3Client, Transfer transfer) throws IOException {
		this.s3Client = s3Client;
		this.transfer = transfer;
		this.file = transfer.file;
		this.filePath = FileSystems.getDefault().getPath(file.toString(), "");
		channel = Files.newByteChannel(filePath);
		buffer = ByteBuffer.allocate(chunkSize);
	}

	public Transfer ResumeUpload() throws IOException, FileNotFoundException {
		byte[] byteArray;
		try {
			// upload chunks only with status = false
			for (FileChunkObject chunk : transfer.listContainer.chunkList) {
				if (!chunk.ChunkStatus) {
					// resume upload
					// get the chunk objects with status:fail for uploading
					channel.position(chunk.offset);
					int readBytes = channel.read(buffer);
					buffer.flip();
					byteArray = new byte[readBytes];
					buffer.get(byteArray);
					ObjectMetadata metadata = new ObjectMetadata();
					metadata.setContentLength(readBytes);
					InputStream stream = new ByteArrayInputStream(byteArray);
					// upload the chunk
					s3Client.putObject("ziroh", chunk.FileName, stream, metadata);
					chunk.ChunkStatus = true; // if no exception was thrown during the above statement
					chunk.size = byteArray.length;
					System.out.println("chunk key:" + chunk.FileName + " offset:" + chunk.offset
							+ " status:" + chunk.ChunkStatus+" size:"+chunk.size);
					stream.close();
					buffer.flip();				
				}
			}
		} catch (AmazonServiceException e) {
			System.out.println(e.getErrorMessage());
		} catch (SdkClientException e) {
			System.out.println(e.getMessage());
		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			buffer.clear();
			channel.close();
			transfer.listContainer.FileSize = uploadedSize(transfer.listContainer);
		}

		return transfer;
	}

	private int uploadedSize(FileChunkListContainer list) {
		int size = 0;
		for (FileChunkObject c : list.chunkList) {
			if (c.ChunkStatus)
				size = size + c.size;
		}
		return size;
	}

}
package com.quarx.filestore.operations;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.Callable;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.quarx.filestore.chunk.FileChunkListContainer;
import com.quarx.filestore.chunk.FileChunkObject;

public class ChunkUpload implements Callable<FileChunkListContainer> {
	private File file;
	private Path filePath;
	private SeekableByteChannel channel;
	private ByteBuffer buffer;
	private AmazonS3 s3Client;
	private int chunkSize = 1024 * 1024 * 1;
	private FileChunkListContainer listContainer;
	private int offset = 0;

	// Constructor for file upload operation.
	public ChunkUpload(AmazonS3 s3Client, File file) throws IOException {
		this.s3Client = s3Client;
		this.file = file;
		this.filePath = FileSystems.getDefault().getPath(file.toString(), "");
		channel = Files.newByteChannel(filePath);
		buffer = ByteBuffer.allocate(chunkSize);
	}

	@Override
	public FileChunkListContainer call() throws Exception {
		listContainer = new FileChunkListContainer();
		listContainer = upload();
		return listContainer;
	}

	private FileChunkListContainer upload() throws IOException, AmazonClientException {
		int readBytes;
		byte[] byteArray;
		int count = 1;
		listContainer.chunkList = new ArrayList<FileChunkObject>();
		try {
			do {
				// Create FileChunkObject for a chunk
				FileChunkObject chunkObject = new FileChunkObject();
				chunkObject = chunkSetup(chunkObject);
				channel.position(chunkObject.offset);
				readBytes = channel.read(buffer);
				buffer.flip(); // prepare the buffer for read operation
				byteArray = new byte[readBytes]; // byte size depends on the number of bytes written to the buffer from
													// channel
				buffer.get(byteArray); // write bytes into byteArray
				InputStream stream = new ByteArrayInputStream(byteArray); // stream is loaded for upload
				ObjectMetadata metadata = new ObjectMetadata();
				metadata.setContentLength(readBytes);
				// skip few chunks to test resumable
				if(count<=2) {
					// upload the chunk
					s3Client.putObject("ziroh", chunkObject.FileName, stream, metadata);
					chunkObject.ChunkStatus = true; // if no exception was thrown during the above statement
					chunkObject.size = byteArray.length;
				}	
				// display the uploaded chunk information
				System.out.println("chunk key:" + chunkObject.FileName + " offset:" + chunkObject.offset + " status:"
						+ chunkObject.ChunkStatus + " size:" + chunkObject.size);
				// add the chunk to the FileChunklistContainer.chunkList
				listContainer.chunkList.add(chunkObject);
				stream.close();
				buffer.flip();
				count++;
			} while (readBytes > 0);

		} finally {
			listContainer.CloudFileName = file.getName();
			listContainer.status = updateStatus(listContainer); // check for individual chunk status
			listContainer.FileSize = (int) uploadedSize(listContainer);
			buffer.clear();
			channel.close();
		}
		return listContainer;
	}

	private FileChunkObject chunkSetup(FileChunkObject chunk_object) {
		// Individual Chunk object properties
		chunk_object.FileName = UUID.randomUUID().toString().replace("-", "");
		chunk_object.offset = 1024 * 1024 * offset;
		chunk_object.ChunkStatus = false; // initially
		offset++;
		return chunk_object;
	}

	private boolean updateStatus(FileChunkListContainer list) {
		boolean statusFlag = true;
		for (FileChunkObject f : list.chunkList) {
			if (!f.ChunkStatus) {
				statusFlag = false;
				break;
			}
		}
		return statusFlag;
	}

	private long uploadedSize(FileChunkListContainer list) {
		long size = 0;
		for (FileChunkObject c : list.chunkList) {
			if (c.ChunkStatus)
				size = size + c.size;
		}
		return size;
	}

}

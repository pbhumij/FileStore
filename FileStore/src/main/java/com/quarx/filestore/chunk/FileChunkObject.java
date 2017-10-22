package com.quarx.filestore.chunk;

/*This object will be created for individual chunk. 
 * If file is split into 10 chunks, 
 * your upload function should have 10 object of this class and 
 * add them to the FileOperationList->chunkFileList.
 */
public class FileChunkObject {
	public String FileName; //This will hold the UUID for this individual chunk.
	public int offset; // Offset in the main file from where the data was read.
	public int size; // Size of this chunk. This may be different for only the last chunk file.
	public Boolean ChunkStatus; // Status information of this chunk. True if it was uploaded successfully.
	
	
}

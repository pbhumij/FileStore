package com.quarx.filestore.chunk;

import java.util.List;

// This is the object which needs to be returned by the upload function.
public class FileChunkListContainer {
	
	/*Status of the complete file. 
	 * This will get updated based on the status value for all the chunks listed in the chunkFileList. 
	 * If all are set to true, this will become true. 
	 * Implement a function UpdateStatus which will read all the items in chunkFileList 
	 * and if FileStatus is set to true for all the items, set status to true.
	 */
	public boolean status;
	public String CloudFileName;  //Name of the actual file name which is send for upload.
	public int FileSize;  //Total size of the file we are trying to upload
	public List<FileChunkObject> chunkList; // List of individual chunks which this file is split

}

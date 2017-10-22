package com.quarx.filestore.chunk;

import java.io.File;

/*
 * This class will be used by the calling layer to understand if the file is completely uploaded/downloaded or not.
 */
public class Transfer {
	public static boolean status;
	public FileChunkListContainer listContainer;
	public File file;
	
	/*
	 * This method sets Transfer.status = false if the file 
	 * was not completely uploaded. If the uploading process was paused
	 * some chunks' upload status becomes false. Returns a Boolean value.
	 */
	public boolean UpdateStatus() {
		status = true;
		for(FileChunkObject c: listContainer.chunkList) {
			if(c.ChunkStatus == false) {
				status = false;
				break;
			}
		}
		return status;
	}
}

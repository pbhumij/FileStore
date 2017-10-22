package com.quarx.filestore;

import com.quarx.filestore.chunk.FileChunkListContainer;

public class Main {
	private static String filePath = "//home//quarx//Music//song.mp3";
	static FileChunkListContainer listContainer;
	
	public static void main(String[] args) throws Exception {
		Pithos client = new Pithos();
		client.initialize();
		
		client.newBucket("demo3");
		listContainer = client.uploadFile(filePath);
		client.downloadFile(listContainer);		

	}

}
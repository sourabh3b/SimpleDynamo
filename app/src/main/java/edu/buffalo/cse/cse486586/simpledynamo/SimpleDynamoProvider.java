package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.util.Log;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.database.*;

import java.io.*;
import java.util.HashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ConcurrentHashMap;
import android.content.*;
import android.telephony.TelephonyManager;

import java.net.ServerSocket;
import java.net.Socket;

import android.os.AsyncTask;

import java.util.*;

public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();

	/*
	* delete - deletes node from the ring
	* */
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		/*
		 Note : Below code is taken from PA3, with slight modification to delete replication
		 only 3 cases
        *   - use only first 2 parameters
            - if * is given in selection parameter, then delete all key-val stored in entire DHT
            - if @ is given in selection parameter, then delete all key-val stored in current AVD
            - other
            Algorithm :
            1. remove data from global map
            2. return number of elements (although returning 1 also works)
        * */
		int numberOfElements = 0;

		String currentPort = Constants.currentPort;
		String[] allFiles = null;


		String[] customizedMessageObject = selection.split(Constants.queryDelimiter);
		String currentKey = customizedMessageObject[0];


		if(currentKey.equals(Constants.all)){

			int deletedRows = 0;
			int deletedRowsForContext = getContext().getContentResolver().delete(uri,Constants.current,null);

			ArrayList<String> globalNodeList = Constants.globalNodeList;
			for (int currentNodeIndex = 0, globalNodeListSize = globalNodeList.size(); currentNodeIndex < globalNodeListSize; currentNodeIndex++) {
				String node = globalNodeList.get(currentNodeIndex);
				if (!node.equals(Constants.currentPort)) {
					String nodeToQuery = String.valueOf(Integer.parseInt(node) * 2);
					NodeRelay nodeRelayObject = new NodeRelay();
					String customMessage = nodeRelayObject.send(nodeToQuery, Constants.deleteString, Constants.current);
					if (customMessage.isEmpty()) {
						continue;
					}
					deletedRows = deletedRows + Integer.valueOf(customMessage);
				}
			}

			return deletedRows == 0 ? deletedRowsForContext : deletedRowsForContext + deletedRows;
		}

		if (currentKey.equals(Constants.current)) {
			allFiles = getContext().fileList();
		} else {
			if (customizedMessageObject.length == 1) {
				try {
					String hashKey = genHash(currentKey);
					currentPort = Constants.findNodeLocationByHashKey(hashKey);
				}catch (Exception e){

				}


			}

			if (!Constants.currentPort.equals(currentPort)) {
				String response = new NodeRelay().send(String.valueOf(Integer.parseInt(currentPort) * 2),Constants.deleteString,currentKey);
				return response.isEmpty() ? 0 : Integer.parseInt(response);
			}
			allFiles = new String[]{currentKey};


			//deleting current key from replica
			Constants.removeReplicaByKey(currentKey, Constants.neighbourMaxNodes);


			int myNodeIndex = Constants.globalNodeList.indexOf(Constants.currentPort);
			int replicaNodeIndex =  (myNodeIndex + 1) % Constants.globalNodeList.size();
			String replicaNodePort = String.valueOf(Integer.parseInt(Constants.globalNodeList.get(replicaNodeIndex)) * 2);


			int numberOfReplicas = 3;
			if(numberOfReplicas-- > 0) {
				NodeRelay nodeRelayObject = new NodeRelay();
				nodeRelayObject.send(replicaNodePort,Constants.deleteRequestString,currentKey + Constants.keyValueSeparator + String.valueOf(numberOfReplicas));
			}
		}

		int currentFileIndex = 0, totalNumberOfFiles = allFiles.length;
		if (totalNumberOfFiles <= currentFileIndex) {
			return numberOfElements;
		} else {
			String currentFile;
			currentFile = allFiles[currentFileIndex];
			try {
				File targetFile;
				targetFile = new File(getContext().getFilesDir(), currentFile);
				if (getContext().deleteFile(targetFile.getName())) {
					numberOfElements++;
				}
			} catch (RuntimeException e) {

			}
			currentFileIndex++;
			if (currentFileIndex < totalNumberOfFiles) {
				do {
					currentFile = allFiles[currentFileIndex];
					try {
						File fileToDelete = new File(getContext().getFilesDir(), currentFile);
						if (getContext().deleteFile(fileToDelete.getName()))
							numberOfElements++;
					} catch (RuntimeException e) {

					}
					currentFileIndex++;
				} while (currentFileIndex < totalNumberOfFiles);
			}
			return numberOfElements;
		}
	}


	@Override
	public String getType(Uri uri) {
		return null;
	}

	/**
	 * Inserts content value
	 * Algorithm  :
	 */
	@Override
	public Uri insert(Uri uri, ContentValues values) {

		//the node which is currentPort by default
		String targetNode = Constants.currentPort;

		//get key, value from ContentValues
		String keyToInsert = (String) values.get(Constants.KEY);
		String valueToInsert = (String) values.get(Constants.VALUE);


		String[] customizedMessage = keyToInsert.split(Constants.queryDelimiter);
		String customizedMessageKey = customizedMessage[0];

		if(customizedMessage.length == 1) {
			try {
				String hashKey = genHash(customizedMessageKey);
				targetNode = Constants.findNodeLocationByHashKey(hashKey);
			}catch (Exception e){

			}
		}

		//check if targetNode is not same as currentPort
		//in this case, node relaying is done and key-val is inserted
		if (!targetNode.equals(Constants.currentPort)) {

			String message = customizedMessageKey + Constants.keyValueSeparator + valueToInsert;
			NodeRelay nodeRelayObject = new NodeRelay();
			nodeRelayObject.send(String.valueOf(Integer.parseInt(targetNode) * 2),Constants.insertString,message);

		} else {
			//else, insert to the file then replicate
			FileOutputStream fileOutputStream;
			try {
				fileOutputStream = getContext().openFileOutput(customizedMessageKey, Context.MODE_PRIVATE);
				fileOutputStream.write(valueToInsert.getBytes());
				fileOutputStream.close();
			} catch (Exception e) {
			}
			if(Constants.intermediateMap.contains(customizedMessageKey)){
				Constants.intermediateMap.get(customizedMessageKey).release();
				Constants.intermediateMap.remove(customizedMessageKey);
			}

			//replicate
			replicate(customizedMessageKey, valueToInsert, Constants.neighbourMaxNodes);
		}

		return uri;
	}


	/***
	 * replicates the key-val
	 */
	public boolean replicateDone = false;
	public void replicate(String key, String value, int replicaCount){

		int currentNodeIndex = Constants.globalNodeList.indexOf(Constants.currentPort);
		int replicationLocation = (currentNodeIndex + 1) % Constants.globalNodeList.size();
		String replicaPort = String.valueOf(Integer.parseInt(Constants.globalNodeList.get(replicationLocation)) * 2);

		if (--replicaCount > 0) {
			String message = key + Constants.keyValueSeparator + value + Constants.keyValueSeparator+ String.valueOf(replicaCount);
			NodeRelay nodeRelayObject = new NodeRelay();
			nodeRelayObject.send(replicaPort,Constants.replicateString,message);
		}
		//set replicateDone flag to true
		replicateDone = true;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {

		String[] allFileList = null;
		String targetNode = Constants.currentPort;


		String[] customizedMessage = selection.split(Constants.queryDelimiter);
		String key = customizedMessage[0];

		if(key.equals(Constants.all)){
			/*
			 * Algorithm :
			 * 1. iterate through globalNodeList
			 * 2. get currentNode from globalNodeList
			 * 3. check currentNode is not same as currentPort
			 * 	3.1 get the nodeToQuery from currentNode information (by 2 * currentNode )
			 * 	3.2 create a semaphore with permit zero, it means there are no threads waiting, but if a thread tries to decrement, it will block.
			 * 	3.3 create a message object for this semaphore
			 * 	3.4 put the current message into global message map
			 * 	3.5 send the node to query to the client with
			 * 	3.6 acquire the semaphore for current message
			 * 	3.7 build acknowledgement from messagedata & prepare customMessage
			 * 4. else continue
			 * 5. customMessage replace all occurences of ; till end of line with empty string
			 * 6. get the cursor of the current message
			 * 7. merge currentCursor with matrixCursor of the currentKeyValPair
			* */
			String customMessage = "";
			Cursor currentCursor = getContext().getContentResolver().query(uri, null, Constants.current, null, null);

			ArrayList<String> globalNodeList = Constants.globalNodeList;

			//iterate through globalNodeList
			for (int currentNodeIndex = 0, globalNodeListSize = globalNodeList.size(); currentNodeIndex < globalNodeListSize; currentNodeIndex++) {

				//get currentNode from globalNodeList
				String currentNode = globalNodeList.get(currentNodeIndex);

				//currentNode is not same as currentPort
				if (!currentNode.equals(Constants.currentPort)){


					//get the nodeToQuery from currentNode information (by 2 * currentNode )
					String nodeToQuery = String.valueOf(Integer.parseInt(currentNode) * 2);


					//create a semaphore with permit zero, it means there are no threads waiting, but if a thread tries to decrement, it will block.
					//Ref : [https://stackoverflow.com/questions/14793416/zero-permit-semaphores]
					Semaphore semaphore = new Semaphore(0);

					//create a message object for this semaphore
					Message message = new Message(null, semaphore);

					//put the current message into global message map
					Constants.messageMap.put(Constants.current, message);

					//send the node to query to the client with
					new DynamoClient().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, nodeToQuery, Constants.queryString, Constants.current);

					//acquire the semaphore for current message
					try {
						message.sem.acquire();
					} catch (Exception e) {

					}

					//build acknowledgement from messagedata & prepare customMessage
					String acknowledgement = message.messageData;
					Constants.messageMap.remove(Constants.current);

					if (!acknowledgement.isEmpty())
						customMessage = new StringBuilder().append(customMessage).append(acknowledgement + Constants.queryDelimiter).toString();
				}
				else {
					//, else continue to next iteration
					continue;
				}

			}

			//check for empty customMessage
			if(customMessage == null || customMessage.isEmpty())
				return currentCursor;

			//customMessage replace all occurences of ; till end of line with empty string
			customMessage = customMessage.replaceAll(";$","");

			//logic to get the cursor the current message
			String[] keyValuePairs = customMessage.split(Constants.queryDelimiter);
			MatrixCursor matrixCursor = new MatrixCursor(Constants.matrixColumns);
			int currentKeyValPair = 0, keyValuePairsLength = keyValuePairs.length;
			if (currentKeyValPair < keyValuePairsLength) {
				do {
					String keyValue = keyValuePairs[currentKeyValPair];
					String[] currentCustomMessage = keyValue.split(Constants.keyValueSeparator);

					MatrixCursor.RowBuilder rowBuilder = matrixCursor.newRow();
					rowBuilder.add(Constants.KEY, currentCustomMessage[0]);
					rowBuilder.add(Constants.VALUE, currentCustomMessage[1]);
					currentKeyValPair += 1;
				} while (currentKeyValPair < keyValuePairsLength);
			}


			//merge currentCursor with matrixCursor of the currentKeyValPair
			MergeCursor mergeCursor = new MergeCursor(new Cursor[]{currentCursor,matrixCursor});
			return mergeCursor;
		}
		//getContext for file  and store in allFileList
		else if(key.equals(Constants.current)){
			try {
				allFileList = getContext().fileList();
			}catch(NullPointerException e){
			}
		}
		else{
			//query other than *,@ case
			if(customizedMessage.length == 1) {

				//find NodeLocation By HashKey
				try {
					String hashKey = genHash(key);
					targetNode = Constants.findNodeLocationByHashKey(hashKey);
				}catch (Exception e){

				}

				//get terget NodeIndex and send key to the node
				String currentNode = null;
				int tergetNodeIndex = (Constants.globalNodeList.indexOf(targetNode) + 2) % Constants.globalNodeList.size();
				String targetQueryNode = Constants.globalNodeList.get(tergetNodeIndex);
				currentNode = targetQueryNode.equals(Constants.currentPort) ? Constants.currentPort : targetQueryNode;

				if (currentNode.equals(Constants.currentPort)) {
					//do nothing
				} else {
					return relayNode(key, currentNode);
				}
			}
			allFileList = new String[]{key};
		}

		try {
			MatrixCursor cursor = new MatrixCursor(Constants.matrixColumns);

			int currentFileIndex = 0, lastFileIndex = allFileList.length;
			if (currentFileIndex < lastFileIndex) {
				do {
					String fileList = allFileList[currentFileIndex];
					BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(getContext().openFileInput(fileList)));

					String currentFileValue = bufferedReader.readLine();

					bufferedReader.close();
					MatrixCursor.RowBuilder rowBuilder = cursor.newRow();


					rowBuilder.add(Constants.KEY, fileList);
					rowBuilder.add(Constants.VALUE, currentFileValue);
					currentFileIndex++;
				} while (currentFileIndex < lastFileIndex);
			}
			return cursor;
		} catch (Exception e) {
		}

		return null;
	}



	//relay to final Destination Node
	private Cursor relayNode(String selection, String finalDestinationNode){

		Semaphore semaphore = new Semaphore(0);
		Message messageTransitObject = new Message(null,semaphore);

		Constants.messageMap.put(selection, messageTransitObject);
		String targetNode = String.valueOf(Integer.parseInt(finalDestinationNode) * 2);


		//send targetNode to DynamoClient
		new DynamoClient().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, targetNode, Constants.queryString, selection);

		Log.d(TAG,"Message in transit :>>>"+messageTransitObject.toString());
		//acquire the semaphore to block
		try {
			messageTransitObject.sem.acquire();
		}catch(Exception e){

		}

		Constants.messageMap.remove(selection);
		if (messageTransitObject.messageData != null && !messageTransitObject.messageData.isEmpty()) {
			Cursor cursor = Constants.createCursor(messageTransitObject.messageData);
			return cursor;
		} else {
			return null;
		}

	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		return 0;
	}


	/*
	* This is the starting point of the app execution
	* 	/*
		 Algorithm :
         * 1. Calculate the port number that this AVD listens on
		 * 2. Generate nodeID using genHash
         * 3. put hash value to global map and sort acc. to node
         * 4. Initialize the global hash map for later use
         * 5. Start server
	*/
	@Override
	public boolean onCreate() {

		//Note: Below code is referenced from previous assignment with modification for DHT

		/*
		 Algorithm :
         * 1. Calculate the port number that this AVD listens on
		 * 2. Generate nodeID using genHash
         * 3. put hash value to global map and sort acc. to node
         * 4. Initialize the global hash map for later use
         * 5. Start server
		 */


         /*
         * Calculate the port number that this AVD listens on. (Referenced from PA1)
         * It is just a hack that I came up with to get around the networking limitations of AVDs.
         * The explanation is provided in the PA1 spec.
         */
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		Constants.currentPort = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		Log.d(TAG, "Current port is >>>>>>>>" + Constants.currentPort);


		try{
			Constants.node_Id = genHash(Constants.currentPort);

			//Add the current node to the global map
			TreeMap<String, String> nodeId_map = new TreeMap<String,String>();
			Constants.NodeHashList = new ArrayList<String>();

			//loop through all emulators
			for (String node : Constants.EmulatorList) {

				//generate hash for current port
				String hash = genHash(node);

				//put this hash value in nodeMap
				nodeId_map.put(hash, node);

				//push this hash value to global ring hash value
				Constants.NodeHashList.add(hash);
			}

			//sort global hash map
			Collections.sort(Constants.NodeHashList);

			//put the nodeMap as a new entry in the node list
			Constants.globalNodeList = new ArrayList<String>(nodeId_map.values());

		}catch(Exception e){

		}

		//initialize the message map and blocking queue for the messages
		Constants.intermediateMap = new ConcurrentHashMap<String, Semaphore>();
		Constants.messageMap = new ConcurrentHashMap<String, Message>();

		//Creating Server Socket
		try{
			ServerSocket serverSocket = new ServerSocket(Constants.SERVER_PORT);
			new DynamoServer().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

		}catch (IOException e){
			Log.e(TAG, "Can't create a ServerSocket");
		}

		return false;
	}


	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	//stablize Ring by adjusting the previous ans next nodes for the current (for currentPort in used)
	void stablizeRing(){

		int currentNodeIndex = Constants.globalNodeList.indexOf(Constants.currentPort);

		//get location of previous and next node
		int previousNodeLocation = (currentNodeIndex + Constants.globalNodeList.size() - 1) % Constants.globalNodeList.size();
		int previousTwoHopLocation = (currentNodeIndex + Constants.globalNodeList.size() - 2) % Constants.globalNodeList.size();
		String previousNodeLocationPort = Constants.globalNodeList.get(previousNodeLocation);
		String previousTwoHopLocationPort = Constants.globalNodeList.get(previousTwoHopLocation);
		String previousNodePort = String.valueOf(Integer.parseInt(previousNodeLocationPort) * 2);


		int nextNodeLocation = (currentNodeIndex + 1) % Constants.globalNodeList.size();
		String nextNodeLocationPort = Constants.globalNodeList.get(nextNodeLocation);
		String previousNodeAcknowledgement = new NodeRelay().send(previousNodePort,Constants.queryString,Constants.current);
		String nextNodePort =  String.valueOf(Integer.parseInt(nextNodeLocationPort) * 2);
		String nextNodeAcknowledgement = new NodeRelay().send(nextNodePort, Constants.queryString,Constants.current);




		if(nextNodeAcknowledgement != null) {
			String[] nextNodeCustomMessage = nextNodeAcknowledgement.split(";");
			int currentSuccessorIndex = 0, successorPartsLength = nextNodeCustomMessage.length;
			if (currentSuccessorIndex < successorPartsLength) {
				do {
					String keyValue = nextNodeCustomMessage[currentSuccessorIndex];

					String key = keyValue.split(Constants.keyValueSeparator)[0];
					String value = keyValue.split(Constants.keyValueSeparator)[1];

					String targetNode = null;
					try {
						String hashKey = genHash(key);
						targetNode = Constants.findNodeLocationByHashKey(hashKey);
					}catch (Exception e){

					}

					//write to file if targetNode == currentPort
					if (targetNode.equals(Constants.currentPort)) {
						FileOutputStream fileOutputStream;
						try {
							fileOutputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
							fileOutputStream.write(value.getBytes());
							fileOutputStream.close();
						} catch (Exception e) {
						}
						if (Constants.intermediateMap.contains(key)) {
							Constants.intermediateMap.get(key).release();
							Constants.intermediateMap.remove(key);
						}
					}
					currentSuccessorIndex++;
				} while (currentSuccessorIndex < successorPartsLength);
			}
		}

		//check for predecessor Response
		if(previousNodeAcknowledgement != null) {
			String[] previousList;
			previousList = previousNodeAcknowledgement.split(Constants.queryDelimiter);
			int currentPrevious = 0;
			if (currentPrevious < previousList.length) {
				do {
					String keyValue = previousList[currentPrevious];

					String key = keyValue.split(Constants.keyValueSeparator)[0];
					String value = keyValue.split(Constants.keyValueSeparator)[1];

					String targetNode = null;
					try {
						String hashKey = genHash(key);
						targetNode = Constants.findNodeLocationByHashKey(hashKey);
					}catch (Exception e){

					}

					if (targetNode.equals(previousNodeLocationPort) || targetNode.equals(previousTwoHopLocationPort)) {
						FileOutputStream file;
						try {
							file = getContext().openFileOutput(key, Context.MODE_PRIVATE);
							file.write(value.getBytes());
							file.close();
						} catch (Exception e) {
						}
						if(Constants.intermediateMap.contains(key)){
							Constants.intermediateMap.get(key).release();
							Constants.intermediateMap.remove(key);
						}
					}

					currentPrevious++;
				} while (currentPrevious < previousList.length);
			}
		}
		if(previousNodeAcknowledgement == null && nextNodeAcknowledgement == null) return;


	}


	/**
	 * The coordinator which acts a a central sequencer and process the queries
	 * This class is Runnable because it's socket is put into a thread doing the task with main UI thread
	 */
	public class Sequencer implements Runnable{

		@Override
		public String toString() {
			return "Sequencer{" +
					"currentSocket=" + currentSocket +
					", acknowledgement='" + acknowledgement + '\'' +
					'}';
		}

		public String getAcknowledgement() {
			return acknowledgement;
		}

		public void setAcknowledgement(String acknowledgement) {
			this.acknowledgement = acknowledgement;
		}

		Socket currentSocket;
		String acknowledgement = null;

		public Sequencer(Socket currentSocket){
			this.currentSocket = currentSocket;
		}

		/**
		 * This is invoked
		 * Algorithm:
		 * 1. fetch input stream from the currentSocket
		 * 2. get message this is coming from currentSocket
		 * 3. split socket message and deelimit to get content and category
		 * 4. get category of the message and call appropriate function to get the acknowledegement string
		 * 5. For valid ack, send server this message with acknowledgemeent
		 * */
		@Override
		public void run(){
			try {

				//fetch input stream from the currentSocket
				DataInputStream dataInputStream = new DataInputStream(currentSocket.getInputStream());

				//get message this is coming from currentSocket
				String socketMessage = dataInputStream.readUTF();

				//split socket message and deelimit to get content and category
				String[] customizedMessage = socketMessage.split("\\###");

				/*
					customizedMessage structure
					0th index = type of query
					1st index = data which contains key-value
								{
									- If it is insert, the customizedMessage structure is key<delimeter>value
									- If it is replicate, the customizedMessage structure is key<delimeter>value
									- If it is fetch query, the customizedMessage structure is either @ or * depending on the test case
									- If it is delete request , the customizedMessage structure is key<delimeter>number of replicas in the global map
									- If it is delete command, then customizedMessage structure return key which is being deleted
								}
				 */
				int category = Constants.categoryMap.get(customizedMessage[0]);


				if (category == Constants.Category.InsertNodeToRing.getValue()) {
					acknowledgement = InsertNodeToRing(customizedMessage[1]);

				}
				else if (category == Constants.Category.deleteNode.getValue()) {
					acknowledgement = deleteNode(customizedMessage[1]);

				}
				else if (category == Constants.Category.messageReplication.getValue()) {
					acknowledgement = messageReplication(customizedMessage[1],replicateDone);

				}
				else if (category == Constants.Category.queryRequest.getValue()) {
					acknowledgement = queryRequest(customizedMessage[1]);

				} else if (category == Constants.Category.replicaDelete.getValue()) {
					acknowledgement = replicaDelete(customizedMessage[1]);

				} else {

				}

				if (((acknowledgement == null) || acknowledgement.isEmpty())) {
					//do nothing (no acknowledgement)
				} else {
					OutputStream socketOutputStream = currentSocket.getOutputStream();
					DataOutputStream dataOutputStream = new DataOutputStream(socketOutputStream);

					//write this acknowledgement string to data output stream
					dataOutputStream.writeUTF(acknowledgement);
				}

			}catch(IOException e){
				Log.e(TAG, "Socket Exception");
			}
			finally{
				try{
					currentSocket.close();
				}catch(IOException e){
				}
			}

		}



		/**
		 * function to handle the replication
		 * Algorithm:
		 * 1. split the input customized message request to get key and value
		 * 2. get the number of replications for this message category
		 * 3. now insert this to key file and global blocking queue
		 * 4. check if key already present in the global blockin gqueue
		 * 	4.1 if exist then, remove it
		 * 5. replicate this message to next 2 nodes (call replicate function)
		 * 6. check if replication is done or not, if done then return ack, else return null
		 * 	6.1 If done then return Ackowledgement
		 *  6.2 Else return null
		 *
		 *
		 */
		private String messageReplication(String customMessage,boolean replicateDone){

			//split the input message request to get key and value
			String[] messageCustomizedObject = customMessage.split(Constants.keyValueSeparator);

			//get the number of replication for this message category
			int replicaCount = Integer.parseInt(messageCustomizedObject[2]);

			//now insert this to key file and global blocking queue
			FileOutputStream tempFileOutputStream = null;
			try {
				tempFileOutputStream = getContext().openFileOutput(messageCustomizedObject[0].trim(), Context.MODE_PRIVATE);
				tempFileOutputStream.write( messageCustomizedObject[1].trim().getBytes());


			} catch (IOException e) {

			}finally {
				try {
					tempFileOutputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			//check if key already present in the global blockin gqueue
			//if exist then, remove it
			if(Constants.intermediateMap.contains(messageCustomizedObject[0].trim())){
				Constants.intermediateMap.get(messageCustomizedObject[0].trim()).release();
				Constants.intermediateMap.remove(messageCustomizedObject[0].trim());
			}


			//replicate this message to next 2 nodes (call replicate function)
			replicate(messageCustomizedObject[0].trim(), messageCustomizedObject[1].trim(), replicaCount);


			//check if replication is done or not, if done then return ack, else return null
			if(replicateDone) {
				return Constants.replicationAck;
			}
			return null;
		}


		/**
		 This function inserts node to the ring
		 Algorithm :
		 1. parse the incoming request and check for the type of request equal to insert or not
		 2. build the content value with key-value pair(where key is the key to be inserted and value being messageRequest[1]
		 3. insert this content value for this URI
		 4. return insert acknowledgement message
		 */
		private String InsertNodeToRing(String customMessage) {

			//parse the incoming request and check for the type of request equal to insert or not
			String[] customMessageObject = customMessage.split(Constants.keyValueSeparator);


			//append the insert keyword to the key with the AVD who is currently sending this request
			String key = customMessageObject[0].trim() + Constants.insertSparator;

			//get value from message object
			String value = customMessageObject[1].trim();

			//build the content value with key-value pair(where key is the key to be inserted and value being messageRequest[1]
			ContentValues contentValue = new ContentValues();
			contentValue.put(Constants.KEY,key);
			contentValue.put(Constants.VALUE,value);


			getContext().getContentResolver().insert(Constants.CONTENT_URL, contentValue);

			//return insert acknowledgement message
			return Constants.insertAck;
		}


		/**
		 * function to query the requested data into
		 * Algorithm:
		 * 1. parse the incoming request and check for the type of request equal to insert or not
		 * 2. build the content value with key-value pair(where key is the key to be inserted and value being messageRequest[1]
		 * 3. get cursor corresponding key to query in the content provider
		 * 	3.1 If empty cursor then return null
		 * 4. If queryCursor is not empty, then get the terget key and value
		 * 5. prepare the result in form of key<delimeter>value //note: this order is important to properly precess messages in later run of the program
		 * 6.
		 * */
		private String queryRequest(String customMessage){

			//parse the incoming request and check for the type of request equal to insert or not
			String[] partialCustomMessageeObject = customMessage.split(Constants.keyValueSeparator);

			//build the content value with key-value pair(where key is the key to be inserted and value being messageRequest[1]
			String keyToQuery = partialCustomMessageeObject[0].trim() + Constants.querySeparator;

			//get cursor corresponding key to query in the content provider
			Cursor queryCursor = getContext().getContentResolver().query(Constants.CONTENT_URL,null,keyToQuery,null,null);


			String ackString = "";

			//check if queryCursor is empty or not
			if (!queryCursor.moveToNext()) {
				return null;
			} else {
				//If queryCursor is not empty, then get the terget key and value
				do {
					String targetKey = Constants.getTargetKeyValue(queryCursor,true);
					String targetValue = Constants.getTargetKeyValue(queryCursor,false);

					//prepare the result in form of key<delimeter>value //note: this order is important to properly precess messages in later run of the program
					String result = new StringBuilder().append(targetKey).append(Constants.keyValueSeparator).append(targetValue).toString();


					//prepare the acknowledgement string
					ackString = new StringBuilder().append(ackString).append(result + Constants.queryDelimiter).toString();
				} while (queryCursor.moveToNext());
			}

			queryCursor.close();

			//replace ack string with regex
			ackString = ackString.replaceAll(";$", "");

			return ackString;
		}


		/**
		 * function to delete replica
		 * Algorithm :
		 * 1.split customMessage to temporary messageCustomizedObject
		 * 2. get the key from the messageCustomizedObject
		 * 3. get count of replicas for this object
		 * 4. initialize file to delete correspnding to current key
		 * 5. delete this replica by calling removeReplicaByKey funcction
		 * 6. return delete ack
		 */
		private String replicaDelete(String customMessage) {

			//split customMessage to temporary messageCustomizedObject
			String[] messageCustomizedObject = customMessage.split(Constants.keyValueSeparator);

			//get the key from the messageCustomizedObject
			String replicaKeyToDelete = messageCustomizedObject[0].trim();

			//get count of replicas for this object
			int replicaCount = Integer.parseInt(messageCustomizedObject[1].trim());

			int initialNumberOfRows = 0;

			//initialize file to delete correspnding to current key
			File fileToDelete = new File(getContext().getFilesDir(),messageCustomizedObject[0].trim());
			if (getContext().deleteFile(fileToDelete.getName()))
				initialNumberOfRows++;
			else{
				//delete failed
			}

			//delete this replica by calling removeReplicaByKey funcction
			//removeReplicaByKey(messageCustomizedObject[0].trim(), replicaCount);
			Constants.removeReplicaByKey(messageCustomizedObject[0].trim(), replicaCount);

			return String.valueOf(initialNumberOfRows);
		}

		/**
		 * function to delete node from the ring
		 * Algorithm:
		 * 1. split customMessage to temporary messageCustomizedObject
		 * 2. Form the key from the messageCustomizedObject and add deleteSeparator to it
		 * 3. call delete funciton to delete this key from content provider
		 * 4. return delee acknowledgement
		 * */
		private String deleteNode(String customMessage){

			//split customMessage to temporary messageCustomizedObject
			String[] messageCustomizedObject = customMessage.split(Constants.keyValueSeparator);


			//Form the key fo delete from the messageCustomizedObject by add deleteSeparator to it to keep in the correct format
			String keyToDelete = messageCustomizedObject[0].trim() + Constants.deleteSeparator;


			//call delete funciton to delete this key from content provider
			getContext().getContentResolver().delete(Constants.CONTENT_URL, keyToDelete, null);

			return Constants.deleteNodeAck;
		}

	}

	/***
	 * DynamoServer is an AsyncTask that should handle incoming messages. It is created by
	 * DynamoServer.executeOnExecutor() call in SimpleMessengerActivity.
	 *
	 * Please make sure you understand how AsyncTask works by reading
	 * http://developer.android.com/reference/android/os/AsyncTask.html
	 *
	 * @author stevko
	 *
	 */
	public class DynamoServer extends AsyncTask<ServerSocket, String, Void> {

		//function to spawn a New Thread
		private void spawnNewThread(Socket inputSocket){
			Thread newThread;
			newThread = new Thread(new Sequencer(inputSocket));
			newThread.start();
		}

		/*
    * Note : Below code is referenced from previous assignment with slight modification to get node object from socket
    Algorithm :
    * 1. initialize the categoryMap which later needs to be filled
    * 2. put the messages types and their corresponding enums in the map
    * 3. do stablizeRing of nodes - to make sure ring is well connected in proper order
    * 4. In order to continue accepting more connections, use infinite while loop
    * 5. Listen for a connection to be made to the socket coming  as a param in AsyncTask and accepts it. [ Reference : https://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html]
    * 6. Create InputStream form incoming socket
    * 7. spawn new DhtServerSocket in a new thread
    * */
		//initialize the categoryMap which later needs to be filled
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			Constants.categoryMap = new HashMap<String, Integer>();


			//put the messages types and their corresponding enums in the map
			Constants.categoryMap.put(Constants.queryString, Constants.Category.queryRequest.getValue());
			Constants.categoryMap.put(Constants.insertString, Constants.Category.InsertNodeToRing.getValue());
			Constants.categoryMap.put(Constants.replicateString, Constants.Category.messageReplication.getValue());
			Constants.categoryMap.put(Constants.deleteString, Constants.Category.deleteNode.getValue());
			Constants.categoryMap.put(Constants.deleteRequestString, Constants.Category.replicaDelete.getValue());


			//do stablizeRing of nodes - to make sure ring is well connected in proper order
			stablizeRing();

			//start the server socket
			ServerSocket dhtServerSocket = sockets[0];

			try {
				do {
					try {
						Socket newDhtServerSocket = dhtServerSocket.accept();
						spawnNewThread(newDhtServerSocket);
					} catch (IOException e) {
					}

				} while (true);
			} catch (Exception e) {
			} finally {
				try {
					dhtServerSocket.close();
				} catch (Exception e ){

				}
			}

			return null;
		}


	}




}

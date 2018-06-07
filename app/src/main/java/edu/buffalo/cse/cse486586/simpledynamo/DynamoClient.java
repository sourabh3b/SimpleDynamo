package edu.buffalo.cse.cse486586.simpledynamo;

import android.os.AsyncTask;
import android.util.Log;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

/***
 * Created by sourabh on 5/8/18.
 * DynamoClient is an AsyncTask that should send a node over the network.
 * It is created by DynamoClient.executeOnExecutor() call
 *
 * @author stevko
 *
 */
public class DynamoClient extends AsyncTask<String, Void, Void> {

    static final String TAG = SimpleDynamoProvider.class.getSimpleName();

    public DynamoClient(){
        Log.d(TAG,"DynamoClient Object");
    }

    @Override
    protected Void doInBackground(String... customizedMessageObject) {

        /* Note : Below code is referenced from previous assignment with little modification to send node data to other clients
         * Objective : message communication to other clients including self
         * Algorithm :
         * 1. Fetch node data from 1st parameter of the function ( nodes[0])
         * 2. Create socket for fetched node (by calling getCurrentNodePort() function)
         * 3. Create a output stream from the socket coming as a param in AsyncTask
         * 4. Create node object output stream which can be sent as an object to other clients
         * 5. write message to buffered writer
         * 6. Flush and close Buffered writer
         * 7    . Close socket
         * Reference : [https://docs.oracle.com/javase/tutorial/networking/sockets/clientServer.html]
         *           : [https://www.youtube.com/watch?v=mq-f7zPZ7b8]
         */

        String nodeToSend = customizedMessageObject[0];

        String category = customizedMessageObject[1].trim();
        String key = customizedMessageObject[2].trim().split(Constants.keyValueSeparator)[0];
        String modifiedMessageObject = category + Constants.nodeRelayDelimiter + customizedMessageObject[2] + Constants.nodeRelayDelimiter + Constants.currentPort; //todo: change delim, move to constants

        Socket socket = null;
        try {
            //creating socket corresponding to current AVD port
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nodeToSend));

            //create a output stream from the socket coming as a param in AsyncTask
            //create message object output stream which can be sent as an object to other clients
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());

            //write message to buffered writer
            dataOutputStream.writeUTF(modifiedMessageObject);

            //check for special  case : message category other than replicate and insert
            if(!(category.equals(Constants.insertString) || category.equals(Constants.replicateString))) {
                String response = dataInputStream.readUTF();
                Constants.messageMap.get(key).setMessageData(response);
                Constants.messageMap.get(key).sem.release();
            }
        }catch(IOException e){
            failureHandle(customizedMessageObject);
        }finally{
            try{
                socket.close();
            }catch(Exception e){
            }
        }

        return null;
    }

    /***
     * function to handle failure
     * @param customMessageObject
     * Algorithm :
     * 1. Fetch message details from customMessageObject in the argument
     * 2. get PreviousNode info for current message port (the idea is to send message data to the next node of the failed node)
     * 3. initialize client socket
     * 4. get message data from client's DataOutputStream
     * 5. read message data from dataInputStream
     * 6. if messageFromNextNode is not null this means message is received successfully
     *  6.1  Then semaphore needs to be released for this message key
     */
    private void failureHandle(String... customMessageObject){


        //get the port of the passed customMessageObject at 0th index
        String currentMessagePort = customMessageObject[0];

        //get messageCategory
        String messageCategory = customMessageObject[1].trim();

        //get messageKey
        String messageKey = customMessageObject[2].trim().split(Constants.keyValueSeparator)[0];

        //form new CustomMessage
        String newCustomMessage = messageCategory + Constants.nodeRelayDelimiter + customMessageObject[2] + Constants.nodeRelayDelimiter + Constants.currentPort;

        //get PreviousNode info for current message port
        //the idea is to send message data to the next node of the failed node
        String newNodePort = Constants.getPreviousNode(currentMessagePort);


        //initialize client socket
        Socket clientSocket = null;
        try{
            clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(newNodePort));

            //get data from client's DataOutputStream
            DataOutputStream dataOutputStream = new DataOutputStream(clientSocket.getOutputStream());
            DataInputStream dataInputStream = new DataInputStream(clientSocket.getInputStream());

            dataOutputStream.writeUTF(newCustomMessage);

            //read message data from dataInputStream
            String messageFromNextNode = dataInputStream.readUTF();

            //if messageFromNextNode is not null this means message is received successfully
            //then semaphore needs to be released for this message key

            Constants.messageMap.get(messageKey).setMessageData(messageFromNextNode);
            Constants.messageMap.get(messageKey).sem.release();
            clientSocket.close();
        }catch(Exception e){
        }

    }
}
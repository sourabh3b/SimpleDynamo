package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;

/**
 * Created by sourabh on 5/9/18.
 *Class to relay nodes across  the ring
 */

public class NodeRelay {


    /**
     * functio to get Node By Port string
     * @param port
     * @return
     * Algorithm :
     * 1. get current node location
     * 2. modify the node location by taking mod with global list size
     * 3. return the location of modified
     */
    String getNodeByPort(String port){
        //get current node location
        String currentNodeLocation = String.valueOf(Integer.parseInt(port) / 2);

        //modify the node location by taking mod with global list size
        int modifiedNodeLocation = (Constants.globalNodeList.indexOf(currentNodeLocation) + 1) % Constants.globalNodeList.size();

        //return the location of modified
        return String.valueOf(Integer.parseInt(Constants.globalNodeList.get(modifiedNodeLocation)) * 2);
    }

    /**
     * function to send message content to the node to relay all kinds of messages, special case to handle failure for query type messages
     *
     * @param targetNode
     * @param messageCategory
     * @param messageMetadata
     * @return
     * Algorithm :
     * 1. create socket for the targetNodePort
     * 2. get OutputStream from serverSocket
     * 3. Initialize dataOutputStream with  server Socket OutputStream
     * 4. send customizedMessage to dataOutputStream to target node
     * 5. receive the message from dataInputStream
     * 6. check for the special case of QUERY
     *    6.1 if message category is not queryString, then handle node failure by calling the function
     *    6.2 else do nothing
     * 7.close server socket finally
     */
    public String send(String targetNode, String messageCategory, String messageMetadata){

        String nodeRelayDelimiter = Constants.nodeRelayDelimiter;

        String customizedMessage = messageCategory + nodeRelayDelimiter + messageMetadata + nodeRelayDelimiter + Constants.currentPort;

        Socket serverSocket = null;
        try {
            //create socket for the targetNodePort
            serverSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(targetNode));

            //get OutputStream from serverSocket
            OutputStream serverSocketOutputStream = serverSocket.getOutputStream();

            //Initialize dataOutputStream with  server Socket OutputStream
            DataOutputStream dataOutputStream = new DataOutputStream(serverSocketOutputStream);
            DataInputStream dataInputStream = new DataInputStream(serverSocket.getInputStream());

            //send customizedMessage to dataOutputStream to target node
            dataOutputStream.writeUTF(customizedMessage);

            //receive the message from dataInputStream
            String messageFromServer = dataInputStream.readUTF();
            return messageFromServer;

        }catch(IOException e){
            //check for the special case of QUERY
            //if message category is not queryString, then handle node failure by calling the function
            //else do nothing
            if(!messageCategory.equals(Constants.queryString)) {

                //get acknowledgement
                String acknowledgement = handleFailure(targetNode, messageCategory, messageMetadata);
                if (acknowledgement != null) return acknowledgement;
            }
        }finally {
            try {
                //close server socket finally
                serverSocket.close();
            } catch (Exception e) {
            }
        }

        return null;
    }

    /***
     * function to handle node failure
     * There are basically 4 types of failure scenarios
     * 1. Insert Failure
     * 2. Delete Failure
     * 3. Replicate failure
     * 4. Delete request failure
     *
     * @param port
     * @param msgType
     * @param customMetadata
     * @return
     *
     * Algorithm :
     * ######## Handle Failure - Insert
     *  1. get Node by port
     *  2. Since, insert has failure, this node will be sending replication request, form a customized message with message denoting that failure_by_insert has occured
     *  3. Form customized message which is sent to the calling function
     *  4. Send customized message to server
     *  5. Return Acknowledgement which shows that insert failure is handled (note, this is also used to check if insert failure didn't happned)
     *
     *
     *
     * ######## Handle Failure - Delete
     * ######## Handle Failure - Replicate
     * ######## Handle Failure - Delete request
     */
    private String handleFailure(String port, String msgType, String customMetadata) {

        if(port.equals("")) {
            return null;
        }

        if(msgType.equals(Constants.insertString)){

            //get Node by port
            String getRecentPort = getNodeByPort(port);

            //Since, insert has failure, this node will be sending replication request, form a customized message with message denoting that failure_by_insert has occured
            String customizedMessage = customMetadata + Constants.keyValueSeparator + String.valueOf(Constants.neighbourMaxNodes - 1) + Constants.insertFailureRequest;

            //Form customized message which is sent to the calling function
            //Send customized message to server
            String acknowledgement = new NodeRelay().send(getRecentPort,Constants.replicateString,customizedMessage);

            //Return Acknowledgement which shows that insert failure is handled (note, this is also used to check if insert failure didn't happned)
            return acknowledgement;
        }
        else if(msgType.equals(Constants.deleteString)){

            //get Node by port
            String getRecentPort = getNodeByPort(port);

            //Since, delete has failure, this node will be sending replication request, form a customized message with message denoting that failure_by_insert has occured
            String customizedMessage = customMetadata +Constants.keyValueSeparator + String.valueOf(Constants.neighbourMaxNodes - 1) + Constants.deleteFailureRequest;

            //Form customized message which is sent to the calling function
            //Send customized message to server
            String acknowledgement = new NodeRelay().send(getRecentPort,Constants.deleteRequestString,customizedMessage);

            //Return Acknowledgement which shows that delete failure is handled (note, this is also used to check if insert failure didn't happned)
            return acknowledgement;
        }
        else if(msgType.equals(Constants.replicateString)){

            //split the customMetadata with keyValueSeparator to getch the current replication count
            String[] customMessage = customMetadata.split(Constants.keyValueSeparator);

            //If number of replicas equal 1 then just return
            if(Integer.parseInt(customMessage[2]) == 1) return Constants.replicationAck;

            //get Node by port
            String getRecentPort = getNodeByPort(port);

            //Form customized message which is sent to the previous replica node
            String modifiedCustomMessage = customMessage[0] + Constants.keyValueSeparator + customMessage[1] + Constants.replicationFailureRequest;

            //Send customized message to server
            String acknowledgement = new NodeRelay().send(getRecentPort, Constants.replicateString, modifiedCustomMessage);

            //Return Acknowledgement which shows that delete failure is handled (note, this is also used to check if insert failure didn't happned)
            return acknowledgement;
        }

        else if(msgType.equals(Constants.deleteRequestString)){

            //split the customMetadata with keyValueSeparator to getch the current replication count
            String[] customMessage = customMetadata.split(Constants.keyValueSeparator);

            //If number of replicas equal 1 then just return
            if(Integer.parseInt(customMessage[1]) == 1) return "0";

            //get Node by port
            String getRecentPort = getNodeByPort(port);

            //Form customized message which is sent to the previous replica node
            String modifiedCustomMessage = customMessage[0] + Constants.deleteRequestFailureRequest;

            //Send customized message to server
            String acknowledgement = new NodeRelay().send(getRecentPort, Constants.deleteRequestString, modifiedCustomMessage);

            //Return Acknowledgement which shows that delete failure is handled (note, this is also used to check if insert failure didn't happned)
            return acknowledgement;

        }

        return null;
    }

}
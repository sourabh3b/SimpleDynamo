package edu.buffalo.cse.cse486586.simpledynamo;

import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * Created by sourabh on 4/16/18.
 * This class contains all constants needed for the program.
 * Idea is to keep constants separate so that there single configuration point for the program that can be changed in a single class
 * rather than changing through out the program.
 */

public class Constants {


    //Starting and Ending port (Other ports are incremented by value of 4)
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";

    //list of all ports for all AVDs
    static final String[] PortList = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};

    //server port
    static final int SERVER_PORT = 10000;

    //namespace for content provider for 2
    static final String contentProviderURI = "content://edu.buffalo.cse.cse486586.simpledynamo.provider";

    //Content Uri corresponding to given contentProviderURI
    static final Uri CONTENT_URL = Uri.parse(contentProviderURI);



    //constant value for all emulator values used to create hash (and store node in the ring)
    static final String BASE_EMULATOR_SERIAL0 = "5554";
    static final String BASE_EMULATOR_SERIAL1 = "5556";
    static final String BASE_EMULATOR_SERIAL2 = "5558";
    static final String BASE_EMULATOR_SERIAL3 = "5560";
    static final String BASE_EMULATOR_SERIAL4 = "5562";

    //list of all emulators
    static final String[] EmulatorList = new String[]{BASE_EMULATOR_SERIAL0, BASE_EMULATOR_SERIAL1,BASE_EMULATOR_SERIAL2,BASE_EMULATOR_SERIAL3,BASE_EMULATOR_SERIAL4};


    //current AVD port being considered
    static String myPort;

    //string constants for key and value
    static final String KEY = "key";
    static final String VALUE = "value";

    //matrix columns used
    static String[] matrixColumns = {KEY, VALUE};

    static int neighbourMaxNodes = 3;

    public static ArrayList<String> globalNodeList;

    public static String node_Id;

    //map to store intermediate nodes (Which will convert later)
    public static ConcurrentHashMap<String, Semaphore> intermediateMap;

    public static ConcurrentHashMap<String, Message> messageMap;


    //Insert, Delete, Query, Replicate
    public static Map<String, Integer> categoryMap;

    public static String currentPort;


    public static ArrayList<String> NodeHashList;

    public static String keyValueSeparator = "!";
    public static String all = "*";
    public static String current = "@";
    public static String queryDelimiter = ";";


    //ack message for various categories
    public static String insertString = "insertString";
    public static String insertSparator = ";insertSeparator";
    public static String insertAck = ";insertAck";
    public static String replicationAck = "replicationAck";

    public static String deleteString = "DELETE";
    public static String deleteSeparator = ";deleteSeparator";
    public static String deleteNodeAck = "deleteNodeAck";
    public static String deleteReplicaAck = "deleteReplicaAck";
    public static String deleteRequestString = "deleteRequestString";


    public static String queryString = "QUERY";
    public static String querySeparator = ";querySeparator";
    public static String queryRequestSeparator = ";$";

    public static String nodeRelayDelimiter = "###";

    public static String replicateString = "REPLICATE";

    //failure handling token
    public static String insertFailureRequest = ",insertFailureRequest";
    public static String replicationFailureRequest = ",1,replicationFailureRequest";
    public static String deleteFailureRequest = ",deleteFailureRequest";
    public static String deleteRequestFailureRequest = "deleteRequestFailureRequest";



    //function to get key-value data for the given cursor
    public static String getTargetKeyValue(Cursor queryCursor, boolean getKeyFlag) {
        String targetData = null;
        int targetColumnIndex;
        if(getKeyFlag) {
            targetColumnIndex = queryCursor.getColumnIndex(Constants.KEY);
        } else {
            targetColumnIndex = queryCursor.getColumnIndex(Constants.VALUE);
        }
        targetData = queryCursor.getString(targetColumnIndex);
        return targetData;
    }


    /**
     * function to clear replicas for the given key key
     * Algorithm :
     * 1. Get location of current port in the global node list
     * 2. Get the replica node location which is equal to size of global list % currentPortLocation (since the ring is circular)
     * 3. remove replicas one by one therefore decrement
     * 4. create nodeRelayObject to which message request is being sent
     * 5. send nodeRelayObject the replicaNodePort with message type to request to delete with the content key an current replications
     * 6. check for empty socketReadData for the node where data is sent
     * 7. return 1, which means removeReplicaByKey is successfully done
     * 8. return -11, which means removeReplicaByKey is failed
     * */
    public static int removeReplicaByKey(String keyToDelete, int totalReplications){


        //getting location of current port in the global node list
        int currentPortLocation = Constants.globalNodeList.indexOf(Constants.currentPort);

        //the replica node location is equal to size of global list % currentPortLocation (since the ring is circular)
        int replicaNodeIndex = (currentPortLocation + 1) % Constants.globalNodeList.size();

        //Form the replica node which needs to be relayed to other nodes
        String replicaNodePort = String.valueOf(Integer.parseInt(Constants.globalNodeList.get(replicaNodeIndex)) << 1);

        //remove replicas one by one therefore decrement replications
        if(totalReplications-- > 0) {
            //create nodeRelayObject to which message request is being sent
            NodeRelay nodeRelayObject = new NodeRelay();

            //send nodeRelayObject the replicaNodePort with message type to request to delete with the content key an current replications
            String socketReadData = nodeRelayObject.send(replicaNodePort,deleteRequestString,keyToDelete + keyValueSeparator + String.valueOf(totalReplications));

            //check for empty socketReadData for the node where data is sent
            //return 1, which means removeReplicaByKey is successfully done
            return !socketReadData.isEmpty() ? 1 : 0;
        }
        //return -11, which means removeReplicaByKey is failed
        return -1;
    }


    /**
     * function to find Node Location By the HashKey in the global node list
     * @param hashKey
     * @return
     * Algorithm :
     * 1. calculate length of global node list and iterate the entire list linearly
     * 2. calculate previous Node location and next location (by prev % globallist.size()
     * 3.1 compare the hash value of current node with previous node location, if true, then get next node location from global list
     * 3.2 compare the hash value of current node with previous node as well as next location, if true, then get next node location from global list
     * 4. return destination location
     */
    public static String findNodeLocationByHashKey(String hashKey){

        try{
            String destinationLocation = null;

            for(int currentNodeLocation=0; currentNodeLocation < Constants.globalNodeList.size(); currentNodeLocation++){
                int previousNodeLocation = currentNodeLocation;
                int nextNodeLocation = (previousNodeLocation + 1) % Constants.globalNodeList.size();

                if ((nextNodeLocation != 0 ||
                        hashKey.compareTo(Constants.NodeHashList.get(previousNodeLocation)) <= 0) &&
                        (hashKey.compareTo(Constants.NodeHashList.get(previousNodeLocation)) <= 0 ||
                                hashKey.compareTo(Constants.NodeHashList.get(nextNodeLocation)) > 0)) {
                    if (hashKey.compareTo(Constants.NodeHashList.get(previousNodeLocation)) < 0)
                        if (previousNodeLocation == 1) {
                            destinationLocation = Constants.globalNodeList.get(0);
                            break;
                        }
                } else {
            destinationLocation = Constants.globalNodeList.get(nextNodeLocation);
            break;
        }
            }

            return destinationLocation;

        }catch(Exception e){

        }

        return null;
    }


    //getPreviousNode - helper functoin to get Previous Node for the port passed in argument
    public static String getPreviousNode(String currentPort){
        int givenPortLocation = Constants.globalNodeList.indexOf(String.valueOf(Integer.parseInt(currentPort) / 2));
        int previousNodeLocation = (givenPortLocation + Constants.globalNodeList.size() - 1) % Constants.globalNodeList.size();
        String prevNode = String.valueOf(Integer.parseInt(Constants.globalNodeList.get(previousNodeLocation)) * 2);
        return prevNode;
    }

    /***
     * function to get cursor by message object
     * @param customMessage
     * @return
     * Algorithm :
     * 1. split incoming custom message
     * 2. create a MatrixCursor object with matrixColumns "key and value"
     * 3. iterate to all key value pairs for incoming custom message
     * 4. split each pair to get the row and add the current cursor to RowBuilder
     * 5. return cursor
     * */
    public static Cursor createCursor(String customMessage){
        String[] keyValuePairs = customMessage.split(Constants.queryDelimiter);

        MatrixCursor matrixCursor = new MatrixCursor(Constants.matrixColumns);

        int currentKeyValPair = 0, keyValuePairsLength = keyValuePairs.length;
        if (currentKeyValPair < keyValuePairsLength) {
            do {
                String keyValue = keyValuePairs[currentKeyValPair];
                String[] currentCustomMessage = keyValue.split(",");

                MatrixCursor.RowBuilder rowBuilder = matrixCursor.newRow();
                rowBuilder.add(Constants.KEY, currentCustomMessage[0]);
                rowBuilder.add(Constants.VALUE, currentCustomMessage[1]);
                currentKeyValPair++;
            } while (currentKeyValPair < keyValuePairsLength);
        }

        return matrixCursor;
    }
    /*
       *
       * I am considering 5 types of message passing based on type of message sent by client - this is the crux of the implementation.
        * queryRequest
        * InsertNodeToRing
        * messageReplication
        * deleteNode
        * replicaDelete
       * */
    public static enum Category {
        Test(0), //not used
        queryRequest(1), //insert query
        InsertNodeToRing(2), //join query
        messageReplication(3), //node wants to join the CHORD ring (request)
        deleteNode(4), //update previous node or predecessor
        replicaDelete(5), //update next or successor
        GetSpecificData(6), //for @ types queries (delete, insert, update)
        GetAllData(7), //for * type queries (delete, insert, update)
        PropogateGetSingleNode(8),//special case of handling single node in the CHORD ring
        PropogateMultipleNodes(9),//
        DeleteSingleNode(10),//delete single node from chord
        DeleteMultipleNodes(11),//
        RelayNodeInsertion(12), //
        RelaySingleNodeCase(13),//
        Stablize(14); //stablize the ring


        //helper function to get value for the enum
        //Ref : https://stackoverflow.com/questions/7996335/how-to-match-int-to-enum/7996473#7996473
        private int enumValue;

        Category(int Value) {
            this.enumValue = Value;
        }

        public int getValue() {
            return enumValue;
        }

    }
}

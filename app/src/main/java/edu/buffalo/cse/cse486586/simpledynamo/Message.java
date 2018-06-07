package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.concurrent.Semaphore;

/**
 * Created by sourabh on 5/8/18.
 * Data model for messageData
 */

public class Message {


    String messageData;//actual data of the messageData
    int messageSequenceNumber; //sequence number in integer
    String messageSender; //messageData sender
    Semaphore sem; //semaphore to control access of shared resource
    //Semaphore controls access to a shared resource through the use of a counter [Ref : https://www.geeksforgeeks.org/semaphore-in-java/]
    String messageDestination; //destination
    boolean isMessageDeliverable; //flag to check if messageData is deliverable


    public Message(String messageData, Semaphore sema){
        this.messageData = messageData;
        this.sem = sema;
    }

    public String getMessageData() {
        return messageData;
    }

    public int getMessageSequenceNumber() {
        return messageSequenceNumber;
    }

    public void setMessageSequenceNumber(int messageSequenceNumber) {
        this.messageSequenceNumber = messageSequenceNumber;
    }

    public String getMessageSender() {
        return messageSender;
    }

    public void setMessageSender(String messageSender) {
        this.messageSender = messageSender;
    }

    public Semaphore getSem() {
        return sem;
    }

    public void setSem(Semaphore sem) {
        this.sem = sem;
    }

    public String getMessageDestination() {
        return messageDestination;
    }

    public void setMessageDestination(String messageDestination) {
        this.messageDestination = messageDestination;
    }

    public boolean isMessageDeliverable() {
        return isMessageDeliverable;
    }

    public void setMessageDeliverable(boolean messageDeliverable) {
        isMessageDeliverable = messageDeliverable;
    }

    public void setMessageData(String messageData){
        this.messageData = messageData;
    }

    @Override
    public String toString() {
        return "Message{" +
                "messageData='" + messageData + '\'' +
                ", messageSequenceNumber=" + messageSequenceNumber +
                ", messageSender='" + messageSender + '\'' +
                ", sem=" + sem +
                ", messageDestination='" + messageDestination + '\'' +
                ", isMessageDeliverable=" + isMessageDeliverable +
                '}';
    }
}
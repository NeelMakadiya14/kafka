package Backup;

import org.apache.kafka.common.TopicPartition;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class SegmentsMapping {
    private static HashMap<TopicPartition, ArrayList<Long>> map;

    //called when kafka is started.
    //Loading the map object from file if exist otherwise initialing the map.
    public void initialise() {
        try {
            FileInputStream fileInputStream = new FileInputStream(System.getProperty("user.dir")+"/_myFile/segmentsMapping.ser");
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
            Object object = objectInputStream.readObject();

            this.map = (HashMap<TopicPartition, ArrayList<Long>>) object;
            objectInputStream.close();
            fileInputStream.close();
            System.out.println(map);

        } catch (FileNotFoundException e) {
            System.out.println("File is not exist. Hence initialising the map.");
            this.map = new HashMap<TopicPartition,ArrayList<Long>>();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    //Called when kafka is shutdown
    //Saving the map object.
    public void saveSegmentsMapping(){
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(System.getProperty("user.dir")+"/_myFile/segmentsMapping.ser");
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
            objectOutputStream.writeObject(this.map);
            objectOutputStream.close();
            fileOutputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Saved Segment Mapping..");
    }

    //push the baseOffset into the baseOffset list of given topicPartition.
    public void addBaseOffset(TopicPartition tp, long baseOffset){
        if(map.get(tp)==null){
            map.put(tp,new ArrayList<>());
        }
        map.get(tp).add(baseOffset);
        System.out.println(map);
    }

    //Return the baseOffset of the segment which contains the given offset of given TopicPartition.
    public long getBaseOffset(TopicPartition tp, long offset){
        ArrayList<Long> baseOffsetList = map.get(tp);
        int pos = Collections.binarySearch(baseOffsetList,offset);
        if(pos>=0){
            return baseOffsetList.get(pos);
        }
        else{
            int index = (-1)*pos;
            index = index - 2;

            return baseOffsetList.get(index);
        }
    }
}

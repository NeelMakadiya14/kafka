package Backup;


import kafka.log.LogSegment;
import org.apache.kafka.common.TopicPartition;

import java.io.File;


public class S3BackUp {

    //first adding the base offset to the mapping and then uploading .log and .index file to S3.
    public static void uploadFile(TopicPartition topicPartition, LogSegment segment, long baseOffset){
        addBaseOffset(topicPartition,baseOffset);
        Storage storage = new Storage();
        storage.uploadFile(segment.log().file(),"Backup/"+topicPartition.topic()+"/partition-"+topicPartition.partition()+"/"+baseOffset+".log");
        storage.uploadFile(segment.offsetIndex().file(), "Backup/"+topicPartition.topic()+"/partition-"+topicPartition.partition()+"/"+baseOffset+".index");
        System.out.println("Backup complete for base offset "+baseOffset);
    }

    public static File retrieveLogFile(long baseOffset, TopicPartition topicPartition){
        Storage storage = new Storage();
        String downloadPath = System.getProperty("user.dir")+"/_myFile/"+topicPartition.topic()+"-"+topicPartition.partition()+"/"+baseOffset+".log";
        String key = "Backup/"+topicPartition.topic()+"/partition-"+topicPartition.partition()+"/"+baseOffset+".log";

        return storage.downloadFile(key,downloadPath);
    }

    public static File retrieveIndexFile(long baseOffset, TopicPartition topicPartition){
        Storage storage = new Storage();
        String downloadPath = System.getProperty("user.dir")+"/_myFile/"+topicPartition.topic()+"-"+topicPartition.partition()+"/"+baseOffset+".index";
        String key = "Backup/"+topicPartition.topic()+"/partition-"+topicPartition.partition()+"/"+baseOffset+".index";

        return storage.downloadFile(key,downloadPath);
    }

    public static void addBaseOffset(TopicPartition tp,long baseOffset){
        SegmentsMapping segmentsMapping = new SegmentsMapping();
        segmentsMapping.addBaseOffset(tp,baseOffset);
    }

    public static long getBaseOffset(TopicPartition tp, long offset){
        SegmentsMapping segmentsMapping = new SegmentsMapping();
        return segmentsMapping.getBaseOffset(tp,offset);
    }

    public static void saveMapping(){
        SegmentsMapping segmentsMapping = new SegmentsMapping();
        segmentsMapping.saveSegmentsMapping();
    }

    public static void initialiseMapping(){
        SegmentsMapping segmentsMapping = new SegmentsMapping();
        segmentsMapping.initialise();
    }
}

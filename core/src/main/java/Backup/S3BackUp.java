package Backup;

import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.Upload;
import kafka.log.LazyIndex;
import kafka.log.LogSegment;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.utils.Time;

import java.io.File;
import java.io.IOException;


public class S3BackUp {

    //first uploading .log and .index file to S3 and then adding the base offset to the mapping.
    public static boolean uploadFile(TopicPartition topicPartition, LogSegment segment, long baseOffset){
        Storage storage = new Storage();

        Upload logUpload = storage.uploadFile(segment.log().file(),"Backup/"+topicPartition.topic()+"/partition-"+topicPartition.partition()+"/"+baseOffset+".log");
        Upload indexUpload = storage.uploadFile(segment.offsetIndex().file(), "Backup/"+topicPartition.topic()+"/partition-"+topicPartition.partition()+"/"+baseOffset+".index");

        if(storage.uploadHandler(logUpload) && storage.uploadHandler(indexUpload)){
            System.out.println("Backup completed for base offset "+baseOffset);
            addBaseOffset(topicPartition,baseOffset);
            return true;
        }
        else {
            return false;
        }
    }

    //creating segment for given topicPartition and baseOffset using log and index file from S3.
    public static LogSegment retrieveSegment(long baseOffset, TopicPartition topicPartition){

        Storage storage = new Storage();

        String logPath = System.getProperty("user.dir")+"/_myFile/"+topicPartition.topic()+"-"+topicPartition.partition()+"/"+baseOffset+".log";
        String indexPath = System.getProperty("user.dir")+"/_myFile/"+topicPartition.topic()+"-"+topicPartition.partition()+"/"+baseOffset+".index";

        String logKey = "Backup/"+topicPartition.topic()+"/partition-"+topicPartition.partition()+"/"+baseOffset+".log";
        String indexKey = "Backup/"+topicPartition.topic()+"/partition-"+topicPartition.partition()+"/"+baseOffset+".index";

        File logFile = new File(logPath);
        File indexFile = new File(indexPath);

        Download logDownload = storage.downloadFile(logKey,logFile);
        Download indexDownload  = storage.downloadFile(indexKey,indexFile);

        if(storage.downloadHandler(logDownload) && storage.downloadHandler(indexDownload)){
            LogSegment segment = null;
            try {
                segment = new LogSegment(FileRecords.open(logFile), LazyIndex.forOffset(indexFile,baseOffset,Config.MAX_INDEX_SIZE,true),null,null,baseOffset,Config.INDEX_INTERVAL_BYTES,Config.ROLL_JITTER_MS, Time.SYSTEM);
                System.out.println("Segment Created for Base offset "+baseOffset);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return segment;
        }
        else {
            return null;
        }
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

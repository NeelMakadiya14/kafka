package Backup;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

import java.io.File;

public class Storage {
    private AmazonS3 client;

    public Storage(){
        AWSCredentials credentials = new BasicAWSCredentials(Config.ACCESS_KEY,Config.SECRET_KEY);

        client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(Config.REGION)
                .build();
    }

    public void uploadFile(File file, String key){
        System.out.println("Uploading.....");
        TransferManager transferManager = TransferManagerBuilder.standard().withS3Client(client).build();
        Upload upload = transferManager.upload(Config.BUCKET,key,file);
        try {
            upload.waitForCompletion();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Upload Done..");
    }

    public File downloadFile(String key, String downloadPath){
        System.out.println("Downloading....");
        File file = new File(downloadPath);
        TransferManager transferManager = TransferManagerBuilder.standard().withS3Client(client).build();
        Download download = transferManager.download(Config.BUCKET,key,file);
        try {
            download.waitForCompletion();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Download Complete..");
        return file;
    }

}

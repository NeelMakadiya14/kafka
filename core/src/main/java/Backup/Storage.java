package Backup;

import com.amazonaws.*;
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

    private static TransferManager transferManager;

    static {
        AWSCredentials credentials = new BasicAWSCredentials(Config.ACCESS_KEY,Config.SECRET_KEY);

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setProtocol(Protocol.HTTPS);
        clientConfiguration.setMaxErrorRetry(15);

        AmazonS3 client= AmazonS3ClientBuilder
                .standard()
                .withClientConfiguration(clientConfiguration)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(Config.REGION)
                .build();

        transferManager = TransferManagerBuilder.standard().withS3Client(client).build();
        System.out.println("Transfer Manager Initialised.");
    }

    public Upload uploadFile(File file, String key){
        System.out.println("Uploading.....");
        Upload upload = null;
        try{
            upload = transferManager.upload(Config.BUCKET,key,file);
        } catch (AmazonClientException e){
            System.out.println("Client Exception in transferManager.upload for "+key);
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        return upload;
    }

    public Download downloadFile(String key, File file){
        System.out.println("Downloading....");

        Download download = null;
        try{
            download = transferManager.download(Config.BUCKET,key,file);
        } catch (AmazonClientException e){
            System.out.println("Client Exception in transferManager.download for "+key);
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

        return download;
    }

    public boolean uploadHandler(Upload upload){
        boolean done=true;
        if(upload!=null){
            try {
                upload.waitForCompletion();
                System.out.println("Upload Complete..");
            } catch (AmazonServiceException e){
                done = false;
                System.out.println("AmazonService Exception : "+e.getErrorMessage());
                e.printStackTrace();
            } catch (AmazonClientException e){
                done = false;
                System.out.println("Client Exception : "+e.getMessage());
                e.printStackTrace();
            } catch (InterruptedException e) {
                done = false;
                System.out.println("Interrupted Exception : "+e.getMessage());
                e.printStackTrace();
            }
            return done;
        }
        else {
            return false;
        }
    }

    public boolean downloadHandler(Download download){
        boolean done=true;
        if(download!=null){
            try {
                download.waitForCompletion();
                System.out.println("Download Complete..");
            } catch (AmazonServiceException e){
                done = false;
                System.out.println("AmazonService Exception : "+e.getErrorMessage());
                e.printStackTrace();
            } catch (AmazonClientException e){
                done = false;
                System.out.println("Client Exception : "+e.getMessage());
                e.printStackTrace();
            } catch (InterruptedException e) {
                done = false;
                System.out.println("Interrupted Exception : "+e.getMessage());
                e.printStackTrace();
            }
            return done;
        }
        else {
            return false;
        }
    }

}

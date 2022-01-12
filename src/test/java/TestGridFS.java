import org.example.mongodb.MongoUtils;
import com.mongodb.Block;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSUploadStream;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;

public class TestGridFS {
    @Test
    public void getFiles(){
        GridFSBucket gridFSBucket = MongoUtils.getGridFSConn();
        gridFSBucket.find().forEach(
                new Block<GridFSFile>() {
                    public void apply(final GridFSFile gridFSFile) {
                        System.out.println(
                                "文件名："+gridFSFile.getFilename()+" "
                                        +"文件大小："+gridFSFile.getLength()+" "
                                        +"文件id:"+gridFSFile.getObjectId().toHexString());
                    }
                });
    }
    @Test
    public void uploadFile(){
        GridFSBucket gridFSBucket = MongoUtils.getGridFSConn();
        try {
            //配置上传文件的参数
            GridFSUploadOptions options = new GridFSUploadOptions()
                    .chunkSizeBytes(358400);//定义块大小
            //创建上传文件流对象，并指定配置参数和文件在GridFS上显示的名称
            GridFSUploadStream uploadStream = gridFSBucket.openUploadStream("Redis.avi", options);
            //一次性读取文件，将文件转为Byte[]包含文件内容的字节数组
            byte[] data = Files.readAllBytes(new File("src\\data\\Redis介绍.mp4").toPath());
            //以字节数组形式上传文件流到GridFS
            uploadStream.write(data);
            //关闭流
            uploadStream.close();
            System.out.println("文件id为: " + uploadStream.getObjectId().toHexString());
        } catch(IOException e){
            // handle exception
        }
    }
    @Test
    public void delFile(){
        GridFSBucket gridFSBucket = MongoUtils.getGridFSConn();
        gridFSBucket.delete(new ObjectId("61a1ca51b1cf8763a033c44a"));
    }
    @Test
    public void downlodFile(){
        GridFSBucket gridFSBucket = MongoUtils.getGridFSConn();
        try {
            //创建文件输出流对象streamToDownloadTo，指定下载的本地路径及文件名
            FileOutputStream streamToDownloadTo =
                    new FileOutputStream("src\\data\\down_Redis.avi");
            /**
             * 通过GridFS中的文件名称下载文件，如果有重名文件则默认下载最新版
             *  GridFSDownloadOptions downloadOptions = new GridFSDownloadOptions().revision(0);
             *  gridFSBucket.downloadToStream("Reids.avi", streamToDownloadTo, downloadOptions);
             */
            //通过GridFS中的文件id以数据流的形式下载文件，并将数据流传给输出流对象streamToDownloadTo
            gridFSBucket.downloadToStream(
                    new ObjectId("61a1ca51b1cf8763a033c44a"), streamToDownloadTo);
            //关闭流
            streamToDownloadTo.close();
        } catch (IOException e) {
            // handle exception
        }
    }
    @Test
    public void renameFile(){
        GridFSBucket gridFSBucket = MongoUtils.getGridFSConn();
        gridFSBucket.rename(
                new ObjectId("61a1ca51b1cf8763a033c44a"),"Redis_new.avi");
    }
}

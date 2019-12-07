package test;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FileTest  extends TestCase {

    @Test
    public void test1(){

        Configuration configuration = new Configuration();
        configuration.addResource("resource/core-site.xml");
        configuration.addResource("resource/hdfs-site.xml");
        Path path = new Path("hdfs://nameservice1/home/");
        try {

            FileSystem srcFs = path.getFileSystem(configuration);

            FileStatus[] fsArray = srcFs.listStatus(path);
            boolean isDirectory = false;
            for (FileStatus f:fsArray) {
                if(f.isDirectory()) {
                    isDirectory = true;
                }
            }

            if(!isDirectory){
                return;
            }

            RemoteIterator iterator = srcFs.listFiles(path,true);
            LocatedFileStatus locatedFileStatus = null;
            FileStatus[] fileStatuses = null;
            int i = -1;

            List<LocatedFileStatus> list = new ArrayList<>();
            while (iterator.hasNext()){
                locatedFileStatus = (LocatedFileStatus)iterator.next();

                System.out.println(locatedFileStatus.getPath());

                list.add(locatedFileStatus);

            }
            fileStatuses = new FileStatus[list.size()];
            for (FileStatus fileStatus: list) {
                fileStatuses[++i] = fileStatus;
            }

           // FileStatus[] fileStatuses = srcFs.listStatus(path);
            FileStatus fileStatus = null;
            for (int j = 0; j <fileStatuses.length ; j++) {
                fileStatus = fileStatuses[j];
                System.out.println(fileStatus.getPath());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

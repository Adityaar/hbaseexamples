/**
 * Created by adityaallamraju on 21/11/17.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class CreateTable {
    public static void main(String args[]) throws IOException {

        Configuration conf = HBaseConfiguration.create();

        //System.out.println(System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
        conf.set("hbase.zookeeper.quorum","10.10.75.107,10.10.75.190,10.10.75.191");
        conf.set("fs.default.name","maprfs:///");
        conf.set("hadoop.spoofed.user.uid","1000");
        conf.set("hadoop.spoofed.user.gid","1000");
        conf.set("hadoop.spoofed.user.username","mapr");
        conf.set("hbase.zookeeper.property.clientPort","5181");
        conf.set("fs.maprfs.impl", "com.mapr.fs.MapRFileSystem");
        conf.set("hadoop.spoofed.user.password","mapr");


        Connection conn = ConnectionFactory.createConnection(conf);

        try {
            Admin admin = conn.getAdmin();

            HTableDescriptor tableName = new HTableDescriptor(TableName.valueOf("/tmp/t1"));
            tableName.addFamily(new HColumnDescriptor("personal"));
            tableName.addFamily(new HColumnDescriptor("professional"));
            tableName.addFamily(new HColumnDescriptor("cf3"));

            if(!admin.tableExists(tableName.getTableName()))
            {
                admin.createTable(tableName);
                System.out.println("table created");
            }
            else
            {
                System.out.println("Table already exists");
            }



        }
        finally {
            conn.close();
        }
    }
}

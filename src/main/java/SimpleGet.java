/**
 * Created by adityaallamraju on 27/11/17.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SimpleGet {

    private static byte[] PERSONAL_CF = Bytes.toBytes("personal");
    private static byte[] PROFESSIONAL_CF = Bytes.toBytes("professional");

    private static byte[] NAME_COLUMN = Bytes.toBytes("name");
    private static byte[] FIELD_COLUMN = Bytes.toBytes("field");

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","10.10.75.107,10.10.75.190,10.10.75.191");
        //conf.set("hadoop.spoofed.user.uid","2147483632");
        //conf.set("hadoop.spoofed.user.gid","2147483632");
        conf.set("hadoop.spoofed.user.username","mapr");
        conf.set("hbase.zookeeper.property.clientPort","5181");
        conf.set("fs.maprfs.impl", "com.mapr.fs.MapRFileSystem");
        //conf.set("fs.default.name","maprfs:///");


        Connection connection = ConnectionFactory.createConnection(conf);

        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf("/tmp/xingdemo"));

            Get get = new Get(Bytes.toBytes("1"));

            get.addColumn(PERSONAL_CF, NAME_COLUMN);
            get.addColumn(PROFESSIONAL_CF, FIELD_COLUMN);

            Result result = table.get(get);

            byte[] nameValue = result.getValue(PERSONAL_CF, NAME_COLUMN);
            System.out.println("Name: " + Bytes.toString(nameValue));

            byte[] fieldValue = result.getValue(PROFESSIONAL_CF, FIELD_COLUMN);
            System.out.println("Field: " + Bytes.toString(fieldValue));

            System.out.println();
            System.out.println("SimpleGet multiple results in one go:");

            List<Get> getList = new ArrayList<Get>();
            Get get1 = new Get(Bytes.toBytes("2"));
            get1.addColumn(PERSONAL_CF, NAME_COLUMN);

            Get get2 = new Get(Bytes.toBytes("3"));
            get1.addColumn(PERSONAL_CF, NAME_COLUMN);

            getList.add(get1);
            getList.add(get2);

            Result[] results = table.get(getList);

            for (Result res : results) {
                nameValue = res.getValue(PERSONAL_CF, NAME_COLUMN);
                System.out.println("Name: " + Bytes.toString(nameValue));
            }
        } finally {
            connection.close();
            if (table != null) {
                table.close();
            }
        }
    }

}

/**
 * Created by adityaallamraju on 23/11/17.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class InsertData {

    private static byte[] PERSONAL_CF = Bytes.toBytes("personal");
    private static byte[] PROFESSIONAL_CF = Bytes.toBytes("professional");

    private static byte[] NAME_COLUMN = Bytes.toBytes("name");
    private static byte[] GENDER_COLUMN = Bytes.toBytes("gender");
    private static byte[] MARITAL_STATUS_COLUMN = Bytes.toBytes("marital_status");

    private static byte[] EMPLOYED_COLUMN = Bytes.toBytes("employed");
    private static byte[] FIELD_COLUMN = Bytes.toBytes("field");

    private String getRandomUUID() {

        UUID uuid = UUID.randomUUID();
        String randomUUIDString = uuid.toString();
        return randomUUIDString;
    }

    //private
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","10.10.75.107,10.10.75.190,10.10.75.191");
        conf.set("hadoop.spoofed.user.uid","1000");
        conf.set("hadoop.spoofed.user.gid","1000");
        conf.set("hadoop.spoofed.user.username","mapr");
        conf.set("hbase.zookeeper.property.clientPort","5181");
        conf.set("fs.maprfs.impl", "com.mapr.fs.MapRFileSystem");
        conf.set("fs.default.name","maprfs:///");


        Connection connection = ConnectionFactory.createConnection(conf);


        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf("/tmp/t1"));

            Put put1 = new Put(Bytes.toBytes("1"));

            put1.addColumn(PERSONAL_CF, NAME_COLUMN, Bytes.toBytes("Mike Jones"));
            put1.addColumn(PERSONAL_CF, GENDER_COLUMN, Bytes.toBytes("male"));
            put1.addColumn(PERSONAL_CF, MARITAL_STATUS_COLUMN, Bytes.toBytes("unmarried"));
            put1.addColumn(PROFESSIONAL_CF, EMPLOYED_COLUMN, Bytes.toBytes("yes"));
            put1.addColumn(PROFESSIONAL_CF, FIELD_COLUMN, Bytes.toBytes("construction"));

            table.put(put1);

            System.out.println("Inserted row for Mike Jones");

            Put put2 = new Put(Bytes.toBytes("2"));

            put2.addColumn(PERSONAL_CF, NAME_COLUMN, Bytes.toBytes("Hillary Clinton"));
            put2.addColumn(PERSONAL_CF, GENDER_COLUMN, Bytes.toBytes("female"));
            put2.addColumn(PERSONAL_CF, MARITAL_STATUS_COLUMN, Bytes.toBytes("married"));
            put2.addColumn(PROFESSIONAL_CF, FIELD_COLUMN, Bytes.toBytes("politics"));

            Put put3 = new Put(Bytes.toBytes("3"));

            put3.addColumn(PERSONAL_CF, NAME_COLUMN, Bytes.toBytes("Donald Trump"));
            put3.addColumn(PERSONAL_CF, GENDER_COLUMN, Bytes.toBytes("male"));
            put3.addColumn(PROFESSIONAL_CF, FIELD_COLUMN, Bytes.toBytes("politics, real estate"));

            Put put4 = new Put(Bytes.toBytes(4));
            short qf1=20;
            short qf2=40;
            String value = "test";
            put4.addColumn(Bytes.toBytes("cf3"), Bytes.add(Bytes.toBytes(qf1), Bytes.toBytes(qf2)), Bytes.toBytes(value));

            List<Put> putList = new ArrayList<Put>();
            putList.add(put2);
            putList.add(put3);
            putList.add(put4);

            table.put(putList);

            System.out.println("Inserted rows for Hillary Clinton and Donald Trump");

        }
        finally {
            connection.close();
            if (table != null) {
                table.close();
            }

        }
    }

}
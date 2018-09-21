/**
 * Created by adityaallamraju on 27/11/17.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
import java.sql.Timestamp;

public class XingInsertData {

    private static byte[] MESSAGE_CF = Bytes.toBytes("message");
    private static byte[] PAYLOAD_CF = Bytes.toBytes("payload");
    private static byte[] METADATA_CF = Bytes.toBytes("metadata");

    private static byte[] MESSAGE_ID_COLUMN = Bytes.toBytes("message_id");
    private static byte[] MESSAGE_DATA_COLUMN = Bytes.toBytes("message");
    private static byte[] MESSAGE_ARRIVAL_COLUMN = Bytes.toBytes("arrival_time");

    private static byte[] PAYLOAD_ID_COLUMN = Bytes.toBytes("payload_id");
    private static byte[] DATA_ID = Bytes.toBytes("data_id");

    private static byte[] METADATA_COLUMN = Bytes.toBytes("metadata");

    private static String[] eventTypes = {"impression","delivery","email_open","click","follow","open"};
    private static String[] targetTypes = {"article","klarticle","container","email","page"};

    private static long getRandomTimestamp() {

        //get random timestamp in a certain year
        long start = Timestamp.valueOf("2017-01-01 00:00:00").getTime();
        long end = Timestamp.valueOf("2018-01-01 00:00:00").getTime();
        long diff = end - start;
        long randTimestamp = start + (long) (Math.random() * diff);
        return randTimestamp;
    }

    private static String getRandomEvent() {
        //get random event type
        int idx = new Random().nextInt(eventTypes.length);
        String randomEvent = eventTypes[idx];
        return randomEvent;
    }
    private static String getRandomTarget() {
        //get random target type
        int idx = new Random().nextInt(targetTypes.length);
        String randomTarget = targetTypes[idx];
        return randomTarget;
    }
    private static String getRowKey() {
        //get UUID
        String randomUUID = UUID.randomUUID().toString();
        return  "_" + getRandomEvent() +"_" + getRandomTarget() + "_" + randomUUID;
    }

    public static void main(String[] args) throws  IOException {

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

            table = connection.getTable(TableName.valueOf("/tmp/xingdemo"));

            List<Put> putList = new ArrayList<Put>();

            for (int i = 0; i < 10000; i++) {

                String rowK = getRowKey();
                //System.out.println("Current rowKey " + String.valueOf(i) + ": " +rowK);
                byte[] rowKey = Bytes.add(Bytes.toBytes(getRandomTimestamp()),Bytes.toBytes(rowK));

                Put put1 = new Put(rowKey);

                //convert_from(t.message.message_id,'INT_BE')
                put1.addColumn(MESSAGE_CF, MESSAGE_ID_COLUMN, Bytes.toBytes((int)Math.ceil(Math.random() * 10000)));
                //convert_from(t.message.message,'UTF8')
                put1.addColumn(MESSAGE_CF, MESSAGE_DATA_COLUMN, Bytes.toBytes(UUID.randomUUID().toString().replace("-", "")));
                put1.addColumn(MESSAGE_CF, MESSAGE_ARRIVAL_COLUMN, Bytes.toBytes(getRandomTimestamp()));

                //unknown--?
                put1.addColumn(PAYLOAD_CF, PAYLOAD_ID_COLUMN, Bytes.toBytes(Math.ceil(Math.random() * 10000000)));

                //convert_from(t.payload.data_id,'INT_BE')
                //put1.addColumn(PAYLOAD_CF, DATA_ID, Bytes.toBytes((new Random().nextInt(10001) + 1000) ));
                put1.addColumn(PAYLOAD_CF, DATA_ID, Bytes.toBytes(String.valueOf(new Random().nextInt(10001) + 1000)));
                //UTF8
                put1.addColumn(METADATA_CF, METADATA_COLUMN, Bytes.toBytes(UUID.randomUUID().toString().replace("-", "")));

                putList.add(put1);

            }

            table.put(putList);
        }

        finally {
            connection.close();
            if (table != null) {
                table.close();
            }
        }

    }

}

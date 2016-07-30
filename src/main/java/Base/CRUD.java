package Base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

import static org.apache.hadoop.hbase.TableName.valueOf;

public class CRUD {

	// 声明静态配置

    private static Configuration conf = HBaseConfiguration.create();
    private static Admin admin;
    private static Connection conn;
	private static final String TABLE_NAME; //表名
    private static final String[] COL_FAMILY = { "info", "count" };//列族

    static {
        conf.set("hbase.zookeeper.quorum", "cmagent1");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        System.setProperty("hadoop.home.dir", "C:\\src\\hadoop-2.6.4");

        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
        TABLE_NAME = "batch_content";
    }
    /**
     *
     * 创建表与列族
     *
     * boolean FALTRU :true 代表如果表存在直接退出
     *               ：false 代表如果表存在先删除表再退出
     * createOrOverwrite(admin,new HTableDescriptor(TableName.valueOf(TABLE_NAME)),COL_FAMILY,true);
     *
     * */
    private static void createOrOverwrite(Admin admin, HTableDescriptor table, String[] COL_FAMILY,boolean FALTRU) throws IOException {

        for (String aCOL_FAMILY : COL_FAMILY) {
            table.addFamily(new HColumnDescriptor(aCOL_FAMILY).setCompressionType(Algorithm.GZ));
        }
       //table.addFamily(new HColumnDescriptor("info").setCompressionType(Algorithm.GZ));

        if (admin.tableExists(table.getTableName())) {
            if (FALTRU)
            {
                //admin.disableTable(valueOf(TABLE_NAME));
                //admin.deleteTable(valueOf(TABLE_NAME));
                System.out.println(TABLE_NAME + ": delete Success!");
                admin.createTable(table);
                System.out.println(TABLE_NAME + ": create Success!");
            }else {
                System.out.println(TABLE_NAME + ": table Exists!");
                //System.exit(0);
            }
        } else {
            admin.createTable(table);
            System.out.println(TABLE_NAME+": create Success!");
        }
    }


    /*
     * 为表添加数据（适合知道有多少列族的固定表）
     *
     * @rowKey rowKey
     *
     * @tableName 表名
     *
     * @column 列的列表
     *
     * @value 列的值的列表
     */
    private static void addData(String rowKey, String tableName,
                                String[] column, String[] value, String famliyName)
            throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
        //HTable table = new HTable(valueOf(tableName));//old api
        Table table = conn.getTable(TableName.valueOf(tableName));// HTabel负责跟记录相关的操作如增删改查等

        if (!(column.length == value.length)) {
            System.out.println("数组column和value长度不同会导致异常，可以补全null值使之长度相等");
            System.exit(0);
        } else {
            for (int j = 0; j < column.length; j++) {
                put.addColumn(Bytes.toBytes(famliyName), Bytes.toBytes(column[j]), Bytes.toBytes(value[j]));
            }
        }

        table.put(put);
        System.out.println("add data Success!");
    }


    private static HTableDescriptor queryTableFamily(String tableName) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));

        return table.getTableDescriptor();
    }

    /*
 * 根据rwokey查询
 *
 * @rowKey rowKey
 *
 * @tableName 表名
 */
    public static Result getResult(String tableName, String rowKey)
            throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        Table table = conn.getTable(TableName.valueOf(tableName)); // 获取表
        Result result = table.get(get);
        System.out.println("result: "+result);

        for (KeyValue kv : result.list()) {
            if (Bytes.toString(kv.getQualifier()) != null) {
                System.out.println("query result..........................");
                System.out.println("family:" + Bytes.toString(kv.getFamily()));
                System.out.println("colnum:" + Bytes.toString(kv.getQualifier()));
                System.out.println("value:" + Bytes.toString(kv.getValue()));
                System.out.println("Timestamp:" + kv.getTimestamp());
                System.out.println("-------------------------------------------");
            }
        }

        return result;
    }

    public static void main(String[] args) throws IOException {
        //创建表与列族
		//HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
		createOrOverwrite(admin,new HTableDescriptor(valueOf(TABLE_NAME)),COL_FAMILY,false);

        //插入数据
        String rockey = "000anyan000-Tl8xp1";
        String[] colnum = {"id"};
        String[] value = {"who are you"};
        String INFO = "info";
        //addData(rockey,TABLE_NAME,colnum,value,INFO);

        //String rockey = "000area-KgW7mg";
        //查看所以列族
        queryTableFamily(TABLE_NAME);

        //通过rowkey查询
        getResult(TABLE_NAME,rockey);
	}
}
package cn.siat.anfang;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBASEAPI {

	/**
	 * 创建表操作
	 * 
	 * @throws IOException
	 */
	public static void createTable(String tablename, String[] cfs)
			throws IOException {
		HBaseAdmin admin = new HBaseAdmin(Const.conf);
		if (admin.tableExists(tablename)) {
			System.out.println("表已经存在！");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tablename);
			for (int i = 0; i < cfs.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(cfs[i]));
			}
			admin.createTable(tableDesc);
			System.out.println("表创建成功！");
		}
	}

	/**
	 * 删除表操作
	 * 
	 * @param tablename
	 * @throws IOException
	 */
	public static void deleteTable(String tablename) throws IOException {
		try {
			HBaseAdmin admin = new HBaseAdmin(Const.conf);
			admin.disableTable(tablename);
			admin.deleteTable(tablename);
			System.out.println("表删除成功！");
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 插入一行记录
	 * 
	 * @param tablename
	 * @param cfs
	 */
	public static void writeRow(String tablename,String rowkey, Map<String, Object> map) {
		try {
			HTable table = new HTable(Const.conf, tablename);
			Put put = new Put(Bytes.toBytes(rowkey));
			Iterator iterator = map.keySet().iterator();
			while(iterator.hasNext()){
				String keyString = (String) iterator.next();
				String[] tmp=keyString.split(":");
				put.add(Bytes.toBytes(tmp[0]),
						Bytes.toBytes(tmp[1]),
						Bytes.toBytes(map.get(keyString).toString()));
				table.put(put);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 删除一行记录
	 * 
	 * @param tablename
	 * @param rowkey
	 * @throws IOException
	 */
	public static void deleteRow(String tablename, String rowkey)
			throws IOException {
		HTable table = new HTable(Const.conf, tablename);
		List list = new ArrayList();
		Delete d1 = new Delete(rowkey.getBytes());
		list.add(d1);
		table.delete(list);
		System.out.println("删除行成功！");
	}

	/**
	 * 查找一行记录
	 * 
	 * @param tablename
	 * @param rowkey
	 * @return
	 * @throws IOException 
	 */
	public static Result selectRow(String tablename, String rowKey) throws IOException {
		HTable table = new HTable(Const.conf, tablename);
		Get g = new Get(rowKey.getBytes());
		Result rs = table.get(g);
		return rs;
	}

	/**
	 * 查询表中所有行
	 * 
	 * @param tablename
	 * @throws IOException 
	 */
	public static ResultScanner scaner(String tablename) throws IOException {
		HTable table = new HTable(Const.conf, tablename);
		Scan s = new Scan();
		ResultScanner rs = table.getScanner(s);
		return rs;

	}
}

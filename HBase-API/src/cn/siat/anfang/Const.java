package cn.siat.anfang;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class Const {
	private static String pathString="/HBase-API/src/";
	static Configuration conf = null;
	static {
		conf = HBaseConfiguration.create();
		conf.addResource(new Path(pathString+"core_site.xml"));
		conf.addResource(new Path(pathString+"hbase-site.xml"));
		conf.addResource(new Path(pathString+"hdfs-site.xml"));
		conf.addResource(new Path(pathString+"mapred-site.xml"));
		conf.addResource(new Path(pathString+"yarn-site.xml"));
	}
	public static final String HOST = "172.21.5.38";
	public static final int VIDEO = 0;
	public static final int IMAGE = 1;
	public static final int FACE = 2;
	
	private static final String PATH = "hdfs://"+HOST+":9000";
	public static final String PATH_VIDEO = PATH+"/video";
	public static final String PATH_IMAGE = PATH+"/image";
	public static final String PATH_FACE = PATH+"/face";
}

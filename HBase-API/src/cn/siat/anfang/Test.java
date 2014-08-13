package cn.siat.anfang;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class Test {
	public static void main(String[] args)
	{
		//HBASEAPI.createTable("face", new String[]{"info","desc"});
/*		Map<String, Object> map = new HashMap<String, Object>();
		map.put("info:video_path", "C:/");
		HBASEAPI.writeRow("video", "video_123_123",map);*/
/*		try {
			Result res=HBASEAPI.selectRow("video", "video_123_123");
			System.out.println(Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("video_path"))));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		File file = new File("C:\\Users\\Public\\Pictures\\Sample Pictures\\Lighthouse.jpg");
		try {
			FileInputStream fileInputStream = new FileInputStream(file);
			HDFSAPI.uploadToHdfs(fileInputStream, Const.IMAGE);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}

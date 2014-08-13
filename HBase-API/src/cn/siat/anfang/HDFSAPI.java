package cn.siat.anfang;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Date;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class HDFSAPI {
	static FileSystem hdfs;
	static {
		try {
			hdfs = FileSystem.get(Const.conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// create a direction
	public static void createDir(String dir) throws IOException {
		Path path = new Path(dir);
		hdfs.mkdirs(path);
		System.out.println("new dir \t" + Const.conf.get("fs.default.name")
				+ dir);
	}

	// copy from local file to HDFS file
	public static void copyFile(String localSrc, String hdfsDst) throws IOException {
		Path src = new Path(localSrc);
		Path dst = new Path(hdfsDst);
		hdfs.copyFromLocalFile(src, dst);

		// list all the files in the current direction
		FileStatus files[] = hdfs.listStatus(dst);
		System.out.println("Upload to \t" + Const.conf.get("fs.default.name")
				+ hdfsDst);
		for (FileStatus file : files) {
			System.out.println(file.getPath());
		}
	}

	// create a new file
	public static void createFile(String fileName, String fileContent)
			throws IOException {
		Path dst = new Path(fileName);
		byte[] bytes = fileContent.getBytes();
		FSDataOutputStream output = hdfs.create(dst);
		output.write(bytes);
		System.out.println("new file \t" + Const.conf.get("fs.default.name")
				+ fileName);
	}

	// list all files
	public static void listFiles(String dirName) throws IOException {
		Path f = new Path(dirName);
		FileStatus[] status = hdfs.listStatus(f);
		System.out.println(dirName + " has all files:");
		for (int i = 0; i < status.length; i++) {
			System.out.println(status[i].getPath().toString());
		}
	}

	// judge a file existed? and delete it!
	public static void deleteFile(String fileName) throws IOException {
		Path f = new Path(fileName);
		boolean isExists = hdfs.exists(f);
		if (isExists) { // if exists, delete
			boolean isDel = hdfs.delete(f, true);
			System.out.println(fileName + "  delete? \t" + isDel);
		} else {
			System.out.println(fileName + "  exist? \t" + isExists);
		}
	}

	public static String uploadToHdfs(InputStream in, int type) {
		Configuration conf = Const.conf;
		String dst = null;
		Date date = new Date();
		Random random = new Random();
		try {
			switch (type) {
			case Const.FACE:
				dst = Const.PATH_FACE + "/face_" + date.getTime() + "_"
						+ random.nextInt(10000);
				break;
			case Const.IMAGE:
				dst = Const.PATH_FACE + "/image_" + date.getTime() + "_"
						+ random.nextInt(10000);
				break;
			case Const.VIDEO:
				dst = Const.PATH_FACE + "/video_" + date.getTime() + "_"
						+ random.nextInt(10000);
				break;

			default:
				break;
			}
			FileSystem fs = FileSystem.get(URI.create(dst), conf);
			OutputStream out = fs.create(new Path(dst));
			IOUtils.copyBytes(in, out, 4096, true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return dst;
	}

	public static FSDataInputStream readFromHdfs(String path) {
		Configuration conf = Const.conf;
		try {
			FileSystem fs=FileSystem.get(URI.create(path),conf);
			FSDataInputStream hdfsInputStream = fs.open(new Path(path));
			return hdfsInputStream;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
		
	}
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.core;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Progressable;

/**
 *
 * @author jpopaholic
 */
public class HdfsFileSystem extends FileSystem {

  private final FileSystem hdfs;

  public HdfsFileSystem(Configuration conf) throws IOException {
    super();
    hdfs = FileSystem.get(conf);
  }

  public HdfsFileSystem() throws IOException {
    super();
    System.err.println("to get hdfs");
    hdfs = FileSystem.get(HBaseConfiguration.create());
  }

//    @Override
//    public Configuration getConf() {
//        return hdfs.getConf();
//    }
  @Override
  public URI getUri() {
    return hdfs.getUri();
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return hdfs.open(f, bufferSize);
  }

  @Override
  public FSDataOutputStream create(Path f) throws IOException {
    return hdfs.create(f);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    return hdfs.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    return hdfs.append(f, bufferSize, progress);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return hdfs.rename(src, dst);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return hdfs.delete(f, recursive);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    return hdfs.listStatus(f);
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    hdfs.setWorkingDirectory(new_dir);
  }

  @Override
  public Path getWorkingDirectory() {
    return hdfs.getWorkingDirectory();
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return hdfs.mkdirs(f, permission);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return hdfs.getFileStatus(f);
  }

  @Override
  public void close() throws IOException {
    //do nothing
  }

}

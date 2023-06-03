package umc.cdc.hds.fs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.core.Protocol;
import umc.cdc.hds.datastorage.HdsDataStorage;
import umc.cdc.hds.uri.Query;
import umc.cdc.hds.uri.UriParser;
import umc.cdc.hds.uri.UriRequest;

public class HDSFileSystem extends FileSystem {

  private URI uri;
  private Path workingDir;
  private static final Log LOG = LogFactory.getLog(HDSFileSystem.class);
  private String namespace;
  private static HdsDataStorage ds;
  private String hdsHostNameWithPort = "192.168.103.53:8000";
  private Configuration c;
  private long blocksize;
  private boolean isUriWithAuthority;

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    LogManager.getRootLogger().setLevel(Level.WARN);
    super.initialize(name, conf);
    setConf(conf);
    if (name.getAuthority() != null) {
      uri = URI.create(name.getScheme() + "://" + name.getAuthority());
      isUriWithAuthority = true;
    } else {
      uri = URI.create(name.getScheme() + ":///");
      isUriWithAuthority = false;
    }
    if (conf.get("fs.hdfs.address") == null) {
      c = conf;
    } else {
      c = new Configuration(false);
      c.set("fs.defaultFS", conf.get("fs.hdfs.address"));
      c.addResource(conf);
    }
    blocksize = conf.getLong("dfs.blocksize", 134217728);
    hdsHostNameWithPort = conf.get("hds.host.address", "hds2:7999");
    namespace = conf.get(
            HDSConstants.HBASE_NAMESPACE_STRING,
            HDSConstants.DEFAULT_HBASE_NAMESAPCE_STRING);
    workingDir = new Path(getScheme() + ":///");
    ds = getHdsDataStorage();
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public FSDataInputStream open(Path path, int i) throws IOException {
    path = checkPathIsAbsolute(path);
    UriRequest req = UriParser.valueOf(createListStatusUrl(path, false), null);
    return new FSDataInputStream(new HDSInputStream(getHdsDataStorage(), req));
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite,
          int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    System.out.println("[HDSFileSystem] create. f = " + f.toString());
    f = checkPathIsAbsolute(f);
    if (!overwrite && exists(f)) {
      throw new IOException(f + " already exists");
    }
    UriRequest req = UriParser.valueOf(createListStatusUrl(f, false), null);
    return new FSDataOutputStream(new HDSOutputStream(getHdsDataStorage(), req), null);
  }

  @Override
  public FSDataOutputStream append(Path path, int i, Progressable p) throws IOException {
    throw new IOException("Not supported");
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    System.out.println("[HDSFileSystem] rename. src = " + src.toString() + ". dst = " + dst.toString());
    src = checkPathIsAbsolute(src);
    dst = checkPathIsAbsolute(dst);
    FileStatus s;
    try {
      s = getFileStatus(src);
    } catch (FileNotFoundException ex) {
      System.out.println("[HDSFileSysten::rename] getFileStatus(src) threw exception");
      return false;
    }
    try {
      getFileStatus(dst);
      System.out.println("[HDSFileSysten::rename] getFileStatus(dst) found file exists");
      return false;
    } catch (FileNotFoundException e) {
      // do nothing
    }

    if (s.isFile()) {
      System.out.println("[HDSFileSysten::rename] s.isFile() is true");
      if (((HDSFileStatus) s).getLocation().equals(Protocol.hbase)) {
        System.out.println("[HDSFileSystem::rename] sending HdsAccessUrl = " + createHdsAccessUrl(src, dst).toString());
        sendHdsRequest(createHdsAccessUrl(src, dst));
        return delete(src, false);
      } else {
        return getHdsDataStorage().renameHdfsFile(src, dst);
      }
    } else {
      System.out.println("[HDSFileSysten::rename] s.isFile() is false");
      List<FileStatus> list = new ArrayList<>();
      listRecursive(list, src);
      getHdsDataStorage().mkdirs(dst);
      for (FileStatus f : list.toArray(new FileStatus[list.size()])) {
        if (f.isDirectory()) {
          mkdirs(new Path(f.getPath().toString().replaceFirst(src.toUri().getPath(), dst.toUri().getPath())), null);
        } else {
          if (((HDSFileStatus) f).getLocation().equals(Protocol.hbase)) {
            sendHdsRequest(createHdsAccessUrl(f.getPath(), new Path(f.getPath().toString().replaceFirst(src.toUri().getPath(), dst.toUri().getPath()))));
          } else {
            if (!getHdsDataStorage().renameHdfsFile(f.getPath(), new Path(f.getPath().toString().replaceFirst(src.toUri().getPath(), dst.toUri().getPath())))) {
              return false;
            }
          }
        }
      }
      return delete(src, true);
    }
  }

  private List<FileStatus> listRecursive(List<FileStatus> list, Path path) throws IOException {
    for (FileStatus f : listStatus(path)) {
      list.add(f);
      if (f.isDirectory()) {
        listRecursive(list, f.getPath());
      }
    }
    return list;
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    path = checkPathIsAbsolute(path);
    FileStatus s;
    try {
      s = getFileStatus(path);
    } catch (FileNotFoundException ex) {
      return false;
    }
    if (s.isFile()) {
      sendHdsRequest(createHdsDeleteUrl(createListStatusUrl(path, false), recursive));
    } else if (s.isDirectory()) {
      if (!recursive) {
        throw new IOException("Path is a folder: " + path);
      }
      sendHdsRequest(createHdsDeleteUrl(createListStatusUrl(path, true), recursive));
    }
    return true;
  }

  @Override
  public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
    path = checkPathIsAbsolute(path);
    if (getPathDepthWithoutAuthority(path) == 0) {
      throw new IOException("Illegal Hds Uri forrmat.");
    }
    FileStatus s = getFileStatus(path);
    if (s.isFile()) {
      return new FileStatus[]{s};
    }
    UriRequest req = UriParser.valueOf(createListStatusUrl(path, true), null);
    Query q = new Query();
    q.put("limit", "-1");
    return HDSFileStatus.getFileStatus(getHdsDataStorage().list(req, q), blocksize);
  }

  @Override
  public void setWorkingDirectory(Path path) {
    workingDir = path;
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public boolean mkdirs(Path path, FsPermission fp) throws IOException {
    path = checkPathIsAbsolute(path);
    getHdsDataStorage().mkdirs(path);
    return true;
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    path = checkPathIsAbsolute(path);
    int depth = getPathDepthWithoutAuthority(path);
    if (depth == 1) {
      if (getHdsDataStorage().checkTableExisted(TableName.valueOf(namespace, path.getName()), false)) {
        if (isUriWithAuthority) {
          return new HDSFileStatus(0L, new Path(uri.toString().concat("/").concat(path.getName())));
        }
        return new HDSFileStatus(0L, path);
      } else {
        throw new FileNotFoundException("No such file or directory: " + path.toUri().toString());
      }
    } else if (depth == 0) {
      if (isUriWithAuthority) {
        return new HDSFileStatus(0L, new Path(uri));
      }
      return new HDSFileStatus(0L, path);
    }
    FileStatus[] status;
    try {
      UriRequest req = UriParser.valueOf(createGetFileStatusUrl(path, true), null);
      status = HDSFileStatus.getFileStatus(getHdsDataStorage().list(req, new Query()), blocksize);
      if (status.length > 0) {
        return status[0];
      }
    } catch (IOException ex) {
      // do nothing
    }
    throw new FileNotFoundException("No such file or directory: " + path.toUri().toString());

  }

  @Override
  public String getScheme() {
    return "hds";
  }

  @Override
  public String getCanonicalServiceName() {
    // Does not support Token
    return null;
  }

  private String createListStatusUrl(Path path, boolean isDirectory) {
    if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
      return "";
    }
    String url = getScheme() + "://" + path.toUri().getPath();
    if (isDirectory) {
      url = url + "/";
    }
    url = url + "?ver=v2&directory=true";
    return url;
  }

  private String createGetFileStatusUrl(Path path, boolean isDirectory) throws UnsupportedEncodingException {
    if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
      return "";
    }
    String url = getScheme() + "://" + path.getParent().toUri().getPath() + "?ver=v2&name=" + URLEncoder.encode(path.getName(), HDSConstants.DEFAULT_CHAR_ENCODING);
    if (isDirectory) {
      url = url + "&directory=true";
    }
    return url;
  }

  private String sendHdsRequest(URL accessUrl) throws IOException {
    HttpURLConnection conn = null;
    StringBuilder sb = new StringBuilder();
    try {
      conn = (HttpURLConnection) accessUrl.openConnection();
      try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
        String line;
        while ((line = br.readLine()) != null) {
          sb.append(line).append("\n");
        }
      }
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
    if (isUriWithAuthority) {
      return sb.toString().replaceAll("hds://", uri.toString());
    }
    return sb.toString();
  }

  private URL createHdsAccessUrl(Path from, Path to) throws MalformedURLException, UnsupportedEncodingException {
    String url = "http://" + hdsHostNameWithPort + "/dataservice/v2/access?from=" + URLEncoder.encode(from.toUri().getScheme() + "://" + from.toUri().getPath(), HDSConstants.DEFAULT_CHAR_ENCODING) + "&" + HDSConstants.URL_ARG_TO + "=" + URLEncoder.encode(to.toUri().getScheme() + "://" + to.toUri().getPath(), HDSConstants.DEFAULT_CHAR_ENCODING);
    return new URL(url);
  }

  private URL createHdsDeleteUrl(String from, boolean recursive) throws MalformedURLException, UnsupportedEncodingException {
    String url = "http://" + hdsHostNameWithPort + "/dataservice/v2/delete?from=" + URLEncoder.encode(from, HDSConstants.DEFAULT_CHAR_ENCODING);
    if (recursive) {
      url = url.concat("&recursive=true");
    }
    return new URL(url);
  }

  private Path checkPathIsAbsolute(Path path) {
    if (!path.isAbsolute()) {
      return new Path(workingDir, path);
    } else if (path.toUri().getAuthority() == null) {
      return path;
    } else {
      return new Path(workingDir, path.toUri().getPath());
    }
  }

  private int getPathDepthWithoutAuthority(Path path) {
    if (path.toUri().getAuthority() != null) {
      return path.depth() - 1;
    }
    return path.depth();
  }

  private synchronized HdsDataStorage getHdsDataStorage() throws IOException {
    if (ds == null) {
      ds = new HdsDataStorage(c);
    }
    return ds;
  }
}

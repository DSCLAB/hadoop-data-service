/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.uri;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.core.Protocol;
import umc.cdc.hds.exceptions.IllegalUriException;
import umc.cdc.hds.mapping.AccountInfo;
import umc.cdc.hds.mapping.AccountInfoBuilder;
import umc.cdc.hds.mapping.Mapping;

/**
 *
 * @author jpopaholic
 */
public abstract class UriParser {

  private static final Log LOG = LogFactory.getLog(UriParser.class);

  private static class CommonUriRequest implements UriRequest {

    private Protocol p;
    private AccountInfo acc;
    private Optional<String> name;
    private String dir;
    private final Optional<String> id;
    private Query q;
    private final String path;

    CommonUriRequest(final String rawUri, final Mapping mapping) throws IOException {
      InnerUriParser innParser = new InnerUriParser(rawUri);
      path = innParser.getPath();
      p = innParser.getProtocol();
      q = innParser.getQuery();
      id = innParser.getDataStorageId();
      acc = id.isPresent() ? mapping.find(id.get()).orElseThrow(()
          -> new IllegalUriException("Cannot find \"" + id.get() + "\" from mapping table"))
          : innParser.getAccountInfoBuilder().build();
      dir = FilenameUtils.getFullPath(path);
      name = parseName(p, q);
    }

    CommonUriRequest(final UriRequest uri) throws IOException {
      path = uri.getPath();
      p = uri.getProtocol();
      q = new Query(uri.getQuery().getQueries());
      id = uri.getStorageId();
      if (uri.getAccountInfo().isPresent()) {
        AccountInfo oldInfo = uri.getAccountInfo().get();
        AccountInfoBuilder accBuilder = new AccountInfoBuilder();
        accBuilder.setDomain(oldInfo.getDomain().orElse(null));
        accBuilder.setHost(oldInfo.getHost().orElse(null));
        accBuilder.setId(oldInfo.getId().orElse(null));
        accBuilder.setPasswd(oldInfo.getPasswd().orElse(null));
        accBuilder.setPort(oldInfo.getPort().orElse(null));
        accBuilder.setUser(oldInfo.getUser().orElse(null));
        acc = accBuilder.build();
      }
      dir = uri.getDir();
      name = uri.getName();
    }

    @Override
    public Protocol getProtocol() {
      return p;
    }

    @Override
    public Optional<AccountInfo> getAccountInfo() throws IOException {
      return Optional.ofNullable(acc);
    }

    @Override
    public String getDir() {
      return dir;
    }

    @Override
    public Optional<String> getName() {
      if (p.equals(Protocol.hds)) {
        return q.getQueryValue(HDSConstants.HDS_URI_ARG_NAME);
      } else {
        return name;
      }
    }

    @Override
    public String getPath() {
      if (p.equals(Protocol.hds)) {
        return path;
      } else if (name.isPresent()) {
        return dir + name.get();
      } else {
        return dir;
      }
    }

    @Override
    public Query getQuery() {
      return q;
    }

    @Override
    public Optional<String> getStorageId() {
      return id;
    }

    @Override
    public void setProtocol(Protocol p) {
      this.p = p;
    }

    @Override
    public void setAccountInfo(AccountInfo a) {
      this.acc = a;
    }

    @Override
    public void setDir(String dir) {
      this.dir = dir;
    }

    @Override
    public void setName(String name) {
      if (p.equals(Protocol.hds)) {
        this.q.getQueries().put(HDSConstants.HDS_URI_ARG_NAME, name);
      } else {
        this.name = Optional.ofNullable(name);
      }
    }

    @Override
    public void setQuery(Query q) {
      this.q = q;
    }

    @Override
    public String toRealUri() {
      try {
        return toUri(false);
      } catch (IOException ex) {
        LOG.error(ex);
        return "";
      }
    }

    @Override
    public String toString() {
      try {
        return toUri(true);
      } catch (IOException ex) {
        LOG.error(ex);
        return "";
      }
    }

    @Override
    public void setFileNameToDirName() {
      if (this.getName().isPresent()) {
        StringBuilder dirName = new StringBuilder();
        dirName.append(this.getDir()).append(this.getName().get()).append("/");
        this.setDir(dirName.toString());
        this.setName(null);
      }
    }

    private Optional<String> parseName(Protocol p, Query q) {
      if (p.equals(Protocol.hds)) {
        return q.getQueryValue(HDSConstants.HDS_URI_ARG_NAME);
      } else {
        return FilenameUtils.getName(path).isEmpty()
            ? Optional.empty()
            : Optional.ofNullable(FilenameUtils.getName(path));
      }
    }

    private String toUri(boolean usingAlias) throws IOException {
      StringBuilder url = new StringBuilder();
      url.append(getProtocol().name()).append("://");
      if (getProtocol().equals(Protocol.jdbc)) {
        url.append(
            URLEncoder.encode(getQuery().getQueryValue(HDSConstants.JDBC_INFO_QUERY)
                .orElse(""), HDSConstants.DEFAULT_CHAR_ENCODING)
        );
        return url.toString();

      }
      if (getAccountInfo().isPresent()) {
        if (usingAlias) {
          url.append(getAccountInfo().get().toString());
        } else {
          url.append(getAccountInfo().get().toRealString());
        }
      }
      url.append(getPath());
      return url.toString();
    }

    @Override
    public void clearName() {
      this.name = Optional.empty();
    }
  }

  public static UriRequest valueOf(final String rawUri, final Mapping mapping) throws IOException {
    return new CommonUriRequest(rawUri, mapping);
  }

  public static UriRequest valueOf(final UriRequest uri) throws IOException {
    return new CommonUriRequest(uri);
  }

  private static class InnerUriParser {

    private Protocol protocol;
    private String path;
    private Optional<String> dsId;
    private Query queries;
    private AccountInfoBuilder builder;

    public InnerUriParser(String rawUri) throws IOException {
      parse(rawUri);
    }

    private void parse(String rawUri) throws IOException {
      builder = new AccountInfoBuilder();
      String partAuth = parseSchema(rawUri);
      parsePath(parseAuth(partAuth));
    }

    private String parseSchema(String raw) throws IOException {
      if (!raw.contains("://")) {
        throw new IllegalUriException(HDSConstants.INCORRECT_SCHEME);
      }
      int wall = raw.indexOf("://");
      String[] schContext = raw.split("://");
      if (schContext.length < 2) {
        throw new IllegalUriException(HDSConstants.INCORRECT_SCHEME);
      }
      if (schContext[0].length() <= 0) {
        throw new IllegalUriException(HDSConstants.INCORRECT_SCHEME);
      }
      Optional<Protocol> p = Protocol.find(schContext[0]);
      if (!p.isPresent()) {
        throw new IllegalUriException(HDSConstants.INCORRECT_SCHEME);
      } else {
        protocol = p.get();
      }
      return raw.substring(wall + 3);
    }

    private String parseAuth(String nonSchema) throws IOException {
      if (!nonSchema.contains("/")) {
        throw new IllegalUriException(HDSConstants.INCORRECT_SCHEME);
      }
      int splitIndex = nonSchema.indexOf("/");
      if (splitIndex < 0) {
        throw new IllegalUriException(HDSConstants.INCORRECT_SCHEME);
      }
      String authString = nonSchema.substring(0, splitIndex);
      parseDSId(authString);
      parseHost(parseUserInfo(authString));
      return nonSchema.substring(splitIndex);
    }

    private void parseDSId(String auth) throws IOException {
      if (auth.startsWith("$")) {
        if (auth.length() <= 1) {
          throw new IllegalUriException(HDSConstants.INCORRECT_MAPPING);
        }
        dsId = Optional.of(auth.substring(1));
      } else {
        dsId = Optional.empty();
      }
    }

    private String parseUserInfo(String authString) throws IOException {
      if (authString.isEmpty()) {
        return authString;
      }
      if (authString.contains("@")) {
        String[] infoHost = authString.split("@");
        if (infoHost.length != 2) {
          throw new IllegalUriException(HDSConstants.INCORRECT_SCHEME);
        }
        parseToAuth(infoHost[0]);
        return infoHost[1];
      }
      return authString;
    }

    private void parseToAuth(String userInfo) throws IOException {
      if (userInfo.contains(";") || userInfo.contains("\\")) {
        parseDomain(userInfo);
      } else {
        parseUserPasswd(userInfo);
      }
    }

    private void parseDomain(String userInfo) throws IllegalUriException, UnsupportedEncodingException {
      String domain = null;
      if (userInfo.contains(";")) {
        String[] domainUserInfo = userInfo.split(";");
        if (domainUserInfo.length != 2) {
          throw new IllegalUriException(HDSConstants.INCORRECT_SCHEME);
        }
        if (domainUserInfo[0] != null
            || !domainUserInfo[0].isEmpty()) {
          domain = domainUserInfo[0];
        }
        parseUserPasswd(domainUserInfo[1]);
      } else if (userInfo.contains("\\")) {
        String[] domainUserInfo = userInfo.split("\\\\");
        if (domainUserInfo.length != 2) {
          throw new IllegalUriException(HDSConstants.INCORRECT_SCHEME);
        }
        if (domainUserInfo[0] != null
            || !domainUserInfo[0].isEmpty()) {
          domain = domainUserInfo[0];
        }
        parseUserPasswd(domainUserInfo[1]);
      } else {
        parseUserPasswd(userInfo);
      }
      builder.setDomain(domain);
    }

    private void parseUserPasswd(String userInfo) throws IllegalUriException, UnsupportedEncodingException {
      String user = null, passwd = null;
      if (userInfo.contains(":")) {
        String[] userPassword = userInfo.split(":");
        if (userPassword.length != 2) {
          throw new IllegalUriException(HDSConstants.INCORRECT_SCHEME);
        }
        if (userPassword[0] != null
            || !userPassword[0].isEmpty()) {
          user = URLDecoder.decode(userPassword[0], HDSConstants.DEFAULT_CHAR_ENCODING);
        }
        if (userPassword[1] != null
            || !userInfo.isEmpty()) {
          passwd = URLDecoder.decode(userPassword[1], HDSConstants.DEFAULT_CHAR_ENCODING);
        }
      }
      builder.setUser(user);
      builder.setPasswd(passwd);
    }

    private void parseHost(String hostPart) throws IOException {
      if (hostPart.contains(":")) {
        String[] hostInfo = hostPart.split(":");
        if (hostInfo.length != 2) {
          throw new IllegalUriException(HDSConstants.INCORRECT_SCHEME);
        }
        builder.setHost(hostInfo[0]);
        if (!Pattern.matches("[0-9]+", hostInfo[1])) {
          throw new IllegalUriException(HDSConstants.INCORRECT_SCHEME);
        }
        builder.setPort(Integer.parseInt(hostInfo[1]));
      }else{
        builder.setHost(hostPart);
      }
    }

    private void parsePath(String pathAndQuery) throws IOException {
      if (pathAndQuery.contains("?")) {
        int wall = pathAndQuery.indexOf("?");
        path = pathAndQuery.substring(0, wall);
        queries = parseQuery(pathAndQuery.substring(wall + 1));
      } else {
        path = pathAndQuery;
        queries = new Query();
      }
    }

    private Query parseQuery(String rawQuery) throws UnsupportedEncodingException, IllegalUriException {
      return new Query(rawQuery);
    }

    public Protocol getProtocol() {
      return protocol;
    }

    public String getPath() {
      return path;
    }

    public Optional<String> getDataStorageId() {
      return dsId;
    }

    public Query getQuery() {
      return queries;
    }

    public AccountInfoBuilder getAccountInfoBuilder() {
      return builder;
    }

  }
}

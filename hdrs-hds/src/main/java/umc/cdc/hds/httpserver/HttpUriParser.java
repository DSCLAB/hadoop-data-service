/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.httpserver;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.Optional;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.exceptions.IllegalUriException;
import umc.cdc.hds.uri.Query;

/**
 *
 * @author brandboat
 */
public class HttpUriParser {

  private final Query query;
  private final URI uri;

  HttpUriParser(URI uri) throws UnsupportedEncodingException, IllegalUriException {
    this.uri = uri;
    String[] rawUri = uri.toString().split("\\?");
    switch (rawUri.length) {
      case 1:
        this.query = new Query();
        break;
      case 2:
        String rawQuery = rawUri[1];
        this.query = new Query(rawQuery);
        break;
      default:
        throw new IllegalUriException("Illegal uri request.");
    }
  }

  public Optional<String> getFrom() throws UnsupportedEncodingException {
    return query.getQueryValue(HDSConstants.URL_ARG_FROM);
  }

  public Optional<String> getTo() throws UnsupportedEncodingException {
    return query.getQueryValue(HDSConstants.URL_ARG_TO);
  }

  public Optional<String> getId() {
    return query.getQueryValue(HDSConstants.MAPPING_ARG_ID);
  }

  public Optional<String> getApplicationId() {
    return query.getQueryValue(HDSConstants.DRS_APPLICATIONID);
  }

  public Optional<String> getCode() {
    return query.getQueryValue(HDSConstants.RUN_ARG_CODE);
  }

  public Optional<String> getData() {
    return query.getQueryValue(HDSConstants.RUN_ARG_DATA);
  }

  public Optional<String> getConfig() {
    return query.getQueryValue(HDSConstants.RUN_ARG_CONFIG);
  }

  public Optional<String> getCodeOut() {
    return query.getQueryValue(HDSConstants.RUN_ARG_CODEOUT);
  }

  public Optional<String> getCopyTo() {
    return query.getQueryValue(HDSConstants.RUN_ARG_COPYTO);
  }

  public Optional<String> getConsoleTo() {
    return query.getQueryValue(HDSConstants.RUN_ARG_CONSOLETO);
  }

  // for access from jdbc  -->
  public Optional<String> getJdbcInfo() { return query.getQueryValue(HDSConstants.JDBC_INFO_QUERY); }

  public Optional<String> getFile() { return query.getQueryValue(HDSConstants.JDBC_QUERY_FILE); }

  public Optional<String> getJdbcQuery() { return query.getQueryValue(HDSConstants.JDBC_QUERY_QUERY); }

  public Optional<String> getJdbcFormat() { return query.getQueryValue(HDSConstants.JDBC_CSV_FORMAT_QUERY); }

  public Optional<String> getJdbcTable() { return query.getQueryValue(HDSConstants.JDBC_TABLE_QUERY); }

  public Optional<String> getJdbcHeader() { return query.getQueryValue(HDSConstants.JDBC_WITH_HEADER_QUERY); }
  // <-- for access from jdbc


  public boolean getEnableWildcard() {
    return query.getQueryValueAsBoolean(
        HDSConstants.URL_ENABLE_WILDCARD, false);
  }

  public boolean getRecursive() {
    return query.getQueryValueAsBoolean(HDSConstants.URL_ARG_DELETE_RECURSIVE, false);
  }

  public Optional<String> getRedirectorFrom() {
    return query.getQueryValue(HDSConstants.URL_REDIRECT_FROM);
  }

  public boolean getAsync() {
    return query.getQueryValueAsBoolean(
        "async", false);
  }

  public Optional<String> getToken() {
    return query.getQueryValue(HDSConstants.URL_ARG_TOKEN);
  }

  public boolean getHistory() {
    return query.getQueryValueAsBoolean(HDSConstants.URL_ARG_HISTORY, false);
  }

  public Query getQuery() {
    return query;
  }

  public String toUri() {
    return uri.toString();
  }
}

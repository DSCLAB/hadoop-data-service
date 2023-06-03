/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.mapping;

import java.io.IOException;
import java.util.Map;

import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.parser.ElaboratedString;
import umc.cdc.hds.parser.ElaboratedPositiveInteger;
import umc.cdc.hds.uri.Query;

/**
 *
 * @author brandboat
 */
public class AccountQuery extends Query {

  private final ElaboratedString id;
  private final ElaboratedString domain;
  private final ElaboratedString host;
  private final ElaboratedPositiveInteger port;
  private final ElaboratedString user;
  private final ElaboratedString passwd;

  public AccountQuery(Map<String, String> query) throws IOException {
    super(query);
    this.id = new ElaboratedString(containsAndGet(HDSConstants.MAPPING_ARG_ID).orElse(null));
    this.domain = new ElaboratedString(containsAndGet(HDSConstants.MAPPING_ARG_DOMAIN).orElse(null));
    this.host = new ElaboratedString(containsAndGet(HDSConstants.MAPPING_ARG_HOST).orElse(null));
    this.port = new ElaboratedPositiveInteger(containsAndGet(HDSConstants.MAPPING_ARG_PORT).orElse(null));
    this.user = new ElaboratedString(containsAndGet(HDSConstants.MAPPING_ARG_USER).orElse(null));
    this.passwd = new ElaboratedString(containsAndGet(HDSConstants.MAPPING_ARG_PASSWD).orElse(null));
  }

  public ElaboratedString getId() {
    return id;
  }

  public ElaboratedString getDomain() {
    return domain;
  }

  public ElaboratedString getHost() {
    return host;
  }

  public ElaboratedPositiveInteger getPort() {
    return port;
  }

  public ElaboratedString getUser() {
    return user;
  }

  public ElaboratedString getPasswd() {
    return passwd;
  }

}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.zookeeper;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

/**
 *
 * @author brandboat
 */
public class ZKAuth {

  private final String scheme;
  private List<ACL> access;
  private static final String DEFAULT_USERNAME = "hds";
  private static final String DEFAULT_PASSWORD = "hdsadmin";
  private static final String DELIMITER = ":";
  private static final String DEFAULT_AUTH
      = DEFAULT_USERNAME + DELIMITER + DEFAULT_PASSWORD;

  public ZKAuth() throws IOException {
    DigestAuthenticationProvider digest
        = new DigestAuthenticationProvider();
    scheme = digest.getScheme();
    initializeACL();
  }

  public List<ACL> getACL() {
    return access;
  }

  public String getScheme() {
    return scheme;
  }

  public byte[] getAuth() {
    return DEFAULT_AUTH.getBytes();
  }

  private void initializeACL() throws IOException {
    try {
      access = new ArrayList();
      access.add(new ACL(ZooDefs.Perms.ALL, new Id(
          scheme,
          DigestAuthenticationProvider.generateDigest(DEFAULT_AUTH))));
    } catch (NoSuchAlgorithmException ex) {
      throw new IOException(ex);
    }
  }
}

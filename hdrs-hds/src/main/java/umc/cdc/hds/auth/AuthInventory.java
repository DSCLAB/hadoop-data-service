/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.auth;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import umc.cdc.hds.auth.AuthInfo.AuthBuilder;
import umc.cdc.hds.core.Api;

/**
 * Load auth file on hdfs.
 *
 * @author brandboat
 */
public class AuthInventory {

  private final Properties authProps;
  private final Map<String, AuthInfo> authMap;

  private static final String HDS_AUTH_ID = "hds.auth.id";

  private static String tokenPropName(String id) {
    return "hds.auth." + id + ".token";
  }

  private static String apiPropName(String id) {
    return "hds.auth." + id + ".api";
  }

  public AuthInventory(InputStream is) throws IOException {
    authProps = loadAuthList(is);
    authMap = loadAuthListToMap();
  }

  public Optional<AuthInfo> getAuth(String token) {
    return Optional.ofNullable(authMap.get(token));
  }

  private static Properties loadAuthList(InputStream is)
      throws IOException {
    Properties prop = new Properties();
    prop.load(is);
    return prop;
  }

  private Map<String, AuthInfo> loadAuthListToMap() {
    Optional<String> authIds = Optional.ofNullable(
        authProps.getProperty(HDS_AUTH_ID));
    List<String> ids = new ArrayList<>();
    if (authIds.isPresent()) {
      ids = Arrays.asList(authIds.get().split(","));
    }
    Map<String, AuthInfo> am = new HashMap<>();
    ids.forEach(id -> {
      try {
        AuthInfo auth = createAuth(id);
        am.put(auth.getToken(), auth);
      } catch (Exception ex) {
      }
    });
    return am;
  }

  private AuthInfo createAuth(String id) {
    String token = authProps.getProperty(tokenPropName(id));
    List<String> apiStringList = Arrays.asList(
        authProps.getProperty(apiPropName(id)).split(","));
    Set<String> apiStringSet = new HashSet<>(apiStringList);
    Set<Api> apiSet = new HashSet<>();
    apiStringSet.forEach(api -> {
      Optional<Api> a = Api.find(api);
      if (a.isPresent()) {
        apiSet.add(a.get());
      }
    });
    AuthBuilder ab = new AuthBuilder();
    ab.setId(id).setToken(token).setApi(apiSet);
    return ab.build();
  }
}

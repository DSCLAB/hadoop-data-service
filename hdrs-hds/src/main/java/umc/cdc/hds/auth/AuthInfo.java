/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.auth;

import java.util.Set;
import umc.cdc.hds.core.Api;

/**
 *
 * @author brandboat
 */
public class AuthInfo {

  private final String id;
  private final String token;
  private final Set<Api> api;

  public AuthInfo(String id, String token, Set<Api> api) {
    this.id = id;
    this.token = token;
    this.api = api;
  }

  public String getId() {
    return id;
  }

  public String getToken() {
    return token;
  }

  public Set<Api> getApi() {
    return api;
  }

  public static class AuthBuilder {

    private String id;
    private String token;
    private Set<Api> api;

    public AuthBuilder setId(String id) {
      this.id = id;
      return this;
    }

    public AuthBuilder setToken(String token) {
      this.token = token;
      return this;
    }

    public AuthBuilder setApi(Set<Api> api) {
      this.api = api;
      return this;
    }

    public AuthInfo build() {
      if (id == null) {
        throw new RuntimeException("Auth id not specified.");
      }
      if (token == null) {
        throw new RuntimeException("Auth token not specified.");
      }
      if (api == null) {
        throw new RuntimeException("Auth api not specified");
      }
      return new AuthInfo(id, token, api);
    }
  }
}

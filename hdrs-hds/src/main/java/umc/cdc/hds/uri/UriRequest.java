/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.uri;

import java.io.IOException;
import java.util.Optional;
import umc.cdc.hds.core.Protocol;
import umc.cdc.hds.mapping.AccountInfo;

/**
 *
 * @author jpopaholic
 */
public interface UriRequest {

  Protocol getProtocol();

  Optional<AccountInfo> getAccountInfo() throws IOException;

  String getDir();

  Optional<String> getName();

  String getPath();

  Query getQuery();

  Optional<String> getStorageId();

  void setProtocol(Protocol p);

  void setAccountInfo(AccountInfo a);

  void setDir(String dir);

  void setName(String name);

  void setQuery(Query q);

  String toRealUri();

  void setFileNameToDirName();
  
  void clearName();

}

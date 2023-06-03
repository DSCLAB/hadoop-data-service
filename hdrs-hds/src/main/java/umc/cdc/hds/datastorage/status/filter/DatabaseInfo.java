/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.datastorage.status.filter;


import java.util.Optional;

public class DatabaseInfo {
  private String tableName = null;
  private String keyName = null;

  public DatabaseInfo(String s) {
    String[] args = s.split("column name :");
    if (args.length == 1) {
      this.tableName = args[0];
    }else if (args.length == 2) {
      this.tableName = args[0];
      this.keyName = args[1];
    }
  }
  public String getTableName() {
    return tableName;
  }

  public String getKeyName() {
    return keyName;
  }

}

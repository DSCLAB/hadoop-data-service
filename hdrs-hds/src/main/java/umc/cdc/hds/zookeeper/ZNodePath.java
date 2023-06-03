/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.zookeeper;

/**
 *
 * @author brandboat
 */
public class ZNodePath {

  private String path;

  public ZNodePath(String p) {
    path = p;
  }

  public String getPath() {
    return path;
  }
}

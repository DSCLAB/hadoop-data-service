/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.load;

import java.io.Closeable;
import java.util.Iterator;

/**
 *
 * @author brandboat
 */
public interface ClusterLoad extends Closeable {

  public Iterator<NodeLoadInfo> getClusterLoading();
}

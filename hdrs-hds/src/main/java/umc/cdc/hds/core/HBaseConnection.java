/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.core;

import java.io.IOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;

/**
 *
 * @author brandboat
 */
public interface HBaseConnection extends Connection {

  public String getDataLocation(final TableName tableName, final byte[] key) throws IOException;
}

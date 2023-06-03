/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.load;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import umc.cdc.hds.core.HDSConstants.LOADINGCATEGORY;

/**
 *
 * @author brandboat
 */
public class OperationLoadInfo {

  private LOADINGCATEGORY method;
  private int dealCount;
  private long dealBytes;
  private int pastCount;
  private long pastBytes;

  public OperationLoadInfo() {
  }

  public OperationLoadInfo(LOADINGCATEGORY method, int dealCount, long dealBytes,
      int pastCount, long pastBytes) {
    this.method = method;
    this.dealCount = dealCount;
    this.dealBytes = dealBytes;
    this.pastCount = pastCount;
    this.pastBytes = pastBytes;
  }

  public LOADINGCATEGORY getLoadingCategory() {
    return method;
  }

  public int getDealCount() {
    return dealCount;
  }

  public long getDealBytes() {
    return dealBytes;
  }

  public int getPastCount() {
    return pastCount;
  }

  public long getPastBytes() {
    return pastBytes;
  }

  public void serialize(DataOutputStream out) throws IOException {
    out.writeUTF(method.name());
    out.writeInt(dealCount);
    out.writeLong(dealBytes);
    out.writeInt(pastCount);
    out.writeLong(pastBytes);
  }

  public void deserialize(DataInputStream in) throws IOException {
    method = LOADINGCATEGORY.valueOf(in.readUTF());
    dealCount = in.readInt();
    dealBytes = in.readLong();
    pastCount = in.readInt();
    pastBytes = in.readLong();
  }
}

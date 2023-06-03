/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.load;

import java.io.IOException;
import umc.cdc.hds.core.HDSConstants.LOADINGCATEGORY;

/**
 *
 * @author brandboat
 */
public class OperationLoadInfoBuilder {

  private LOADINGCATEGORY method = null;
  private int dealCount = 0;
  private long dealBytes = 0;
  private int pastCount = 0;
  private long pastBytes = 0;

  public OperationLoadInfoBuilder() {
  }

  public OperationLoadInfoBuilder setLoadingCategory(LOADINGCATEGORY method) {
    this.method = method;
    return this;
  }

  public OperationLoadInfoBuilder setDealCount(int dealCount) {
    this.dealCount = dealCount;
    return this;
  }

  public OperationLoadInfoBuilder setDealBytes(long dealBytes) {
    this.dealBytes = dealBytes;
    return this;
  }

  public OperationLoadInfoBuilder setPastCount(int pastCount) {
    this.pastCount = pastCount;
    return this;
  }

  public OperationLoadInfoBuilder setPastBytes(long pastBytes) {
    this.pastBytes = pastBytes;
    return this;
  }

  public OperationLoadInfo build() throws IOException {
    if (method == null) {
      throw new IOException("Loading is null.");
    }
    return new OperationLoadInfo(method, dealCount, dealBytes, pastCount, pastBytes);
  }
}

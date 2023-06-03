/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.requestloadbalancer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.load.NodeLoadInfo;
import umc.cdc.hds.load.OperationLoadInfo;

/**
 *
 * @author jpopaholic
 */
public class RequestCostsCalculator {

  private final double readRequestCoefficient;
  private final double writeRequestCoefficient;
  private final double readBytesCoefficient;
  private final double writeBytesCoefficient;
  private final double pastCoefficient;
  private final double serverWeight;

  public RequestCostsCalculator(Configuration conf) {
    readBytesCoefficient = conf.getDouble(HDSConstants.READ_BYTES_COST_COEFFICIENT,
        HDSConstants.DEFAULT_READ_BYTES_COST_COEFFICIENT);
    readRequestCoefficient = conf.getDouble(HDSConstants.READ_REQUEST_COST_COEFFICIENT,
        HDSConstants.DEFAULT_READ_REQUEST_COST_COEFFICIENT);
    writeBytesCoefficient = conf.getDouble(HDSConstants.WRITE_BYTES_COST_COEFFICIENT,
        HDSConstants.DEFAULT_WRITE_BYTES_COST_COEFFICIENT);
    writeRequestCoefficient = conf.getDouble(HDSConstants.WRITE_REQUEST_COST_COEFFICIENT,
        HDSConstants.DEFAULT_WRITE_REQUEST_COST_COEFFICIENT);
    double pastWeight = conf.getDouble(HDSConstants.PAST_PROPORTION,
        HDSConstants.DEFAULT_PAST_PROPORTION);
    pastCoefficient = pastWeight >= 0 && pastWeight <= 1 ? pastWeight : HDSConstants.DEFAULT_PAST_PROPORTION;
    serverWeight = conf.getDouble(HDSConstants.SERVER_WEIGHT_COEFFICIENT,
        HDSConstants.DEFAULT_SERVER_WEIGHT_COEFFICIENT);
  }

  public double getCost(ServerName s, NodeLoadInfo datas) {
    if (datas == null) {
      return Double.MAX_VALUE;
    }
    return (getReadCost(datas) + getWriteCost(datas)) * serverWeight;
  }

  private double getReadCost(NodeLoadInfo datas) {
    OperationLoadInfo read
        = datas.getOperationLoadInfos().stream()
        .filter((info) -> info.getLoadingCategory().equals(HDSConstants.LOADINGCATEGORY.READ))
        .findFirst().orElse(null);
    if (read == null) {
      return Double.MAX_VALUE;
    }
    double cost = 0;
    cost += ((read.getDealCount() * readRequestCoefficient
        + read.getDealBytes() * readBytesCoefficient) * (1 - pastCoefficient));
    cost += ((read.getPastBytes() * readBytesCoefficient
        + read.getPastCount() * readRequestCoefficient)) * pastCoefficient;
    return cost;
  }

  private double getWriteCost(NodeLoadInfo datas) {
    OperationLoadInfo write
        = datas.getOperationLoadInfos().stream()
        .filter((info) -> info.getLoadingCategory().equals(HDSConstants.LOADINGCATEGORY.WRITE))
        .findFirst().orElse(null);
    if (write == null) {
      return Double.MAX_VALUE;
    }
    double cost = 0;
    cost += ((write.getDealCount() * writeRequestCoefficient
        + write.getDealBytes() * writeBytesCoefficient) * (1 - pastCoefficient));
    cost += ((write.getPastBytes() * writeBytesCoefficient
        + write.getPastCount() * writeRequestCoefficient)) * pastCoefficient;
    return cost;

  }
}

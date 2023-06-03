/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.metric;

import java.io.Closeable;
import umc.cdc.hds.core.HDSConstants.LOADINGCATEGORY;

/**
 *
 * @author jpopaholic
 */
public interface MetricSystem extends Closeable {

  MetricCounter registeExpectableCounter(LOADINGCATEGORY type, long expectBytes);

  MetricCounter registerUnExpectableCounter(LOADINGCATEGORY type);

  long getAllHistoryBytes(LOADINGCATEGORY type);

  int getAllHistoryRequests(LOADINGCATEGORY type);

  long getAllFutureBytes(LOADINGCATEGORY type);

  int getAllFutureRequests(LOADINGCATEGORY type);

}

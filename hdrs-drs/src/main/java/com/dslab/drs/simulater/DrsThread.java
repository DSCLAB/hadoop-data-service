package com.dslab.drs.simulater;

import com.dslab.drs.restful.api.json.JsonUtils;
import com.dslab.drs.restful.api.response.list.DataInfos;
import com.dslab.drs.simulater.connection.hds.HdsRequester;
import com.dslab.drs.utils.DrsConfiguration;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author kh87313
 */
public class DrsThread implements Runnable {

  private static final Log LOG = LogFactory.getLog(DrsThread.class);
  private static final AtomicInteger runningThreadNumber = new AtomicInteger(0);

  private final String id;
  private final DrsArguments arguments;
  private final DrsConfiguration conf;
  private final boolean async;

  public DrsThread(String id, DrsArguments arguments, DrsConfiguration conf, boolean async) {
    this.id = id;
    this.arguments = arguments;
    this.conf = conf;
    this.async = async;
  }

  @Override
  public void run() {
    runningThreadNumber.incrementAndGet();
    waitFileCanList(arguments, 20);

    HdsRequester hdsRequester = new HdsRequester();
    String response = null;
    try {
      response = hdsRequester.requestDrs(arguments, conf, async);
    } catch (MalformedURLException ex) {
      Logger.getLogger(DrsThread.class.getName()).log(Level.SEVERE, null, ex);
    } catch (IOException ex) {
      Logger.getLogger(DrsThread.class.getName()).log(Level.SEVERE, null, ex);
    }

    LOG.info(this.id + ":" + response);
    runningThreadNumber.decrementAndGet();
  }

  private void waitFileCanList(DrsArguments drsArgument, long maxWaitSeconds) {
    HdsRequester hdsRequestor = new HdsRequester();
    for (int i = 1; i <= maxWaitSeconds; i++) {
       LOG.debug(id + " check drs arguments data upload.");
      try {
        String dataListResult = hdsRequestor.list(conf, drsArgument.getData());
        String configListResult = hdsRequestor.list(conf, drsArgument.getConfig());
        String codeListResult = hdsRequestor.list(conf, drsArgument.getCode());
        DataInfos dataList = JsonUtils.fromJson(dataListResult, DataInfos.class);
        DataInfos configList = JsonUtils.fromJson(configListResult, DataInfos.class);
        DataInfos codeList = JsonUtils.fromJson(codeListResult, DataInfos.class);

        if (dataList.getDataInfo().size() > 0 && configList.getDataInfo().size() > 0 && codeList.getDataInfo().size() > 0) {
          return;
        }

        Thread.sleep(1000);
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
  }

  public String getId() {
    return this.id;
  }

  public static int getRunningThread() {
    return runningThreadNumber.get();
  }
}

package com.dslab.drs.socket;

import com.dslab.drs.utils.DrsConfiguration;
import java.io.IOException;

public class StateFactory {

  public ResponseState getResponseType(ResponseType responseType, SocketRun sr,
          SocketGlobalVariables global, DrsConfiguration conf) throws IOException {
    switch (responseType) {
      case APPCLI_ASK_PROGRESS:
        return new AppCliAskProgress(sr, global);
      case DRSCLI_KILL:
        return new DrsCliKill(sr, global);
      case APPCLI_REGISTER:
        return new AppCliRegister(sr, global);
      case WATCH_REQ:
        return new WatchReq(sr, global);
      case PROGRESS:
        return new Progress(sr, global);
      case REGISTER:
        return new Register(sr, global);
      case SCHEDULE_SUCCESS:
        return new SchedulerSuccess(sr, global);
      case NEW_TASK:
        return new NewTask(sr, global);
      case FAILED_TASK:
        return new FailTask(sr, global);
      case DONE_TASK:
        return new DoneTask(sr, global);
      case ASK_STATUS:
        return new AskStatus(sr, global, conf);
      case CONTAINER_WAIT:
        return new ContainerWait(sr, global);
      case WRONG_REQ:
        return new WrongReq(sr, global);
      default:
        throw new IOException("no this Type " + responseType);
    }
  }
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.httpserver;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.datastorage.status.ErrorInfo;
import umc.cdc.hds.mapping.AccountInfo;

/**
 *
 * @author brandboat
 */
public abstract class JsonUtil {

  public static JsonObject getHttpErrorResponse(Exception ex) {
    JsonObject errorJson = new JsonObject(),
        remoteException = new JsonObject();
    ErrorInfo error = new ErrorInfo(ex);
    remoteException.add(HDSConstants.ERROR_RESPONSE_JAVACLASSNAME,
        new JsonPrimitive(error.getClassName()));
    remoteException.add(HDSConstants.ERROR_RESPONSE_EXCEPTION,
        new JsonPrimitive(error.getExceptionName()));
    remoteException.add(HDSConstants.ERROR_RESPONSE_MESSAGE,
        new JsonPrimitive(error.getErrorMessage()));
    errorJson.add(HDSConstants.ERROR_RESPONSE, remoteException);
    return errorJson;
  }

  public static JsonObject getAccountInfoList(Iterable<AccountInfo> le) {
    JsonObject listMappingResponse = new JsonObject();
    JsonArray mappingArray = new JsonArray();
    le.forEach((AccountInfo accountInfo) -> {
      mappingArray.add(createAccountInfo(accountInfo));
    });
    listMappingResponse.add(HDSConstants.MAPPING_ACCOUNT, mappingArray);
    return listMappingResponse;
  }

  public static JsonObject getAccountInfo(AccountInfo accountInfo) {
    JsonObject obj = new JsonObject();
    obj.add(HDSConstants.MAPPING_ACCOUNT, createAccountInfo(accountInfo));
    return obj;
  }

  private static JsonObject createAccountInfo(AccountInfo accountInfo) {
    JsonObject mapping = new JsonObject();
    JsonPrimitive id = accountInfo.getId().isPresent()
        ? new JsonPrimitive(accountInfo.getId().get()) : null;
    mapping.add(HDSConstants.MAPPING_ARG_ID, id);
    JsonPrimitive domain = accountInfo.getDomain().isPresent()
        ? new JsonPrimitive(accountInfo.getDomain().get()) : null;
    mapping.add(HDSConstants.MAPPING_ARG_DOMAIN, domain);
    JsonPrimitive user = accountInfo.getUser().isPresent()
        ? new JsonPrimitive(accountInfo.getUser().get()) : null;
    mapping.add(HDSConstants.MAPPING_ARG_USER, user);
    JsonPrimitive passwd = accountInfo.getPasswd().isPresent()
        ? new JsonPrimitive(accountInfo.getPasswd().get()) : null;
    mapping.add(HDSConstants.MAPPING_ARG_PASSWD, passwd);
    JsonPrimitive host = accountInfo.getHost().isPresent()
        ? new JsonPrimitive(accountInfo.getHost().get()) : null;
    mapping.add(HDSConstants.MAPPING_ARG_HOST, host);
    JsonPrimitive port = accountInfo.getPort().isPresent()
        ? new JsonPrimitive(accountInfo.getPort().get()) : null;
    mapping.add(HDSConstants.MAPPING_ARG_PORT, port);
    return mapping;
  }

}

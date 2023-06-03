/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.core;

import com.google.gson.JsonElement;
import com.google.gson.JsonSyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author brandboat
 */
public class JsonParserUtil {

  public static JsonElement getChild(JsonElement e, String key) {
    if (e.isJsonObject() && e.getAsJsonObject().has(key)) {
      return e.getAsJsonObject().get(key);
    }
    throw new JsonSyntaxException("wrong format [key:JSON] or no such key: " + key);
  }

  public static String getStringElement(JsonElement e, String key) {
    if (e.isJsonObject() && e.getAsJsonObject().has(key)) {
      return e.getAsJsonObject().get(key).getAsString();
    }
    throw new JsonSyntaxException("wrong format {key:String} or no such key: " + key);
  }

  public static long getLongElement(JsonElement e, String key) {
    if (e.isJsonObject() && e.getAsJsonObject().has(key)) {
      return e.getAsJsonObject().get(key).getAsLong();
    }
    throw new JsonSyntaxException("wrong format {key:long} or no such key: " + key);
  }

  public static int getIntegerElement(JsonElement e, String key) {
    if (e.isJsonObject() && e.getAsJsonObject().has(key)) {
      return e.getAsJsonObject().get(key).getAsInt();
    }
    throw new JsonSyntaxException("wrong format {key:int} or no such key: " + key);
  }

  public static List<JsonElement> getArray(JsonElement e, String key) {
    if (e.isJsonObject() && e.getAsJsonObject().has(key)) {
      List<JsonElement> array = new ArrayList();
      e.getAsJsonObject().get(key).getAsJsonArray().forEach((child) -> {
        array.add(child);
      });
      return array;
    }
    throw new JsonSyntaxException("wrong format {key:JSON array} or no such key: " + key);
  }
}

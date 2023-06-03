package com.dslab.drs.restful.api.json;

import com.google.gson.Gson;
import com.dslab.drs.exception.GetJsonBodyException;


/**
 * Wraps all json functions.
 */
public final class JsonUtils {

  public static <T extends JsonSerialization> T fromJson(String json, Class<T> classOfT) throws GetJsonBodyException {
    Gson gson = new Gson();
    return gson.fromJson(json, classOfT);
  }
  public static String toJson(JsonSerialization src) {
    Gson gson = new Gson();
    return gson.toJson(src);
  }

  /**
   * private ctor for util class.
   */
  private JsonUtils() {
  }
}

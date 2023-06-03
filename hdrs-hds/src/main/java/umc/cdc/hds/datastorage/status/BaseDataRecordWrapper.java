/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.datastorage.status;

import java.util.*;

import com.jcraft.jsch.ChannelSftp;
import umc.cdc.hds.core.Protocol;
import umc.cdc.hds.datastorage.status.DataRecord.FileType;
import umc.cdc.hds.tools.CloseableIterator;

/**
 *
 * @author jpopaholic
 * @param <F>
 */
public abstract class BaseDataRecordWrapper<F> {

  public DataRecord wrapSingleFile(F file) {
    DataRecordBuilder builder = new DataRecordBuilder();
    builder.setName(wrapName(file));
    FileType type = wrapFileType(file);
    if (type != null) {
      builder.setFileType(type);
    }
    builder.setOwner(wrapDataOwner(file));
    builder.setProtocol(wrapLocation());
    builder.setUploadTime(wrapModifiedTime(file));
    builder.setUri(wrapUri(file));
    builder.setSize(wrapSize(file));
    return builder.build();
  }

  public CloseableIterator<BatchDeleteRecord> wrapParticalFilesWithErrorMessage(F[] allFiles, int[] indexs, List<Exception> errors) {
    return new CloseableIterator<BatchDeleteRecord>() {
      int currentIndex = 0;

      @Override
      public boolean hasNext() {
        return currentIndex < indexs.length;
      }

      @Override
      public BatchDeleteRecord next() {
        if (!this.hasNext()) {
          return null;
        }
        BatchDeleteRecord result = new BatchDeleteRecord(wrapSingleFile(allFiles[indexs[currentIndex]]), new ErrorInfo(errors.get(currentIndex)));
        currentIndex++;
        return result;
      }

    };
  }
  public CloseableIterator<BatchDeleteRecord> wrapParticalFilesWithErrorMessage(F allFiles, int[] indexs, List<Exception> errors) {
    return new CloseableIterator<BatchDeleteRecord>() {
      int currentIndex = 0;

      @Override
      public boolean hasNext() {
        return currentIndex < indexs.length;
      }

      @Override
      public BatchDeleteRecord next() {
        if (!this.hasNext()) {
          return null;
        }
        BatchDeleteRecord result = new BatchDeleteRecord(wrapSingleFile((F) ((Vector)allFiles).get(indexs[currentIndex])), new ErrorInfo(errors.get(currentIndex)));
        currentIndex++;
        return result;
      }

    };
  }

  public Iterator<DataRecord> wrapFiles(F[] allFiles) {
    if (allFiles == null) {
      return Collections.emptyIterator();
    }
    return new Iterator<DataRecord>() {
      int current = 0;

      @Override
      public boolean hasNext() {
        return current < allFiles.length;
      }

      @Override
      public DataRecord next() {
        if (!this.hasNext()) {
          return null;
        }
        DataRecord result = wrapSingleFile(allFiles[current]);
        current++;
        return result;
      }
    };
  }
  public Iterator<DataRecord> wrapFilesSftp(Vector allFiles) {
    if (allFiles == null) {
      return Collections.emptyIterator();
    }
    return new Iterator<DataRecord>() {
      int current = 0;

      @Override
      public boolean hasNext() {
        return current < allFiles.size();
      }

      @Override
      public DataRecord next() {
        if (!this.hasNext()) {
          return null;
        }
        Vector af = new Vector();
        af.add(allFiles.get(current));
        DataRecord result = wrapSingleFile((F) af);
        current++;
        return result;
      }
    };
  }
  protected abstract String wrapName(F file);

  protected abstract long wrapSize(F file);

  protected abstract long wrapModifiedTime(F file);

  protected abstract Protocol wrapLocation();

  protected abstract Map<String, Double> wrapDataOwner(F file);

  protected abstract FileType wrapFileType(F file);

  protected abstract String wrapUri(F file);

}

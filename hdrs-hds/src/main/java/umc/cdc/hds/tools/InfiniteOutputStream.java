/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.tools;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author brandboat
 */
public class InfiniteOutputStream extends OutputStream {

  private final ByteArrayOutput buffer;
  private OutputStream infiniteOutput;
  private final InfiniteDumpLocation infiniteLoc;
  private int bufferLimit = 0;
  private int writeSize = 0;
  private boolean isClosed = false;

  public InfiniteOutputStream(final int bufferLimit,
      InfiniteDumpLocation infiniteLoc) throws IOException {
    this.bufferLimit = bufferLimit;
    this.infiniteLoc = infiniteLoc;
    this.buffer = bufferLimit < 0
        ? null
        : new ByteArrayOutput(bufferLimit);
    this.infiniteOutput = bufferLimit < 0
        ? infiniteLoc.createOutputStream()
        : null;
  }

  public InfiniteOutputStream(final byte[] buffer,
      InfiniteDumpLocation infiniteLoc) throws IOException {
    this.bufferLimit = buffer.length;
    this.infiniteLoc = infiniteLoc;
    this.buffer = bufferLimit < 0
        ? null
        : new ByteArrayOutput(buffer);
    this.infiniteOutput = bufferLimit < 0
        ? infiniteLoc.createOutputStream()
        : null;
  }

  @Override
  public void close() throws IOException {
    isClosed = true;
    getValidOutputStream().close();
  }

  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public void write(int b) throws IOException {
    getValidOutputStream().write((byte) b);
    writeSize++;
  }

  public int getWriteSize() {
    return writeSize;
  }

  public boolean isInMemory() {
    if (bufferLimit < 0) {
      return false;
    }
    return writeSize <= bufferLimit;
  }

  public OutputStream getValidOutputStream() throws IOException {
    if (isInMemory()) {
      return buffer;
    }
    return createInfiniteOutputIfNull();
  }

  private OutputStream createInfiniteOutputIfNull() throws IOException {
    if (infiniteOutput == null) {
      infiniteOutput = infiniteLoc.createOutputStream();
      flushBufferToInfiniteOutput();
    }
    return infiniteOutput;
  }

  private void flushBufferToInfiniteOutput() throws IOException {
    if (buffer != null) {
      infiniteOutput.write(buffer.getByteBuffer(), 0, writeSize);
    }
  }

  public InputStream getInputStream() throws IOException {
    if (!isClosed()) {
      throw new IOException("Infinite outputStream is not closed.");
    }
    if (isInMemory()) {
      return new ByteArrayInputStream(buffer.toByteArray());
    } else {
      return infiniteLoc.createInputStream();
    }
  }

  public static class ByteArrayOutput extends ByteArrayOutputStream {

    public ByteArrayOutput(int bufferLimit) {
      super(bufferLimit);
    }

    public ByteArrayOutput(byte[] byteArray) {
      if (byteArray == null) {
        throw new IllegalArgumentException("Byte Array is null.");
      }
      buf = byteArray;
    }

    public byte[] getByteBuffer() {
      return buf;
    }
  }

  public static interface InfiniteDumpLocation {

    public OutputStream createOutputStream() throws IOException;

    public InputStream createInputStream() throws IOException;

    public void delete() throws IOException;
  }

  public static final class LocalFile implements InfiniteDumpLocation {

    private final File f;

    public LocalFile() {
      final String fileName = LocalFile.class.getCanonicalName()
          + UUID.randomUUID().toString();
      f = new File(fileName);
    }

    @Override
    public OutputStream createOutputStream() throws FileNotFoundException {
      return new FileOutputStream(f);
    }

    @Override
    public InputStream createInputStream() throws IOException {
      return new FileInputStream(f);
    }

    @Override
    public void delete() throws IOException {
      if (!f.delete()) {
        throw new IOException(f.getName()
            + " file is not delete correctly.");
      }
    }
  }

  public static final class HdfsFile implements InfiniteDumpLocation {

    private final FileSystem fileSystem;
    private final Path path;

    public HdfsFile(FileSystem fs, Path p) {
      fileSystem = fs;
      path = p;
    }

    @Override
    public OutputStream createOutputStream() throws IOException {
      return fileSystem.create(path);
    }

    @Override
    public InputStream createInputStream() throws IOException {
      return fileSystem.open(path);
    }

    @Override
    public void delete() throws IOException {
      if (!fileSystem.delete(path, false)) {
        throw new IOException(path.getName()
            + " file is not delete correctly.");
      }
    }
  }
}

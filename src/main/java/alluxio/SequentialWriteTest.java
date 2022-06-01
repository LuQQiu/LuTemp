package alluxio;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class SequentialWriteTest {
  public static final int BUFFER_LEN = 4 * 1024 * 1024;
  public static final Random RANDOM = new Random();

  public static void sequentialWriteSingleFile(String localFolder, String fuseFolder, long fileSize, long bufferSize) throws IOException {
    System.out.printf("Validate file size %s and buffer size %s%n", fileSize, bufferSize);
    String fileName = getRandomFileName("FuseTest");
    Path localPath = Paths.get(localFolder, fileName);
    Path fusePath = Paths.get(fuseFolder, fileName);
    createFileWithLen(localPath.toString(), fileSize);
    try {
      byte[] buffer = new byte[(int) bufferSize];
      try (FileInputStream in = new FileInputStream(String.valueOf(localPath));
           FileOutputStream out = new FileOutputStream(String.valueOf(fusePath))) {
        while (true) {
          int size = in.read(buffer);
          if (size == -1) {
            break;
          }
          out.write(buffer, 0, size);
        }
      }
      validateFilesEqual(localPath, fusePath);
    } finally {
      Files.delete(localPath);
      if (Files.exists(fusePath)) {
        Files.delete(fusePath);
      }
    }
  }

  private static void validateFilesEqual(Path local, Path fuse) throws IOException {
    long size = Files.size(local);
    if (size != Files.size(fuse)) {
      throw new IOException(String.format("The size of local path %s and fuse path %s is different", local, fuse));
    }
    if (size < 2 * WriteMain.MB) {
      if (!Arrays.equals(Files.readAllBytes(local),
          Files.readAllBytes(fuse))) {
        throw new IOException(String.format("File content of %s and %s is different", local, fuse));
      }
    }

    int bufferSize = (int) Math.min(BUFFER_LEN, size);
    byte[] localBuffer = new byte[bufferSize];
    byte[] fuseBuffer = new byte[bufferSize];
    try (FileInputStream localStream = new FileInputStream(local.toFile());
    FileInputStream fuseStream = new FileInputStream(fuse.toFile())) {
      while (true) {
        int readSize = fuseStream.read(fuseBuffer, 0, bufferSize);
        if (readSize == -1) {
          if (localStream.read(localBuffer, 0, bufferSize) == -1) {
            return;
          }
          throw new IOException("fuseStream finish reading while local stream still have data");
        }
        int localOffset = 0;
        while (localOffset < readSize) {
          int localBytesRead = localStream.read(localBuffer, localOffset, readSize - localOffset);
          if (localBytesRead == -1) {
            throw new IOException(String.format("Cannot read %s bytes from local file", readSize));
          }
          localOffset += localBytesRead;
        }
        for (int i = 0; i < readSize; i++) {
          if (localBuffer[i] != fuseBuffer[i]) {
            throw new IOException("Data read from fuse is different from data read from local");
          }
        }
      }
    }
  }

  public static void createFileWithLen(String filePath, long len) throws IOException {
    FileOutputStream stream = new FileOutputStream(filePath);
    int bufferLen = (int) Math.min(BUFFER_LEN, len);
    byte[] buffer = new byte[bufferLen];
    while (len > 0) {
      RANDOM.nextBytes(buffer);
      int sizeToWrite = (int) Math.min(bufferLen, len);
      stream.write(buffer, 0, sizeToWrite);
      len -= sizeToWrite;
    }
  }

  public static String getRandomFileName(String prefix) {
    return prefix + RANDOM.nextLong();
  }
}

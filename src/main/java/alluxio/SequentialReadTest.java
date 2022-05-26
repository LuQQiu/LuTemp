package alluxio;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class SequentialReadTest {
  public static void testSingleBuffer(String localPath, String fusePath, long fileSize, long bufferSize, int iteration) throws IOException {
    if (bufferSize > Integer.MAX_VALUE) {
      throw new IOException("Cannot handle buffer size bigger than Integer.MAX_VALUE");
    }
    try (RandomAccessFile localInStream = new RandomAccessFile(localPath, "r")) {
      for (int i = 0; i < iteration; i++) {
        sequentialReadSingleFile(localInStream, fusePath, fileSize, new byte[(int) bufferSize]);
      }
    }
  }

  public static void testAllBuffer(String localPath, String fusePath, long fileSize, long[] bufferSizes, int iteration, long endTime) throws IOException {
    List<byte[]> buffers = new ArrayList<>();
    for (long bufferSize : bufferSizes) {
      if (bufferSize > Integer.MAX_VALUE) {
        throw new IOException("Cannot handle buffer size bigger than Integer.MAX_VALUE");
      }
      buffers.add(new byte[(int) bufferSize]);
    }
    boolean firstRound = true;
    try (RandomAccessFile localInStream = new RandomAccessFile(localPath, "r")) {
      while (firstRound || System.currentTimeMillis() < endTime) {
        for (int i = 0; i < iteration; i++) {
          for (byte[] buffer : buffers) {
            sequentialReadSingleFile(localInStream, fusePath, fileSize, buffer);
          }
          System.out.printf("Finished iteration %s of file size %s%n", iteration, fileSize);
        }
        firstRound = false;
      }
    }
  }
  
  private static void sequentialReadSingleFile(RandomAccessFile localInStream, String fusePath, long fileSize, byte[] buffer) throws IOException {
    try (FileInputStream fuseInStream = new FileInputStream(fusePath)) {
      long offset = 0;
      int fuseBytesRead;
      while (offset < fileSize) {
        fuseBytesRead = fuseInStream.read(buffer);
        if (fuseBytesRead == -1) {
          throw new IOException(String.format("Read to the end with offset %s and file size %s", offset, fileSize));
        }
        ReadMain.validateDataCorrectness(localInStream, buffer, offset, fuseBytesRead);
        offset += fuseBytesRead;
      }
    }
  }
}

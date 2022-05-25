package alluxio;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

public class SequentialReadTest {
  public static void test(String localPath, String fusePath, long fileSize, long bufferSize, int loop) throws IOException {
    if (bufferSize > Integer.MAX_VALUE) {
      throw new IOException("Cannot handle buffer size bigger than Integer.MAX_VALUE");
    }
    byte[] fuseBuffer = new byte[(int) bufferSize];
    long offset;
    int fuseBytesRead;
    try (RandomAccessFile localInStream = new RandomAccessFile(localPath, "r")) {
      for (int i = 0; i < loop; i++) {
        try (FileInputStream fuseInStream = new FileInputStream(fusePath)) {
          offset = 0;
          while (offset < fileSize) {
            fuseBytesRead = fuseInStream.read(fuseBuffer);
            if (fuseBytesRead == -1) {
              throw new IOException(String.format("Read to the end with offset %s and file size %s", offset, fileSize));
            }
            ReadMain.validateDataCorrectness(localInStream, fuseBuffer, offset, fuseBytesRead);
            offset += fuseBytesRead;
          }
        }
      }
    }
  }
}

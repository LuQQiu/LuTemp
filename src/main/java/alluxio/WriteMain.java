package alluxio;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WriteMain {
  public static final long KB = 1024;
  public static final long MB = 1024 * KB;
  public static final long GB = 1024 * MB;
  public static final long[] FILE_SIZES = {100 * KB, MB, 1034, 63 * MB, 65 * MB, GB, 10 * GB};
  public static final long[] WRITE_BUFFER_SIZES = {128, 1000, 1001, MB, 1025, 4 * KB, 32 * KB, 128 * KB, MB, 4 * MB};

  private static final CommandLineParser PARSER = new DefaultParser();
  private static final String LOCAL_FOLDER_OPTION_NAME = "l";
  private static final String FUSE_FOLDER_OPTION_NAME = "f";
  private static final String HELP_OPTION_NAME = "h";
  private static final String THREAD_NUMBER_OPTION_NAME = "t";
  private static final String ITERATION_OPTION_NAME = "i";

  private static final Option LOCAL_FOLDER_OPTION = Option.builder(LOCAL_FOLDER_OPTION_NAME)
      .hasArg()
      .required(true)
      .longOpt("local-folder")
      .desc("Local folder to write source file to and validate data correctness with.")
      .build();
  private static final Option FUSE_FOLDER_OPTION
      = Option.builder(FUSE_FOLDER_OPTION_NAME)
      .hasArg()
      .required(true)
      .longOpt("fuse-folder")
      .desc("The Fuse folder to validate read correctness with")
      .build();
  private static final Option HELP_OPTION = Option.builder(HELP_OPTION_NAME)
      .required(false)
      .desc("Print this help message")
      .build();
  private static final Option THREAD_NUMBER_OPTION = Option.builder(THREAD_NUMBER_OPTION_NAME)
      .required(true)
      .hasArgs()
      .longOpt("thread-number")
      .desc("Number of threads to execute the read testing")
      .build();
  private static final Option ITERATION_OPTION = Option.builder(ITERATION_OPTION_NAME)
      .required(true)
      .hasArgs()
      .longOpt("iteration")
      .desc("Number of iterations to execute the read testing")
      .build();


  private static final Options OPTIONS = new Options()
      .addOption(LOCAL_FOLDER_OPTION)
      .addOption(FUSE_FOLDER_OPTION)
      .addOption(THREAD_NUMBER_OPTION)
      .addOption(ITERATION_OPTION)
      .addOption(HELP_OPTION);

  public static void main(String[] args) throws Exception {
    CommandLine cli = PARSER.parse(OPTIONS, args);
    String localFolder = cli.getOptionValue(LOCAL_FOLDER_OPTION_NAME);
    String fuseFolder = cli.getOptionValue(FUSE_FOLDER_OPTION_NAME);
    int threadNum = Integer.parseInt(cli.getOptionValue(THREAD_NUMBER_OPTION_NAME));
    int iteration = Integer.parseInt(cli.getOptionValue(ITERATION_OPTION_NAME));
    if (threadNum == 1) {
      runSingleThreadFullTest(localFolder, fuseFolder, iteration);
      return;
    }
    runMultiThreadSpeedUpTest(localFolder, fuseFolder, iteration);
  }

  private static void runSingleThreadFullTest(String localFolder, String fuseFolder, int iteration) throws IOException {
    for (int i = 0; i < iteration; i++) {
      for (long fileSize : FILE_SIZES) {
        for (long bufferSize : WRITE_BUFFER_SIZES) {
          SequentialWriteTest.sequentialWriteSingleFile(localFolder, fuseFolder, fileSize, bufferSize);
        }
      }
    }
  }
  
  private static void runMultiThreadSpeedUpTest(String localFolder, String fuseFolder, int iteration) throws IOException, InterruptedException {
    long[] local_file_sizes = {63 * MB, 65 * MB, GB, 10 * GB};
    long[] local_buffer_sizes = {128, 1000, 1001, MB, 1025, 4 * KB, 32 * KB, 128 * KB, MB, 4 * MB};
    for (int i = 0; i < iteration; i++) {
      List<Thread> threads = new ArrayList<>(local_file_sizes.length * local_buffer_sizes.length);
      for (long fileSize : FILE_SIZES) {
        for (long bufferSize : WRITE_BUFFER_SIZES) {
          Thread thread = new Thread(() ->
          {
            try {
              SequentialWriteTest.sequentialWriteSingleFile(localFolder, fuseFolder, fileSize, bufferSize);
            } catch (IOException e) {
              e.printStackTrace();
            }
          });
          thread.start();
          threads.add(thread);
        }
      }
      for (Thread thread : threads) {
        thread.join();
      }
    }
  }
}

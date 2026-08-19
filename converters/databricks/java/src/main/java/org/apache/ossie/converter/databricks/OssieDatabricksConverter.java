/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ossie.converter.databricks;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Command-line entry point for the converter between Apache Ossie and Databricks Metric Views.
 *
 * <pre>{@code
 *   ossie-databricks import <model.yaml> [-o <view.yaml>] [--source <dataset>]
 *   ossie-databricks export <view.yaml>  [-o <model.yaml>] [--name <model>]
 * }</pre>
 *
 * <p>{@code import} converts an Apache Ossie semantic model to a Metric View; {@code export}
 * converts a Metric View to an Apache Ossie model. Output goes to the {@code -o} file, or to stdout
 * when omitted. Conversion notices (features dropped on import) are written to stderr. A broken
 * input raises {@link OssieConverter.ConversionException}, reported as a non-zero exit.
 *
 * <p>This is a thin command-line wrapper around {@link OssieConverter}: it parses arguments, reads
 * the input YAML, invokes the library, and writes the result. Programmatic callers should use
 * {@link OssieConverter} directly.
 */
public final class OssieDatabricksConverter {

  private OssieDatabricksConverter() {}

  public static void main(String[] args) {
    try {
      run(args, System.out, System.err);
    } catch (ExitException e) {
      System.err.println(e.getMessage());
      System.exit(e.code);
    } catch (OssieConverter.ConversionException e) {
      System.err.println("Conversion failed: " + e.getMessage());
      System.exit(1);
    }
  }

  /** Testable core: parses args, runs the conversion, and writes output. */
  static void run(String[] args, PrintStream out, PrintStream err) {
    if (args.length == 0) {
      throw new ExitException(2, usage());
    }
    String command = args[0];
    Args parsed = Args.parse(args);

    String input = read(parsed.inputPath);
    OssieConverter.Result result;
    switch (command) {
      case "import":
        // Apache Ossie -> Metric View. `--source` picks the fact/grain (optional).
        result = OssieConverter.convertOssieToMetricView(input, parsed.option);
        break;
      case "export":
        // Metric View -> Apache Ossie. `--name` sets the model name (optional).
        result = OssieConverter.convertMetricViewToOssie(input, parsed.option);
        break;
      default:
        throw new ExitException(2, "Unknown command '" + command + "'.\n" + usage());
    }

    write(parsed.outputPath, result.yaml, out);
    List<String> notices = result.notices;
    if (!notices.isEmpty()) {
      err.println("Conversion notices (" + notices.size() + "):");
      for (String notice : notices) {
        err.println("  " + notice);
      }
    }
  }

  private static String read(String path) {
    try {
      return Files.readString(Path.of(path), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new ExitException(1, "Cannot read input file '" + path + "': " + e.getMessage());
    }
  }

  private static void write(String path, String content, PrintStream out) {
    if (path == null) {
      out.println(content);
      return;
    }
    try {
      Files.writeString(Path.of(path), content, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new ExitException(1, "Cannot write output file '" + path + "': " + e.getMessage());
    }
  }

  private static String usage() {
    return "Usage:\n"
        + "  ossie-databricks import <model.yaml> [-o <view.yaml>] [--source <dataset>]\n"
        + "  ossie-databricks export <view.yaml>  [-o <model.yaml>] [--name <model>]";
  }

  /** Parsed command-line arguments: the input file, an optional output file, and the option. */
  private static final class Args {
    final String inputPath;
    final String outputPath;
    final String option;

    private Args(String inputPath, String outputPath, String option) {
      this.inputPath = inputPath;
      this.outputPath = outputPath;
      this.option = option;
    }

    static Args parse(String[] args) {
      String inputPath = null;
      String outputPath = null;
      String option = null;
      for (int i = 1; i < args.length; i++) {
        String arg = args[i];
        switch (arg) {
          case "-o":
          case "--output":
            outputPath = requireValue(args, ++i, arg);
            break;
          case "--source":
          case "--name":
            option = requireValue(args, ++i, arg);
            break;
          default:
            if (arg.startsWith("-")) {
              throw new ExitException(2, "Unknown option '" + arg + "'.\n" + usage());
            }
            if (inputPath != null) {
              throw new ExitException(2, "Unexpected extra argument '" + arg + "'.\n" + usage());
            }
            inputPath = arg;
        }
      }
      if (inputPath == null) {
        throw new ExitException(2, "Missing input file.\n" + usage());
      }
      return new Args(inputPath, outputPath, option);
    }

    private static String requireValue(String[] args, int index, String flag) {
      if (index >= args.length) {
        throw new ExitException(2, "Option '" + flag + "' requires a value.\n" + usage());
      }
      return args[index];
    }
  }

  /** Signals a clean CLI exit with a message and status code (kept out of the library core). */
  static final class ExitException extends RuntimeException {
    final int code;

    ExitException(int code, String message) {
      super(message);
      this.code = code;
    }
  }
}

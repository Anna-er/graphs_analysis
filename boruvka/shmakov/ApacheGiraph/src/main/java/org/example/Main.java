package org.example;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.example.mst.BoruvkaMSTComputation;
import org.example.mst.LogMstWeightMasterCompute;
import org.example.mst.input.LongDoubleTextEdgeInputFormat;
import org.apache.hadoop.mapreduce.counters.Limits;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {
        String input = null;
        String output = null;
        int threads = Math.max(1, Runtime.getRuntime().availableProcessors());

        for (int i = 0; i < args.length; i++) {
            if ("--input".equals(args[i]) && i + 1 < args.length) input = args[++i];
            else if ("--output".equals(args[i]) && i + 1 < args.length) output = args[++i];
            else if ("--threads".equals(args[i]) && i + 1 < args.length) threads = Integer.parseInt(args[++i]);
        }
        if (input == null) {
            System.err.println("Usage: java -jar test_giraph_2-...-shaded.jar --input graph.edgelist [--output /tmp/out] [--threads N]");
            System.exit(2);
        }

        List<String> edgeLines = Files.readAllLines(Paths.get(input));
        String[] edgeList = edgeLines.toArray(new String[0]);

        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setComputationClass(BoruvkaMSTComputation.class);
        conf.setMasterComputeClass(LogMstWeightMasterCompute.class);
        conf.setEdgeInputFormatClass(LongDoubleTextEdgeInputFormat.class);

        conf.setWorkerConfiguration(1, 1, 100.0f);
        conf.setBoolean("giraph.isLocal", true);
        conf.setInt("giraph.numComputeThreads", threads);

        conf.setInt("giraph.numInputSplitsThreads", threads);
        conf.setInt("giraph.numOutputThreads", 1);
        conf.setBoolean("giraph.metrics.enable", false);
        conf.setBoolean("giraph.splitMasterWorker", false);

        conf.setInt("mapreduce.job.counters.max", 5000);
        Limits.init(conf);

        int partitions = threads * 10;
        if (partitions < 50) partitions = 50;
        conf.setInt("giraph.userPartitionCount", partitions);

        conf.setBoolean("giraph.useUnsafeSerialization", true);

        System.out.println("[Main] In-process run with " + threads + " threads, " + partitions + " partitions");
        long t0 = System.nanoTime();

        Iterable<String> result = InternalVertexRunner.run(conf, null, edgeList);

        long t1 = System.nanoTime();
        double secs = (t1 - t0) / 1e9;
        System.out.printf("[Main] In-process job finished. Total wall time: %.3f s%n", secs);

        if (output != null && result != null) {
            java.nio.file.Path outDir = Paths.get(output);
            java.nio.file.Files.createDirectories(outDir);
            java.nio.file.Path outFile = outDir.resolve("part-00000");
            java.nio.file.Files.write(outFile, toList(result));
            System.out.println("[Main] Wrote output to " + outFile.toAbsolutePath());
        }
    }

    private static java.util.List<String> toList(Iterable<String> it) {
        java.util.ArrayList<String> list = new java.util.ArrayList<>();
        for (String s : it) list.add(s);
        return list;
    }
}

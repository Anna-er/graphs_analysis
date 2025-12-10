package org.example.mst.tools;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GrToMtx {

    static class Edge {
        long u;
        long v;
        double w;

        Edge(long u, long v, double w) {
            this.u = u;
            this.v = v;
            this.w = w;
        }
    }

    public static void main(String[] args) throws Exception {
        String in = null, out = null;
        for (int i = 0; i < args.length; i++) {
            if ("--input".equals(args[i]) && i + 1 < args.length) in = args[++i];
            else if ("--output".equals(args[i]) && i + 1 < args.length) out = args[++i];
        }
        if (in == null || out == null) {
            System.err.println("Usage: java ... GrToMtx --input graph.gr --output graph.mtx");
            System.exit(2);
        }

        Map<String, Double> uniqueEdges = new HashMap<>();
        long maxVertexId = 0;

        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(in), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("c") || line.startsWith("p")) continue;

                if (line.startsWith("a")) {
                    String[] toks = line.split("\\s+");
                    if (toks.length >= 4) {
                        long u = Long.parseLong(toks[1]);
                        long v = Long.parseLong(toks[2]);
                        double w = Double.parseDouble(toks[3]);

                        if (u == v) continue; 

                        if (u > maxVertexId) maxVertexId = u;
                        if (v > maxVertexId) maxVertexId = v;

                        long min = Math.min(u, v);
                        long max = Math.max(u, v);
                        String key = min + "_" + max;

                        uniqueEdges.put(key, w);
                    }
                }
            }
        }

        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(out), StandardCharsets.UTF_8))) {
            bw.write("%%MatrixMarket matrix coordinate real symmetric");
            bw.newLine();

            bw.write(maxVertexId + " " + maxVertexId + " " + uniqueEdges.size());
            bw.newLine();

            for (Map.Entry<String, Double> entry : uniqueEdges.entrySet()) {
                String[] parts = entry.getKey().split("_");
                long u = Long.parseLong(parts[0]);
                long v = Long.parseLong(parts[1]);
                double w = entry.getValue();

                bw.write(v + " " + u + " " + w);
                bw.newLine();
            }
        }

        System.out.println("Converted " + in + " -> " + out + " (Vertices: " + maxVertexId + ", Unique Edges: " + uniqueEdges.size() + ")");
    }
}

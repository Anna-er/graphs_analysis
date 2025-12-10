package org.example.mst.tools;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class MtxToEdgeList {
    public static void main(String[] args) throws Exception {
        String in = null, out = null;
        for (int i = 0; i < args.length; i++) {
            if ("--input".equals(args[i]) && i + 1 < args.length) in = args[++i];
            else if ("--output".equals(args[i]) && i + 1 < args.length) out = args[++i];
        }
        if (in == null || out == null) {
            System.err.println("Usage: java ... MtxToEdgeList --input graph.mtx --output graph.edgelist");
            System.exit(2);
        }

        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(in), StandardCharsets.UTF_8));
             BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(out), StandardCharsets.UTF_8))) {

            String line;
            boolean headerSeen = false;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("%")) continue;
                if (!headerSeen) { headerSeen = true; continue; } // skip size line

                String[] toks = line.split("\\s+");
                if (toks.length < 2) continue;
                long i = Long.parseLong(toks[0]);
                long j = Long.parseLong(toks[1]);
                double w = (toks.length >= 3) ? Double.parseDouble(toks[2]) : 1.0;
                if (i == j) continue;

                bw.write(i + " " + j + " " + w); bw.newLine();
                bw.write(j + " " + i + " " + w); bw.newLine();
            }
        }
    }
}

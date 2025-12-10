package org.example.mst.input;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;


public class LongDoubleTextEdgeInputFormat extends TextEdgeInputFormat<LongWritable, DoubleWritable> {

    @Override
    public EdgeReader<LongWritable, DoubleWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new TextEdgeReaderFromEachLine() {
            private final LongWritable src = new LongWritable();
            private final LongWritable dst = new LongWritable();
            private final DoubleWritable weight = new DoubleWritable();
            private String lastLine = null;

            @Override
            protected LongWritable getSourceVertexId(Text line) throws IOException {
                parse(line);
                return src;
            }

            @Override
            protected LongWritable getTargetVertexId(Text line) throws IOException {
                parse(line);
                return dst;
            }

            @Override
            protected DoubleWritable getValue(Text line) throws IOException {
                parse(line);
                return weight;
            }

            private void parse(Text line) throws IOException {
                String s = line.toString().trim();
                if (s.equals(lastLine)) return; // already parsed
                lastLine = s;
                if (s.isEmpty()) throw new IOException("Empty line in edge list is not allowed");
                String[] toks = s.split("\\s+");
                if (toks.length < 3) throw new IOException("Expected: src dst weight, got: " + s);
                long u = Long.parseLong(toks[0]);
                long v = Long.parseLong(toks[1]);
                double w = Double.parseDouble(toks[2]);
                src.set(u);
                dst.set(v);
                weight.set(w);
            }
        };
    }
}

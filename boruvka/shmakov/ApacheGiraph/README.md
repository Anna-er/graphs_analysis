# Boruvka algorithm \[Apache Giraph\]

## Build

```bash
mvn clean package
```

## Run

### Convert graph in `.mtx` format to `.edgelist`

Example for:

- `USA-road-d.CAL.mtx`
- Example output name: `sample_converted.edgelist`

```bash
java -cp target/boruvka-giraph-1.0.0.jar org.example.mst.tools.MtxToEdgeList \
    --input ../graphs_mtx/USA-road-d.CAL.mtx \
    --output sample_converted.edgelist
```

### Run for a certain graph in `.edgelist` format

Example for:

- Graph `sample_converted.edgelist`
- Running on 22 threads

```bash
java -jar target/boruvka-giraph-1.0.0.jar \
    --input sample_converted.edgelist \
    --output /tmp/mst-mtx-out \
    --threads 22
```

### Run on specific processor cores using `taskset`

Example for:

- Graph `sample_converted.edgelist`
- Performance cores on my machine (Intel Core Ultra 9 185H)

```bash
taskset -c 0,1,3,6,8,10 \
  java -XX:ActiveProcessorCount=6 \
       -jar target/boruvka-giraph-1.0.0.jar \
       --input sample_converted.edgelist \
       --output /tmp/mst-mtx-out \
       --threads 6
```

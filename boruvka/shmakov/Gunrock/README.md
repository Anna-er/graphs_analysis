# Boruvka algorithm \[Gunrock\]

## Build

```bash
cmake -S . -B build && cmake --build build
```

## Run

> Takes graphs in `.mtx` format as input

Example for:

- Graph `sample_graph.mtx`

```bash
./build/bin/boruvka sample_graph.mtx
```

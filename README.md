
Lauda parallel data load utility

```
lauda [-h] [--truncate] [-a=<sourceTable>] [-b=<targetTable>]
                    [-c=<config>] [-p=<parallel>] [-s=<source>]
                    [--source_parallel=<sourceParallel>] [-t=<target>]
                    [--target_parallel=<targetParallel>]
  -a, --source_table=<sourceTable>
                          Source table to name
  -b, --target_table=<targetTable>
                          Target table to name
  -c, --config=<config>   config file (yaml)
  -h, --help              display help message
  -p, --parallel=<parallel>
                          number of parallel transfer jobs
  -s, --source=<source>   source connector
      --source_parallel=<sourceParallel>
                          number of parallel source jobs
  -t, --target=<target>   target connector
      --target_parallel=<targetParallel>
                          number of parallel target jobs
      --truncate          Truncate table to name
```

example:



```sh
./lauda.sh \
    --source=jdbc_source \
    --target=oracle_1 \
    --parallel=16 \
    --source_table=lauda.demo_1 \
    --target_table=lauda.lauda_target \
    --truncate \
    --filter="col_int>1000"
```

```sh
./lauda.sh \
    --source=jdbc_source \
    --target=file_1 \
    --parallel=4 \
    --source_table=lauda.demo_1
```
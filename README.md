jboss-threads benchmarking with various lock configurations

Note: the CoreAffinity.java class shells out to run `taskset -a -p <mask> <java pid>`. This
requires JRE9+ runtime, however the code can build on jdk8.

*TODO: regenerate full results, this output is very stale!*

Initial results:
```
Benchmark                           (cores)      (lockMode)   Mode  Cnt        Score        Error  Units
ExecutorBenchmarks.benchmarkSubmit        1        NO_LOCKS  thrpt    3   175427.334 ±  99633.942  ops/s
ExecutorBenchmarks.benchmarkSubmit        1       SPIN_LOCK  thrpt    3   136126.723 ± 832505.826  ops/s
ExecutorBenchmarks.benchmarkSubmit        1  REENTRANT_LOCK  thrpt    3   166802.606 ± 220491.622  ops/s
ExecutorBenchmarks.benchmarkSubmit        2        NO_LOCKS  thrpt    3   238119.673 ±  57374.632  ops/s
ExecutorBenchmarks.benchmarkSubmit        2       SPIN_LOCK  thrpt    3   163173.290 ± 479121.888  ops/s
ExecutorBenchmarks.benchmarkSubmit        2  REENTRANT_LOCK  thrpt    3   202860.295 ± 224134.046  ops/s
ExecutorBenchmarks.benchmarkSubmit        4        NO_LOCKS  thrpt    3   389107.805 ± 277479.225  ops/s
ExecutorBenchmarks.benchmarkSubmit        4       SPIN_LOCK  thrpt    3    82818.469 ±  70899.526  ops/s
ExecutorBenchmarks.benchmarkSubmit        4  REENTRANT_LOCK  thrpt    3   220755.634 ±   9220.538  ops/s
ExecutorBenchmarks.benchmarkSubmit        8        NO_LOCKS  thrpt    3   727343.230 ±  73120.671  ops/s
ExecutorBenchmarks.benchmarkSubmit        8       SPIN_LOCK  thrpt    3   217107.857 ±  31126.494  ops/s
ExecutorBenchmarks.benchmarkSubmit        8  REENTRANT_LOCK  thrpt    3   211775.638 ±  46812.757  ops/s
ExecutorBenchmarks.benchmarkSubmit       14        NO_LOCKS  thrpt    3  1112300.950 ± 103541.988  ops/s
ExecutorBenchmarks.benchmarkSubmit       14       SPIN_LOCK  thrpt    3   204114.786 ±  38581.330  ops/s
ExecutorBenchmarks.benchmarkSubmit       14  REENTRANT_LOCK  thrpt    3   208072.127 ±  33223.526  ops/s
ExecutorBenchmarks.benchmarkSubmit       28        NO_LOCKS  thrpt    3  1257170.980 ± 196998.383  ops/s
ExecutorBenchmarks.benchmarkSubmit       28       SPIN_LOCK  thrpt    3   202660.260 ±  46212.712  ops/s
ExecutorBenchmarks.benchmarkSubmit       28  REENTRANT_LOCK  thrpt    3   201910.260 ±   1235.492  ops/s
```

Results using EnhancedQueueExecutor from jboss-threads cebaabe28ddb69f6316229cb44d561871eaa93e9
```
Benchmark                           (cores)      (lockMode)   Mode  Cnt        Score        Error  Units
ExecutorBenchmarks.benchmarkSubmit        1        NO_LOCKS  thrpt    3   177682.503 ±  29152.631  ops/s
ExecutorBenchmarks.benchmarkSubmit        1       SPIN_LOCK  thrpt    3   138459.142 ± 910710.466  ops/s
ExecutorBenchmarks.benchmarkSubmit        1  REENTRANT_LOCK  thrpt    3   175306.322 ±  49142.728  ops/s
ExecutorBenchmarks.benchmarkSubmit        2        NO_LOCKS  thrpt    3   233056.306 ±  97009.830  ops/s
ExecutorBenchmarks.benchmarkSubmit        2       SPIN_LOCK  thrpt    3   137316.373 ± 934090.351  ops/s
ExecutorBenchmarks.benchmarkSubmit        2  REENTRANT_LOCK  thrpt    3   195180.037 ±  49247.649  ops/s
ExecutorBenchmarks.benchmarkSubmit        4        NO_LOCKS  thrpt    3   404299.120 ±  15981.076  ops/s
ExecutorBenchmarks.benchmarkSubmit        4       SPIN_LOCK  thrpt    3    82607.299 ± 236308.555  ops/s
ExecutorBenchmarks.benchmarkSubmit        4  REENTRANT_LOCK  thrpt    3   220588.659 ±   9097.295  ops/s
ExecutorBenchmarks.benchmarkSubmit        8        NO_LOCKS  thrpt    3   719863.985 ±  72185.815  ops/s
ExecutorBenchmarks.benchmarkSubmit        8       SPIN_LOCK  thrpt    3   217386.154 ±  38372.337  ops/s
ExecutorBenchmarks.benchmarkSubmit        8  REENTRANT_LOCK  thrpt    3   220711.843 ±  39463.419  ops/s
ExecutorBenchmarks.benchmarkSubmit       14        NO_LOCKS  thrpt    3  1145361.605 ± 172351.992  ops/s
ExecutorBenchmarks.benchmarkSubmit       14       SPIN_LOCK  thrpt    3   207927.066 ±  28230.226  ops/s
ExecutorBenchmarks.benchmarkSubmit       14  REENTRANT_LOCK  thrpt    3   204606.787 ±  57407.248  ops/s
ExecutorBenchmarks.benchmarkSubmit       28        NO_LOCKS  thrpt    3  1280977.712 ± 167202.094  ops/s
ExecutorBenchmarks.benchmarkSubmit       28       SPIN_LOCK  thrpt    3   207360.516 ±  19624.459  ops/s
ExecutorBenchmarks.benchmarkSubmit       28  REENTRANT_LOCK  thrpt    3   206660.768 ±  12704.079  ops/s
```

Results using `synchronized` (modificaiton on top of cebaabe28ddb69f6316229cb44d561871eaa93e9, not checked in):
```
Benchmark                           (cores)    (lockMode)   Mode  Cnt       Score        Error  Units
ExecutorBenchmarks.benchmarkSubmit        1  SYNCHRONIZED  thrpt    3  174014.211 ±  29150.313  ops/s
ExecutorBenchmarks.benchmarkSubmit        2  SYNCHRONIZED  thrpt    3  229332.544 ± 111243.731  ops/s
ExecutorBenchmarks.benchmarkSubmit        4  SYNCHRONIZED  thrpt    3  284431.888 ±  67850.596  ops/s
ExecutorBenchmarks.benchmarkSubmit        8  SYNCHRONIZED  thrpt    3  405161.611 ±  42873.820  ops/s
ExecutorBenchmarks.benchmarkSubmit       14  SYNCHRONIZED  thrpt    3  385375.220 ±  38420.860  ops/s
ExecutorBenchmarks.benchmarkSubmit       28  SYNCHRONIZED  thrpt    3  362371.843 ±  27700.167  ops/s
```

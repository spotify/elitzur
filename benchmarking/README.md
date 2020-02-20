## Benchmarking

Benchmarking with JMH can be done with:

```sbtshell
sbt:validation> benchmarking/jmh:run -i 20 -wi 10 -f1 -t1 .*Benchmarking.*
```
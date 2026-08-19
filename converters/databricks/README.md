Ossie <-> Metric View converters
=================================

A bidirectional converter between [Apache Ossie](https://github.com/apache/ossie) semantic models
and Databricks Unity Catalog Metric Views (YAML v1.1). Conversion is pure YAML text in, YAML text
out: it reads and writes the two formats as parsed maps and lists, independent of any engine.

Layout
------

| Path | Language | Role |
|------|----------|------|
| [`java/`](java/) | Java | The maintained implementation; also ships a command-line tool (`OssieDatabricksConverter`). |
| [`python/`](python/) | Python | The original reference implementation. To be deprecated. |

See [`java/README.md`](java/README.md) and [`python/README.md`](python/README.md) for building and
using each implementation.

# Apache Ossie Databricks Converter

Bidirectional, offline conversion between an [Apache Ossie](https://github.com/apache/ossie)
semantic model and a Databricks
[Unity Catalog Metric View](https://docs.databricks.com/aws/en/metric-views/) (YAML `1.1`). Pure
YAML text in, YAML text out: it reads and writes the two formats as parsed maps and lists.

- **Export** (`MetricViewToOssie`): Metric View -> Apache Ossie. The direction is named from the
  Metric View's point of view -- it takes a Metric View *out* to Ossie. Metric-View-only features
  Apache Ossie has no native field for are preserved in `custom_extensions[DATABRICKS]`, so
  `MV -> Apache Ossie -> MV` is lossless.
- **Import** (`OssieToMetricView`): Apache Ossie -> Metric View (one fact `source` with a nested
  `joins` tree and a flat `dimensions` list).

On **import** (Apache Ossie -> Metric View), Apache Ossie features with no Metric View slot --
relationship `ai_context`, `dimension.is_time`, non-`DATABRICKS`/`ANSI_SQL` dialects, foreign-vendor
`custom_extensions` -- are **dropped with a notice**. On **export** (Metric View -> Apache Ossie),
Metric-View-only features (`filter`, `parameters`, `materialization`, per-column `format`, measure
`window` / `partition`) are instead **preserved** in `custom_extensions[DATABRICKS]`, so
`MV -> Apache Ossie -> MV` is lossless. Any input that breaks a [requirement](#requirements)
**raises a `ConversionException`** -- the converter never silently drops a field or produces an
invalid result.

## Requirements

- **Java 21+**
- **Maven 3.6+** -- required to build the jar

## Building

Build the self-contained executable jar from source:

```bash
mvn clean package
```

This produces `target/ossie-databricks-converter-0.1.0-SNAPSHOT.jar` with all dependencies
(Jackson and SnakeYAML) bundled.

## Usage

### Command line

```bash
# import: Apache Ossie -> Metric View
java -jar target/ossie-databricks-converter-0.1.0-SNAPSHOT.jar import model.yaml -o view.yaml

# export: Metric View -> Apache Ossie
java -jar target/ossie-databricks-converter-0.1.0-SNAPSHOT.jar export view.yaml -o model.yaml
```

With no `-o`, output goes to stdout. `--source` (import) picks the fact/grain (default: the FK-sink
dataset; naming a coarser-grain dataset produces `one_to_many` joins); `--name` (export) sets the
Apache Ossie model name (default: the source's last identifier). Conversion notices (features
dropped on import) are written to stderr; a non-convertible input exits non-zero.

### Java API

```java
import org.apache.ossie.converter.databricks.OssieConverter;

// export: Metric View -> Apache Ossie (optionally name the model; default: the source's last part)
OssieConverter.Result ossie = OssieConverter.convertMetricViewToOssie(metricViewYaml, "sales");

// import: Apache Ossie -> Metric View (optionally choose the fact/grain; default: the FK-sink
// dataset -- naming a coarser-grain dataset produces one_to_many joins)
OssieConverter.Result view = OssieConverter.convertOssieToMetricView(ossieYaml, "orders");
```

Each `Result` carries the output YAML (`result.yaml`) and any notices raised (`result.notices`,
the features dropped on import). A broken [requirement](#requirements) throws a
`ConversionException` instead.

## Mapping

Each row maps in both directions; the **Notes** flag where a behavior is specific to
**export** (Metric View -> Apache Ossie) or **import** (Apache Ossie -> Metric View).

| Apache Ossie | Metric View (v1.1) | Notes |
|---|---|---|
| `semantic_model.description` | `comment` | Model-level description only. |
| root dataset | `source` | The fact/grain. |
| other `datasets` | nested `joins[]` | Import: the relationship graph is reassembled into the join tree; a dataset reached by two paths (a diamond) fans out into one aliased join per path. |
| `relationship` `from_columns`/`to_columns` | join `on` (differing names) / `using` (shared names) | Decomposed into columns on export; rebuilt into `on`/`using` on import. |
| `relationship.from`/`to` direction | join `cardinality` | Import: source on the many (`from`) side -> `many_to_one`; on the one (`to`) side -> `one_to_many`. |
| `dataset.primary_key` / `unique_keys` | join `rely.at_most_one_match` | Both directions: import sets `at_most_one_match` when a key covers the join columns; export recovers a `unique_keys` from it. |
| `dataset.fields[]` | `dimensions[]` | Import: fields flatten into one list and a joined column is qualified by its full join path (`customer.c_name`; `customer.region.r_name` when nested). |
| `field.expression.dialects[]` | `expr` | Import: prefer the `DATABRICKS` dialect, else `ANSI_SQL`. |
| `metrics[]` | `measures[]` | Import: fact columns are referenced bare (`SUM(amount)`). |
| `field.label` | `display_name` | |
| `field` / `metric` `description` | `comment` | |
| `ai_context.synonyms` | `synonyms` | |
| `custom_extensions[DATABRICKS]` | `filter`, `parameters`, `materialization`, per-column `format`, measure `window` / `partition` | Export stashes Metric-View-only features here; import restores them -- keeping `MV -> Apache Ossie -> MV` lossless. |

## Requirements

Conversion throws a `ConversionException` (rather than guessing or emitting something invalid) when
an input breaks one of these:

- the Metric View `version` is not `1.1`;
- a `source` is not a 3-part `catalog.schema.table` name or a `SELECT`/`WITH` subquery;
- the relationship graph is not acyclic and resolvable to a single fact -- a cycle, or multiple
  candidate facts without a chosen source, is rejected (a diamond is allowed and fanned out);
- a join has no condition (a cross join has no Apache Ossie relationship form);
- a join condition is non-equi or otherwise can't be decomposed into equi-join columns (Apache
  Ossie relationships are equi-joins, so the join has no Apache Ossie representation);
- the input YAML is malformed.

## Development

Run the test suite:

```bash
mvn test
```

JUnit 5 suites (unit + round-trip) live under `src/test/java/`, with the YAML fixtures in
`src/test/resources/`. The source layout:

```
src/main/java/org/apache/ossie/converter/databricks/
  OssieConverter.java           public facade: entry points + ConversionException/Notices/Result
  OssieConverterCommon.java     shared constants, YAML I/O, map accessors, the stash codec
  MetricViewToOssie.java        export: Metric View v1.1 -> Apache Ossie
  OssieToMetricView.java        import: Apache Ossie -> Metric View v1.1
  OssieDatabricksConverter.java command-line entry point (import / export)
```

The authoritative contract is Metric View YAML v1.1 as Databricks defines it; the checked-in
fixtures under `src/test/resources/` pin the expected output of both directions.

## Future effort

Both the Apache Ossie specification and the Databricks Unity Catalog Metric View YAML are still
evolving. As either side adds or changes fields, this converter will be updated to track them --
extending the mapping and coverage in both directions to keep the conversion current and to support
as much as each format allows over time.

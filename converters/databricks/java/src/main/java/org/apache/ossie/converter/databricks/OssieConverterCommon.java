/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ossie.converter.databricks;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.resolver.Resolver;

import org.apache.ossie.converter.databricks.OssieConverter.ConversionException;

/**
 * Shared constants, YAML I/O, and typed accessors for the Apache Ossie &lt;-&gt; Databricks
 * Metric View converter. The direction-specific logic lives in {@link OssieToMetricView}
 * (export) and {@link MetricViewToOssie} (import), which static-import these members. The public
 * entry points and shared types are re-exported through {@link OssieConverter}.
 */
// Map-based YAML manipulation: casts of the parsed Object graph to Map/List are inherently
// unchecked; the asMap/asList helpers guard them, so unchecked warnings here are expected.
@SuppressWarnings("unchecked")
final class OssieConverterCommon {

  // -- constants -------------------------------------------------------------
  static final String OSSIE_VERSION = "0.2.0.dev0";
  static final String MV_VERSION = "1.1";
  static final String VENDOR = "DATABRICKS";
  static final String DIALECT_DATABRICKS = "DATABRICKS";
  static final String DIALECT_ANSI = "ANSI_SQL";
  static final int SYNONYM_LIMIT = 10;
  static final int STASH_VERSION = 1;
  static final String STASH_SOURCE_KEY = "source_dataset";
  static final String CARD_ONE_TO_MANY = "one_to_many";
  static final String CARD_MANY_TO_ONE = "many_to_one";
  static final int MAX_JOIN_NODES = 200;

  static final Pattern IDENTIFIER_RE = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");
  static final Pattern SELECT_WITH_RE =
      Pattern.compile("(?i)^(select|with)\\b");

  static final ObjectMapper MAPPER = buildMapper();
  // Writer for the custom_extensions stash blob. The exact byte format is pinned by the
  // checked-in fixtures: a space after ':' and ', ' between entries, as in
  // {"_v": 1, "filter": "x"}.
  static final com.fasterxml.jackson.databind.ObjectWriter JSON_WRITER = buildJsonWriter();

  /** A MinimalPrettyPrinter (no newlines) using the stash blob's separators: ": " / ", ". */
  private static final class JsonDumpsPrinter
      extends com.fasterxml.jackson.core.util.MinimalPrettyPrinter {
    @Override
    public void writeObjectFieldValueSeparator(com.fasterxml.jackson.core.JsonGenerator g)
        throws java.io.IOException {
      g.writeRaw(": ");
    }
    @Override
    public void writeObjectEntrySeparator(com.fasterxml.jackson.core.JsonGenerator g)
        throws java.io.IOException {
      g.writeRaw(", ");
    }
    @Override
    public void writeArrayValueSeparator(com.fasterxml.jackson.core.JsonGenerator g)
        throws java.io.IOException {
      g.writeRaw(", ");
    }
  }

  private static com.fasterxml.jackson.databind.ObjectWriter buildJsonWriter() {
    // ESCAPE_NON_ASCII keeps the blob pure ASCII (every non-ASCII
    // char is emitted as a \\uXXXX escape). Set on the JsonFactory so it takes effect on the
    // generator. Jackson emits the hex in UPPERCASE; writeStash lowercases it so the blob is
    // byte-identical to the checked-in fixtures.
    com.fasterxml.jackson.core.JsonFactory jf = new com.fasterxml.jackson.core.JsonFactory();
    jf.enable(com.fasterxml.jackson.core.JsonGenerator.Feature.ESCAPE_NON_ASCII);
    return new ObjectMapper(jf).writer(new JsonDumpsPrinter());
  }

  // Matches a real unicode escape in serialized JSON so writeStash can lowercase its hex digits.
  //
  // The escape must be preceded by an EVEN number of backslashes, otherwise the `u` belongs to an
  // escaped backslash rather than to an escape sequence: a stashed value holding a literal
  // backslash followed by "uABCD" serializes with a doubled backslash, where the `u` is ordinary
  // text and must be left alone (lowercasing it there would corrupt the value). Group 1 captures
  // the (possibly empty) run of escaped backslashes so it can be re-emitted verbatim; group 2 is
  // the hex to lowercase.
  private static final Pattern UNICODE_ESCAPE_RE =
      Pattern.compile("(?<!\\\\)((?:\\\\\\\\)*)\\\\u([0-9A-Fa-f]{4})");

  private static ObjectMapper buildMapper() {
    // Quote scalars (do NOT minimize quotes) so a string like "1.1" or a bool-like token
    // is emitted quoted and never re-parsed as a number/boolean. Used for the WRITE path only;
    // reads go through the YAML-1.2 SnakeYAML loader below.
    YAMLFactory yf = YAMLFactory.builder()
        .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
        .build();
    return new ObjectMapper(yf);
  }

  // A SnakeYAML Resolver with YAML 1.2 boolean semantics: only true/false are booleans, NOT the
  // YAML 1.1 tokens on/off/yes/no/y/n. A metric view join's bare `on:` key, and any "on"/"off"
  // string value, must read back as a string rather than silently becoming a boolean (a YAML 1.1
  // reader would coerce it).
  private static final class Yaml12Resolver extends Resolver {
    @Override
    protected void addImplicitResolvers() {
      addImplicitResolver(Tag.MERGE, MERGE, "<");
      addImplicitResolver(Tag.NULL, NULL, "~nN ");
      addImplicitResolver(Tag.NULL, EMPTY, null);
      addImplicitResolver(Tag.BOOL, java.util.regex.Pattern.compile(
          "^(?:true|True|TRUE|false|False|FALSE)$"), "tTfF");
      addImplicitResolver(Tag.INT, INT, "-+0123456789");
      addImplicitResolver(Tag.FLOAT, FLOAT, "-+0123456789.");
      addImplicitResolver(Tag.TIMESTAMP, TIMESTAMP, "0123456789");
    }
  }

  private static final ThreadLocal<Yaml> YAML_READER = ThreadLocal.withInitial(() ->
      new Yaml(new SafeConstructor(new LoaderOptions()), new org.yaml.snakeyaml.representer.Representer(
          new org.yaml.snakeyaml.DumperOptions()), new org.yaml.snakeyaml.DumperOptions(),
          new LoaderOptions(), new Yaml12Resolver()));

  static Object loadYaml(String s) {
    return YAML_READER.get().load(s);
  }

  private OssieConverterCommon() {}

  // -- typed accessors over parsed YAML (Object) ----------------------------
  @SuppressWarnings("unchecked")
  static Map<String, Object> asMap(Object x) {
    if (x instanceof Map) {
      return (Map<String, Object>) x;
    }
    return new LinkedHashMap<>();
  }

  @SuppressWarnings("unchecked")
  static List<Object> asList(Object x) {
    if (x instanceof List) {
      return (List<Object>) x;
    }
    return new ArrayList<>();
  }

  static Object get(Map<String, Object> m, String k) {
    return m.get(k);
  }

  static String str(Object x) {
    if (x == null) {
      return null;
    }
    return x.toString();
  }

  static List<String> strList(Object x) {
    List<String> out = new ArrayList<>();
    for (Object o : asList(x)) {
      if (o != null) {
        out.add(o.toString());
      }
    }
    return out;
  }

  // -- helpers ---------------------------------------------------------------
  static boolean isSimpleIdentifier(Object expr) {
    return expr instanceof String && IDENTIFIER_RE.matcher(((String) expr).trim()).matches();
  }

  /**
   * Applies {@code pattern -> replacement} to {@code sql}, but only to the parts of the expression
   * that are actual SQL code -- spans inside string literals ({@code '...'}, {@code "..."}),
   * backquoted identifiers, {@code -- line} comments, and {@code /* block *}{@code /} comments are
   * copied through untouched.
   *
   * <p>Measure expressions are rewritten to add or strip a fact qualifier, and a blind
   * {@code replaceAll} over the raw text also rewrites any occurrence inside a literal: a measure
   * such as {@code SUM(IF(source.region = 'source.us', amt, 0))} would silently become
   * {@code ... = 'us'}, changing the predicate and therefore the measure's value. Only code spans
   * may be rewritten.
   *
   * <p>{@code replacement} is treated as a literal string, not as a regex replacement template.
   */
  static String replaceOutsideLiterals(String sql, Pattern pattern, String replacement) {
    StringBuilder out = new StringBuilder(sql.length());
    int i = 0;
    int codeStart = 0;
    while (i < sql.length()) {
      char c = sql.charAt(i);
      int skipTo = -1;
      if (c == '\'' || c == '"' || c == '`') {
        skipTo = endOfQuoted(sql, i, c);
      } else if (c == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
        int nl = sql.indexOf('\n', i);
        skipTo = nl < 0 ? sql.length() : nl;
      } else if (c == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
        int end = sql.indexOf("*/", i + 2);
        skipTo = end < 0 ? sql.length() : end + 2;
      }
      if (skipTo < 0) {
        i++;
        continue;
      }
      // Rewrite the code span that precedes this literal/comment, then copy the span verbatim.
      out.append(rewriteLiterally(sql.substring(codeStart, i), pattern, replacement));
      out.append(sql, i, skipTo);
      i = skipTo;
      codeStart = skipTo;
    }
    out.append(rewriteLiterally(sql.substring(codeStart), pattern, replacement));
    return out.toString();
  }

  /** Index just past the quoted span starting at {@code start}; handles doubled-quote escapes. */
  private static int endOfQuoted(String sql, int start, char quote) {
    int i = start + 1;
    while (i < sql.length()) {
      char c = sql.charAt(i);
      if (c == '\\' && quote != '`' && i + 1 < sql.length()) {
        i += 2;
        continue;
      }
      if (c == quote) {
        // A doubled quote is an escaped quote, not the end of the span.
        if (i + 1 < sql.length() && sql.charAt(i + 1) == quote) {
          i += 2;
          continue;
        }
        return i + 1;
      }
      i++;
    }
    // Unterminated literal: treat the remainder as part of the span rather than rewriting it.
    return sql.length();
  }

  private static String rewriteLiterally(String code, Pattern pattern, String replacement) {
    return pattern.matcher(code).replaceAll(Matcher.quoteReplacement(replacement));
  }

  // Presence is tested by key, so a legitimately falsy non-string value (0, false) is returned;
  // a missing key, a null, or an empty/whitespace-only string is rejected.
  static Object require(Map<String, Object> obj, String key, String what) {
    if (!obj.containsKey(key) || obj.get(key) == null) {
      throw new ConversionException(what + " is missing required '" + key + "'");
    }
    Object value = obj.get(key);
    if (value instanceof String && ((String) value).trim().isEmpty()) {
      throw new ConversionException(what + " has an empty '" + key + "'");
    }
    return value;
  }

  // Like require(), but the value must be a string.
  static String requireStr(Map<String, Object> obj, String key, String what) {
    Object v = require(obj, key, what);
    if (v instanceof String) {
      return (String) v;
    }
    throw new ConversionException(
        what + ": '" + key + "' must be a string, got " + v.getClass().getSimpleName());
  }

  static String validateSource(Object source, String datasetName) {
    String s = source == null ? "" : source.toString().trim();
    if (s.isEmpty()) {
      throw new ConversionException("Dataset '" + datasetName + "': missing/empty 'source'");
    }
    if (SELECT_WITH_RE.matcher(s).find()) {
      return s;
    }
    String[] parts = s.split("\\.", -1);
    boolean ok = parts.length == 3;
    if (ok) {
      for (String p : parts) {
        if (p.isEmpty() || containsWhitespace(p)) {
          ok = false;
          break;
        }
      }
    }
    if (ok) {
      return s;
    }
    throw new ConversionException("Dataset '" + datasetName + "': source '" + source
        + "' must be a 3-part catalog.schema.table identifier or a SELECT/WITH subquery");
  }

  private static boolean containsWhitespace(String p) {
    for (int i = 0; i < p.length(); i++) {
      if (Character.isWhitespace(p.charAt(i))) {
        return true;
      }
    }
    return false;
  }

  static String pickExpression(Object osiExpression) {
    // Keep the raw (possibly non-string) values so
    // the type check below can fire; select DATABRICKS-or-ANSI by truthiness (`or`), so a
    // null/empty DATABRICKS expr falls through to ANSI; and raise on a non-string chosen
    // value rather than silently coercing it.
    Map<String, Object> dialects = new LinkedHashMap<>();
    for (Object d : asList(get(asMap(osiExpression), "dialects"))) {
      Map<String, Object> dm = asMap(d);
      dialects.put(str(get(dm, "dialect")), get(dm, "expression"));
    }
    Object chosen = truthy(dialects.get(DIALECT_DATABRICKS))
        ? dialects.get(DIALECT_DATABRICKS) : dialects.get(DIALECT_ANSI);
    if (chosen != null && !(chosen instanceof String)) {
      throw new ConversionException(
          "expression must be a string, got " + chosen.getClass().getSimpleName());
    }
    return (String) chosen; // null when neither dialect present -> caller warns and skips
  }

  /**
   * Emptiness test used throughout the converter for optional YAML values: null, the empty string,
   * an empty list/map, numeric zero, and boolean false are all treated as absent; everything else
   * is present. Used for the DATABRICKS-or-ANSI expression fallthrough (so an empty or absent
   * DATABRICKS expr falls through to ANSI) and for optional-field mapping (so an empty
   * `comment`/`synonyms` is dropped rather than emitted as an empty value).
   */
  static boolean truthy(Object v) {
    if (v == null) {
      return false;
    }
    if (v instanceof String) {
      return !((String) v).isEmpty();
    }
    if (v instanceof java.util.Collection) {
      return !((java.util.Collection<?>) v).isEmpty();
    }
    if (v instanceof Map) {
      return !((Map<?, ?>) v).isEmpty();
    }
    if (v instanceof Number) {
      return ((Number) v).doubleValue() != 0.0;
    }
    if (v instanceof Boolean) {
      return (Boolean) v;
    }
    return true;
  }

  static List<String> synonymsOf(Object aiContext) {
    if (aiContext instanceof Map) {
      return strList(get(asMap(aiContext), "synonyms"));
    }
    return new ArrayList<>();
  }

  static String mergeDescription(Object description, Object aiContext) {
    String desc = str(description);
    if (aiContext instanceof String && !((String) aiContext).trim().isEmpty()) {
      String s = (String) aiContext;
      // When both are present they are joined with a newline; otherwise the
      // `if description` is a truthiness test, so an empty (or null) description returns the
      // ai_context alone rather than prepending a stray newline.
      return (desc != null && !desc.isEmpty()) ? desc + "\n" + s : s;
    }
    return desc;
  }

  static Map<String, Object> readStash(Map<String, Object> obj) {
    for (Object extObj : asList(get(obj, "custom_extensions"))) {
      Map<String, Object> ext = asMap(extObj);
      if (VENDOR.equals(str(get(ext, "vendor_name")))) {
        // A null or empty-string `data` is treated as an empty object rather than a parse error.
        String data = str(get(ext, "data"));
        if (data == null || data.isEmpty()) {
          data = "{}";
        }
        Map<String, Object> parsed;
        try {
          parsed = asMap(MAPPER.readValue(data, Object.class));
        } catch (Exception e) {
          throw new ConversionException(
              "DATABRICKS custom_extensions data is not valid JSON: " + e.getMessage(), e);
        }
        parsed.remove("_v");
        return parsed;
      }
    }
    return new LinkedHashMap<>();
  }

  static List<Object> foreignVendorExtensions(Map<String, Object> obj) {
    List<Object> out = new ArrayList<>();
    for (Object extObj : asList(get(obj, "custom_extensions"))) {
      if (!VENDOR.equals(str(get(asMap(extObj), "vendor_name")))) {
        out.add(extObj);
      }
    }
    return out;
  }

  /** Attach a DATABRICKS custom_extensions entry holding `data`; no-op when empty. */
  @SuppressWarnings("unchecked")
  static void writeStash(Map<String, Object> obj, Map<String, Object> data) {
    if (data.isEmpty()) {
      return;
    }
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("_v", STASH_VERSION);
    payload.putAll(data);
    String blob;
    try {
      blob = JSON_WRITER.writeValueAsString(payload);
    } catch (Exception e) {
      throw new ConversionException("failed to serialize stash: " + e.getMessage(), e);
    }
    // Jackson emits unicode escapes with uppercase hex; the stash format uses lowercase. Lowercase
    // just the 4 hex digits of each real escape, preserving any escaped-backslash run in front of
    // it (see UNICODE_ESCAPE_RE).
    blob = UNICODE_ESCAPE_RE.matcher(blob)
        .replaceAll(m -> m.group(1) + "\\\\u" + m.group(2).toLowerCase(Locale.ROOT));
    List<Object> exts = (List<Object>) obj.computeIfAbsent("custom_extensions", k -> new ArrayList<>());
    for (Object extObj : exts) {
      Map<String, Object> ext = asMap(extObj);
      if (VENDOR.equals(str(get(ext, "vendor_name")))) {
        ext.put("data", blob);
        return;
      }
    }
    Map<String, Object> ext = new LinkedHashMap<>();
    ext.put("vendor_name", VENDOR);
    ext.put("data", blob);
    exts.add(ext);
  }

  /** Last dotted part of a table reference: `samples.tpch.lineitem` -> `lineitem`.
   * Trim the whole reference, take the final dotted
   * segment, then strip any surrounding backticks (so `cat.sch.`t`` -> `t`). */
  static String lastIdentifier(Object source) {
    if (source == null) {
      return null;
    }
    String s = source.toString().trim();
    int dot = s.lastIndexOf('.');
    String last = dot >= 0 ? s.substring(dot + 1) : s;
    int start = 0;
    int end = last.length();
    while (start < end && last.charAt(start) == '`') {
      start++;
    }
    while (end > start && last.charAt(end - 1) == '`') {
      end--;
    }
    return last.substring(start, end);
  }

  /** Parse YAML text into a plain value (YAML 1.2 booleans, matching the converter). */
  static Object parseYaml(String s) {
    try {
      return loadYaml(s);
    } catch (Exception e) {
      throw new ConversionException("failed to parse YAML: " + e.getMessage(), e);
    }
  }

  /** Serialize a value to YAML using the converter's write mapper (for tests without their
   * own jackson-yaml import). */
  static String dumpYaml(Object obj) {
    try {
      return MAPPER.writeValueAsString(obj);
    } catch (Exception e) {
      throw new ConversionException("failed to serialize YAML: " + e.getMessage(), e);
    }
  }
}

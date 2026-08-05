# SimPlan Variables (HOCON) vs Jinja2 Templates: Comprehensive Comparison

**Author:** Technical Documentation  
**Date:** 2026-04-08  
**Audience:** Data Engineers building pipeline configurations in SimPlan

---

## Executive Summary

**SimPlan** uses **HOCON (Human-Optimized Config Object Notation)** for configuration with qualified parameters and JavaScript expression evaluation capabilities. **Jinja2** is a full-featured templating engine designed for dynamic content generation. This document compares both systems in the context of data pipeline configuration.

**Key Takeaway:** HOCON is a configuration system with substitution capabilities, while Jinja2 is a templating engine with programming features. SimPlan bridges the gap using qualified parameters, JavaScript expressions, and operator-based logic.

---

## 1. Feature Comparison Table

| Feature | SimPlan Variables (HOCON) | Jinja2 Templates | SimPlan Workaround |
|---------|---------------------------|------------------|-------------------|
| **Variable Substitution** | `${variable}` | `{{ variable }}` | ✅ Native HOCON |
| **Default Values** | `${var:-default}` | `{{ var \| default('default') }}` | ✅ Native HOCON |
| **Optional Substitution** | `${?var}` (field omitted if missing) | `{% if var is defined %}` | ✅ Native HOCON |
| **Conditional Logic (if/else)** | ❌ Not in config | `{% if condition %}...{% else %}...{% endif %}` | ⚠️ Operators + JavaScript |
| **Loops/Iteration** | ❌ Not in config | `{% for item in list %}...{% endfor %}` | ⚠️ Operators only |
| **String Filters** | ❌ Limited | `{{ var \| upper \| trim }}` | ⚠️ JavaScript functions |
| **Mathematical Operations** | ❌ Not in config | `{{ (price * 1.1) \| round(2) }}` | ⚠️ JavaScript expressions |
| **String Concatenation** | ✅ `foo = ${a}"-"${b}` | `{{ a ~ "-" ~ b }}` | ✅ Native HOCON |
| **Nested Substitutions** | ✅ `${outer:-${inner:-default}}` | `{{ outer \| default(inner \| default('default')) }}` | ✅ Native HOCON |
| **Type Handling** | ✅ Strings, numbers, booleans, objects, lists | ✅ All Python types | ✅ Native HOCON |
| **Custom Functions** | ✅ Qualified params (`idpsSecret()`) | `{{ custom_function(arg) }}` | ✅ Qualified params |
| **Template Inheritance** | ❌ No | `{% extends "base.html" %}` | ⚠️ HOCON includes |
| **Macros/Reusable Blocks** | ❌ No | `{% macro card(title) %}...{% endmacro %}` | ⚠️ Config includes |
| **Comments** | `# comment` or `// comment` | `{# comment #}` | ✅ Native HOCON |
| **Environment Variables** | ✅ `${ENV_VAR}` (from system props) | `{{ env['ENV_VAR'] }}` | ✅ Native HOCON |
| **Expression Evaluation** | ⚠️ Limited (via JavaScript) | ✅ Full Python expressions | ⚠️ JavaScript engine |
| **Access Operator Responses** | ✅ `responseValue(task,key)` | ❌ N/A | ✅ Qualified params |
| **Secret Management** | ✅ `idpsSecret(path)` | ❌ External integration needed | ✅ Qualified params |
| **Schema Definition** | ✅ `schemaDDL(...)`, `schemaJson(...)` | ❌ N/A | ✅ Qualified params |
| **Configuration Merging** | ✅ Object merging with `${defaults}` | ❌ No | ✅ Native HOCON |
| **Whitespace Control** | N/A (config file) | `{%- if ... -%}` | N/A |

**Legend:**
- ✅ Fully supported natively
- ⚠️ Supported with workarounds/limitations
- ❌ Not supported

---

## 2. Use Case Differences

### When to Use SimPlan Variables (HOCON)

**Design Philosophy:** Declarative configuration with type safety and hierarchical structure.

**Best For:**
1. **Static configuration files** that define pipeline structure
2. **Environment-specific configs** (dev/qal/prd) with value overrides
3. **Type-safe configurations** parsed into Scala case classes
4. **Hierarchical settings** with inheritance and merging
5. **Secret management** integration (IDPS, config service)
6. **Operator configurations** in data pipelines

**Example Use Cases:**
```hocon
# Database connection configs varying by environment
simplan.system.config.database {
  host = ${DB_HOST:-localhost}
  port = ${DB_PORT:-5432}
  password = idpsSecret(${simplan.application.environment}/db/password)
}

# Spark configuration with defaults
spark.properties {
  "spark.executor.memory" = ${EXECUTOR_MEMORY:-4g}
  "spark.executor.cores" = ${EXECUTOR_CORES:-2}
}
```

### When to Use Jinja2 Templates

**Design Philosophy:** Dynamic content generation with programming constructs.

**Best For:**
1. **Dynamic SQL generation** based on runtime parameters
2. **Complex conditional logic** in templates
3. **Iterating over collections** to generate repeated structures
4. **String manipulation** with filters and transformations
5. **HTML/documentation generation** from data
6. **Runtime template rendering** with user input

**Example Use Cases:**
```jinja2
{# Generate partition filters dynamically #}
SELECT * FROM {{ table_name }}
WHERE 1=1
{% for partition in partitions %}
  AND partition_date = '{{ partition }}'
{% endfor %}
{% if include_deleted %}
  -- Include soft-deleted records
{% else %}
  AND deleted_at IS NULL
{% endif %}
```

---

## 3. Missing Features Analysis

### 3.1 Control Flow (if/else, for loops)

**What Jinja2 Has:**
```jinja2
{% if environment == 'prod' %}
SET spark.executor.memory = 8g;
{% elif environment == 'qal' %}
SET spark.executor.memory = 4g;
{% else %}
SET spark.executor.memory = 2g;
{% endif %}

{% for table in tables %}
CREATE TABLE {{ table.name }} ({{ table.schema }});
{% endfor %}
```

**SimPlan Doesn't Have:** Native control flow in configuration files.

**Why:** HOCON is a configuration format, not a programming language. Control flow belongs in code, not config.

---

### 3.2 Filters and Transformations

**What Jinja2 Has:**
```jinja2
{{ username | upper | trim }}
{{ price | round(2) }}
{{ date | strftime('%Y-%m-%d') }}
{{ list | join(', ') }}
```

**SimPlan Doesn't Have:** Built-in filters for string/data manipulation in config.

---

### 3.3 Template Inheritance

**What Jinja2 Has:**
```jinja2
{# base.sql #}
{% block header %}SELECT *{% endblock %}
FROM {{ table }}
{% block where_clause %}WHERE 1=1{% endblock %}

{# child.sql #}
{% extends "base.sql" %}
{% block where_clause %}
WHERE created_at > '{{ start_date }}'
{% endblock %}
```

**SimPlan Doesn't Have:** Template inheritance mechanism.

---

### 3.4 Macros and Reusable Blocks

**What Jinja2 Has:**
```jinja2
{% macro create_table(name, columns) %}
CREATE TABLE {{ name }} (
  {% for col in columns %}
  {{ col.name }} {{ col.type }}{% if not loop.last %},{% endif %}
  {% endfor %}
);
{% endmacro %}

{{ create_table('users', user_columns) }}
{{ create_table('orders', order_columns) }}
```

**SimPlan Doesn't Have:** Macro system for reusable config patterns.

---

## 4. Workarounds in SimPlan

### 4.1 Conditional Logic Using Operators

**Instead of Jinja2 if/else, use SimPlan operators:**

```hocon
# Trigger-based conditional execution
checkEnvironment {
  trigger {
    operator = SparkExpressionOperator
    config = {
      # JavaScript expression for boolean evaluation
      expression = "environment == 'prod'"
    }
  }
  action {
    operator = SqlDDLDMLOperation
    config = {
      queries = ["SET spark.executor.memory = 8g"]
    }
  }
}
```

**Or use JavaScript evaluation:**
```hocon
stateProcessing {
  action {
    operator = JsMapGroupByStateOperator
    config = {
      source = events
      groupBy = ["user_id"]
      stateRules = {
        incrementCounter = {
          condition = "event.type == 'purchase' && state.counters.total < 100"
          action = "state.counters.total += 1"
        }
      }
    }
  }
}
```


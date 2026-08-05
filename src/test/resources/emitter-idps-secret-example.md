# Using IdpsSecret with SimPlan Emitters

This document explains how to use `idpsSecret` qualified parameters in SimPlan emitter configurations using HOCON variable substitution (Option 1).

## Overview

You can securely pass secrets from IDPS to emitter configurations by:
1. Defining secrets in `simplan.system.config` using `idpsSecret()`
2. Referencing those values in emitter config using HOCON's `${...}` substitution syntax

## How It Works

### Step 1: Define Secrets in System Config

```hocon
simplan {
  system {
    config {
      idps {
        endpoint = vkm.ps.idps.a.intuit.com
        policy_id = p-xxxxx
      }
      
      kafka {
        # These idpsSecret() calls are resolved when config is loaded
        password = idpsSecret(kafka/my-password)
        saslConfig = idpsSecret(kafka/sasl-config)
        bootstrapServers = "kafka.example.com:9092"
      }
    }
  }
}
```

### Step 2: Reference Secrets in Emitter Config

```hocon
simplan {
  emitters {
    myKafkaEmitter {
      handler = "com.intuit.data.simplan.common.emitters.KafkaEmitter"
      enabled = true
      config = {
        producerConfig {
          # Use HOCON substitution to reference the resolved secrets
          "bootstrap.servers" = ${simplan.system.config.kafka.bootstrapServers}
          "sasl.jaas.config" = ${simplan.system.config.kafka.saslConfig}
          "ssl.keystore.password" = ${simplan.system.config.kafka.password}
          "key.serializer" = "org.apache.kafka.common.serialization.StringSerializer"
          "value.serializer" = "org.apache.kafka.common.serialization.StringSerializer"
        }
        topic = "my-topic"
        maxRetries = 3
        retryInterval = 1000
      }
    }
  }
}
```

## Execution Flow

1. **Config Loading**: When `SparkAppContext` is created with `IdpsSupport`
2. **IdpsSecret Resolution**: The `IdpsQualifiedStringHandler` resolves all `idpsSecret()` calls
3. **HOCON Substitution**: HOCON's `${...}` references are resolved to the actual secret values
4. **Emitter Initialization**: Emitters receive the fully-resolved configuration with actual secret values

## Requirements

Your `SparkAppContext` must mix in `IdpsSupport`:

```scala
val context = new SparkAppContext(initContext) with IdpsSupport
```

## Benefits

- ✅ **Secure**: Secrets are fetched from IDPS, not hardcoded
- ✅ **Simple**: Uses standard HOCON substitution syntax
- ✅ **Centralized**: All secrets defined in one place (`system.config`)
- ✅ **Reusable**: Same secret can be referenced by multiple emitters
- ✅ **Environment-specific**: Different configs for dev/qal/prd environments

## Example: Multiple Emitters Sharing Secrets

```hocon
simplan {
  system {
    config {
      kafka {
        password = idpsSecret(kafka/shared-password)
        bootstrapServers = "kafka.example.com:9092"
      }
    }
  }
  
  emitters {
    emitter1 {
      handler = "com.intuit.data.simplan.common.emitters.KafkaEmitter"
      enabled = true
      config = {
        producerConfig {
          "bootstrap.servers" = ${simplan.system.config.kafka.bootstrapServers}
          "sasl.jaas.config" = ${simplan.system.config.kafka.password}
        }
        topic = "topic-1"
        maxRetries = 3
        retryInterval = 1000
      }
    }
    
    emitter2 {
      handler = "com.intuit.data.simplan.common.emitters.KafkaEmitter"
      enabled = true
      config = {
        producerConfig {
          # Same secrets, different topic
          "bootstrap.servers" = ${simplan.system.config.kafka.bootstrapServers}
          "sasl.jaas.config" = ${simplan.system.config.kafka.password}
        }
        topic = "topic-2"
        maxRetries = 5
        retryInterval = 2000
      }
    }
  }
}
```

## Testing

See `IdpsSecretEmitterTest.scala` for a complete test example that demonstrates:
- Mocking IDPS for testing
- Verifying secret resolution
- Checking emitter configuration

## Alternative Approaches

If you need more control over when secrets are resolved, see:
- **Option 2**: Custom emitter with `QualifiedParam` fields
- **Option 3**: Programmatic configuration override

Refer to the main documentation for details on these alternatives.


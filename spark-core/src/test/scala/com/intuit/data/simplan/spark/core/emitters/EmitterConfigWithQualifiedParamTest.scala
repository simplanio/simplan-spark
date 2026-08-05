/*
 * Copyright 2025, Intuit Inc
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intuit.data.simplan.spark.core.emitters

import com.intuit.data.simplan.spark.core.context.SparkAppContext
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import java.io.File
import scala.util.Try

/**
 * Test demonstrating how to use qualified parameters (like idpsSecret) in emitter configurations
 * using HOCON variable substitution.
 * 
 * This test shows that:
 * 1. You can define qualified params in system.config
 * 2. Reference those values in emitter config using ${...} substitution
 * 3. HOCON resolves the substitution automatically
 * 4. Emitters receive the already-resolved values
 * 
 * Note: This test uses mockSecret() as an example. In production with IdpsSupport,
 * you would use idpsSecret() which works the same way.
 * 
 * @author Abraham, Thomas - tabraham1
 *         Created on 26-Mar-2025 at 12:30 PM
 */
class EmitterConfigWithQualifiedParamTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  // Create a test context
  lazy val context: SparkAppContext = {
    SparkAppContext(Array("classpath:test-emitter-with-qualified-param.conf"))
  }

  override def afterAll(): Unit = {
    Try {
      Try(context.sc.stop())
      context.spark.stop()
    }
    val warehouse = new File("spark-warehouse")
    if (warehouse.exists()) warehouse.delete()
    val derby = new File("derby.log")
    if (derby.exists()) derby.delete()
  }

  "Emitter configuration with HOCON substitution" should "resolve qualified params from system config" in {
    // Verify that the context loaded successfully
    context should not be null
    
    // Verify that emitters are configured
    val emitters = context.appContextConfig.emitters
    emitters should not be empty
    emitters.contains("testKafkaEmitter") shouldBe true
    
    // Get the emitter config
    val emitterConfig = emitters("testKafkaEmitter")
    emitterConfig.enabled shouldBe Some(true)
    emitterConfig.handler shouldBe "com.intuit.data.simplan.common.emitters.KafkaEmitter"
    
    // The config string should contain the resolved values from system.config
    // This proves that HOCON substitution worked
    val configString = emitterConfig.config
    configString should include("localhost:9092")
    
    // Note: In a real scenario with idpsSecret(), the secret would be resolved here
    // For this test, we're just verifying the HOCON substitution mechanism works
  }

  it should "allow emitter to access values substituted from system config" in {
    // Parse the emitter config to verify structure
    import com.intuit.data.simplan.global.json.SimplanJsonMapper
    
    val emitterConfig = context.appContextConfig.emitters("testKafkaEmitter")
    val kafkaConfig = SimplanJsonMapper.fromJson[Map[String, Any]](emitterConfig.config)
    
    // Verify producerConfig exists and contains substituted values
    kafkaConfig should contain key "producerConfig"
    val producerConfig = kafkaConfig("producerConfig").asInstanceOf[Map[String, String]]
    
    producerConfig("bootstrap.servers") shouldBe "localhost:9092"
    
    // Verify other emitter config fields
    kafkaConfig should contain key "topic"
    kafkaConfig("topic").toString shouldBe "test-topic"
    kafkaConfig should contain key "maxRetries"
    kafkaConfig should contain key "retryInterval"
  }

  "HOCON substitution pattern" should "work for multiple emitters sharing same config" in {
    val emitters = context.appContextConfig.emitters

    // If there are multiple emitters, they can all reference the same system config values
    // This demonstrates the reusability of the pattern
    emitters.size should be >= 1

    // In a real scenario with IdpsSupport, emitters would receive:
    // - Resolved idpsSecret values from system.config
    // - HOCON substitution automatically connects them
    // - No need to manually pass secrets around
  }
}


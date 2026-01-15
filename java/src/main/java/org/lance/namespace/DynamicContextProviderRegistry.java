/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.namespace;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Utility for creating DynamicContextProvider instances from configuration properties.
 *
 * <p>This class loads provider classes dynamically using the full class path specified in the
 * {@code dynamic_context_provider.impl} property.
 *
 * <p>Provider classes must implement {@link DynamicContextProvider} and have a constructor that
 * accepts {@code Map<String, String>} for configuration properties.
 *
 * <p>Example usage:
 *
 * <pre>
 * // Define a provider class
 * public class MyAuthProvider implements DynamicContextProvider {
 *   private final String token;
 *
 *   public MyAuthProvider(Map&lt;String, String&gt; properties) {
 *     this.token = properties.get("token");
 *   }
 *
 *   &#64;Override
 *   public Map&lt;String, String&gt; provideContext(String operation, String objectId) {
 *     Map&lt;String, String&gt; context = new HashMap&lt;&gt;();
 *     context.put("headers.Authorization", "Bearer " + token);
 *     return context;
 *   }
 * }
 *
 * // Use via namespace properties with full class path
 * Map&lt;String, String&gt; properties = new HashMap&lt;&gt;();
 * properties.put("root", "/path/to/data");
 * properties.put("dynamic_context_provider.impl", "com.example.MyAuthProvider");
 * properties.put("dynamic_context_provider.token", "secret-token");
 *
 * DirectoryNamespace namespace = new DirectoryNamespace();
 * namespace.initialize(properties, allocator);
 * </pre>
 */
public final class DynamicContextProviderRegistry {

  private static final String PROVIDER_PREFIX = "dynamic_context_provider.";
  private static final String IMPL_KEY = "dynamic_context_provider.impl";

  private DynamicContextProviderRegistry() {}

  /**
   * Create a context provider from properties if configured.
   *
   * <p>Looks for {@code dynamic_context_provider.impl} in the properties. If found, loads the class
   * using {@code Class.forName()} and instantiates it with the extracted provider properties.
   *
   * <p>The provider class must:
   *
   * <ul>
   *   <li>Implement {@link DynamicContextProvider}
   *   <li>Have a public constructor that accepts {@code Map<String, String>}
   * </ul>
   *
   * @param properties The full configuration properties.
   * @return An Optional containing the created provider, or empty if no provider is configured.
   * @throws IllegalArgumentException if {@code dynamic_context_provider.impl} is set but the class
   *     cannot be loaded or instantiated.
   */
  public static Optional<DynamicContextProvider> createFromProperties(
      Map<String, String> properties) {
    String className = properties.get(IMPL_KEY);
    if (className == null || className.isEmpty()) {
      return Optional.empty();
    }

    // Extract provider-specific properties (strip prefix, exclude impl key)
    Map<String, String> providerProps = new HashMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(PROVIDER_PREFIX) && !key.equals(IMPL_KEY)) {
        String propName = key.substring(PROVIDER_PREFIX.length());
        providerProps.put(propName, entry.getValue());
      }
    }

    try {
      Class<?> providerClass = Class.forName(className);
      if (!DynamicContextProvider.class.isAssignableFrom(providerClass)) {
        throw new IllegalArgumentException(
            String.format(
                "Class '%s' does not implement DynamicContextProvider interface", className));
      }

      @SuppressWarnings("unchecked")
      Class<? extends DynamicContextProvider> typedClass =
          (Class<? extends DynamicContextProvider>) providerClass;

      Constructor<? extends DynamicContextProvider> constructor =
          typedClass.getConstructor(Map.class);
      return Optional.of(constructor.newInstance(providerProps));

    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          String.format("Failed to load context provider class '%s': %s", className, e), e);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Context provider class '%s' must have a public constructor "
                  + "that accepts Map<String, String>",
              className),
          e);
    } catch (ReflectiveOperationException e) {
      throw new IllegalArgumentException(
          String.format("Failed to instantiate context provider '%s': %s", className, e), e);
    }
  }

  /**
   * Filter out dynamic_context_provider.* properties from the map.
   *
   * <p>These properties are handled at the Java level and should not be passed to the native layer.
   *
   * @param properties The full properties map.
   * @return A new map with dynamic_context_provider.* keys removed.
   */
  public static Map<String, String> filterProviderProperties(Map<String, String> properties) {
    Map<String, String> filtered = new HashMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (!entry.getKey().startsWith(PROVIDER_PREFIX)) {
        filtered.put(entry.getKey(), entry.getValue());
      }
    }
    return filtered;
  }
}

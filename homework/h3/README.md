# ECE4721 homework 3

## Ex.1

- The comparison graph is as following

![graph](img/comparison.jpg)

- As we can see in the graph, the embedded mapreduce attains faster speed when dealing with larger data.

## Ex.2

1; Explain the three ways or API styles into which Avro can be used in MapReduce, and when to apply each of them.

- **Specific API**: This involves generating Java classes from an Avro schema (`.avsc` file) using the `avro-tools` compiler.
  - **How it works:**
    - You write an Avro schema file
    - Use `avro-tools` to generate a Java class (POJO-like) that corresponds to the schema.
    - In MapReduce, you use the generated class (e.g., `User.class`) as the key or value.
  - **When to use:**
    - Schema is fixed and known ahead of time
    - You want **type safety** and **better performance**
    - You are working in a **compiled, strongly typed** Java environment
  - **Pros:**
    - Fastest runtime (compiled class, no reflection)
    - Type-safe, IDE-friendly
    - Easy to work with in Java
  - **Cons:**
    - Need to regenerate code whenever the schema changes
- **Generic API**: This uses `GenericRecord` to work with schemas **without code generation**.
  - **How it works:**
    - Schema is still required, but not compiled to a Java class.
    - You parse the schema at runtime and use `GenericRecord` objects.
  - **When to use:**
    - Schema is **known**, but you don't want to generate Java code
    - You need **flexibility** (e.g., handling many schemas dynamically)
    - Useful in **data pipelines** where schemas evolve or vary
  - **Pros:**
    - No code generation
    - Good for dynamic schema use cases
  - **Cons:**
    - Less type-safe
    - Slightly slower than specific
- **Reflect API**: This uses **Java reflection** to serialize/deserialize POJOs with Avro.
  - **How it works:**
    - You write a regular Java class (POJO) with fields and getters/setters.
    - Avro uses reflection to infer the schema and read/write data.
  - **When to use:**
    - You already have **existing Java classes** and don't want to write Avro schemas
    - Quick prototyping or **one-off jobs**
    - Classes are simple and follow JavaBean conventions
    - Then Avro infers schema from this class automatically.
  - **Pros:**
    - No need to define schemas or generate code
    - Convenient for quick or small-scale use
  - **Cons:**
    - Slowest due to reflection
    - Harder to evolve schemas cleanly
    - Not as portable across languages

## Ex.3

1; Describe what a Bloom filter is and how it works.

A **Bloom filter** is a **space-efficient probabilistic data structure** used to test whether an element is **a member of a set**. It can say with certainty if an element **is not** in the set, but if it says an element **might be** in the set, there's a chance of a **false positive**.

- How It Works:

1. **Initialization**:

   - A Bloom filter uses a **bit array** of size `m`, initially set to all 0s.
   - It uses **`k` independent hash functions**, each of which maps an input to one of the `m` positions in the bit array.

2. **Adding Elements**:

   - To add an element, pass it through the `k` hash functions to get `k` indices.
   - Set the bits at all these `k` indices in the array to 1.

3. **Checking Membership**:

   - To check whether an element is in the set, hash it with the same `k` functions.
   - If **all `k` bits** at the corresponding indices are **1**, the element **might be in the set**.
   - If **any of the `k` bits** is **0**, the element is **definitely not in the set**.

- Key Properties:
  - **False positives are possible**: The filter may indicate that an element is in the set when it is not.
  - **False negatives are impossible**: If the filter says an element is not in the set, it truly isn't.
  - **No deletions**: Standard Bloom filters do not support removal of elements (unless using a variant like the **Counting Bloom filter**).
- Use Cases:
  - Caches (e.g., to avoid unnecessary disk lookups)
  - Databases (to filter out non-existent rows before a query)
  - Networking (packet inspection, routing)
  - Distributed systems (checking element existence without full data transfer)
- Trade-offs:
  - The probability of false positives increases as more elements are added.
  - A balance must be struck between the size of the bit array `m`, the number of hash functions `k`, and the number of inserted items `n` to control the false positive rate.

# ViewForge

ViewForge is a domain-specific language (DSL) designed for defining and managing **computational pipelines** over various data sources. It offers a structured and declarative approach that abstracts away the complexities of workflow construction and management.

## 💬 About the DSL

The ViewForge DSL enables users to:

- Define data sources
- Apply transformation steps
- Select appropriate execution engines
- Specify computation mode (real-time or batch)

Its design promotes simplicity and expressiveness, making it suitable for both exploratory data flows and production pipelines.

## 🛠 Build & Usage

### Compile the Code

To compile the ViewForge project:

```bash
sbt compile
```

### Generate Parser Files

To generate the parser files:

```bash
sbt generate-parser
```

## 📦 Requirements

- [SBT](https://www.scala-sbt.org/) 1.5+
- JDK 8 or later
- Scala 2.13

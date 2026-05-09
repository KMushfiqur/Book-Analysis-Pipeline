# Book Analysis — Parallel Data Processing (Final Project)

**Overview**
- Java + Apache Spark project that performs dataset filtering, PageRank on a book-user graph, and KNN-based book recommendations.
- The orchestrator is `BookAnalysis` which runs filtering, PageRank, and KNN recommendation steps.

**Important source files**
- `java/BookAnalysis.java` — main entry point and orchestration ([java/BookAnalysis.java](java/BookAnalysis.java#L1-L400)).
- `java/PageRankAnalysis.java` — builds graph and runs PageRank ([java/PageRankAnalysis.java](java/PageRankAnalysis.java#L1-L400)).
- `java/KNNBookRecommendation.java` — KNN recommender helpers ([java/KNNBookRecommendation.java](java/KNNBookRecommendation.java#L1-L400)).
- `java/DataFilter.java`, `java/Timer.java` — helpers used by the pipeline.

**Data files**
- Example CSVs: `books.csv`, `ratings.csv`, `users.csv` in the project root. Some code uses S3 paths by default.

**Prerequisites**
- Java 8 (or compatible JDK matching project settings)
- Maven (to build the shaded/fat JAR)
- Apache Spark 3.x installed (or use the spark-submit binary bundled with your Spark distribution)
- GraphFrames package compatible with your Spark/Scala version
- AWS credentials set in the environment if you plan to read/write the S3 paths used by the code

**Build**
1. From the project root run:

```
mvn clean package
```

This produces a shaded JAR in `target/` (the POM config adds `BookAnalysis` as the manifest main class).

**Run (recommended: spark-submit)**
- Local quick run (uses local Spark master):

```
spark-submit \
  --class BookAnalysis \
  --master local[4] \
  --packages graphframes:graphframes:0.8.2-spark3.2-s_2.12 \
  --conf spark.driver.memory=4g \
  target/book-analysis-1.0.jar
```

- Notes:
  - The code currently reads/writes to S3 paths like `s3://khan-final-project/...`. To run locally, either configure AWS credentials or edit `java/BookAnalysis.java` to point to local CSVs (`books.csv`, `ratings.csv`). See the file at [java/BookAnalysis.java](java/BookAnalysis.java#L1-L400).
  - If GraphFrames is not available via `--packages`, add the GraphFrames JAR to Spark's classpath or use the correct `--packages` coordinate for your Spark/Scala version.

**Running locally without S3**
1. Edit `java/BookAnalysis.java` and replace the S3 paths with local paths, for example:

```
    .csv("books.csv")
    .csv("ratings.csv")
```

2. Build and run with the `spark-submit` example above. You can also uncomment the `.master("local[4]")` configuration in `BookAnalysis` for quick debugging.

**Outputs**
- PageRank results and recommendations are written to the S3 paths configured in the source code (or local paths if you changed them). The code also writes timing info to `output/timing.txt` (see the `Timer` usage).
- The repository contains an `output files/` folder with example outputs produced previously.

**Troubleshooting / Tips**
- If you see classpath or GraphFrames errors, confirm the GraphFrames package coordinate matches your Spark and Scala versions.
- Increase `spark.driver.memory` and executor memory for large datasets.
- To inspect results locally, write outputs to a local directory instead of S3 and open the CSVs with your preferred tool.

**Next steps / Improvements**
- Add a small wrapper script to switch between S3 and local modes via environment variables.
- Parameterize file paths, thresholds, and Spark configs via CLI args or a properties file.

**License / Contact**
- (Add license or contact info here as needed.)

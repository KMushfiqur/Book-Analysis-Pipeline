import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.graphframes.GraphFrame;
import static org.apache.spark.sql.functions.*;

public class PageRankAnalysis {

    private SparkSession spark;

    public PageRankAnalysis(SparkSession spark) {
        this.spark = spark;
    }

    public void runPageRank(Dataset<Row> booksDF, Dataset<Row> ratingsDF) {

        // Rename columns to remove quotes/dashes
        Dataset<Row> ratingsClean = ratingsDF
            .withColumnRenamed("User-ID", "UserID")
            .withColumnRenamed("ISBN", "ISBN")
            .withColumnRenamed("Book-Rating", "BookRating");

        Dataset<Row> booksClean = booksDF
            .withColumnRenamed("ISBN", "ISBN")
            .withColumnRenamed("Book-Title", "BookTitle")
            .withColumnRenamed("Book-Author", "BookAuthor");

        // Filter books with at least 5 ratings
        Dataset<Row> bookRatingCounts = ratingsClean.groupBy("ISBN")
            .agg(count("BookRating").alias("rating_count"));

        Dataset<Row> popularBooks = bookRatingCounts.filter(col("rating_count").geq(5));
        Dataset<Row> filteredRatings = ratingsClean.join(popularBooks, "ISBN");

        // Create vertices
        Dataset<Row> userVertices = filteredRatings.select(col("UserID").alias("id"))
            .distinct()
            .withColumn("type", lit("user"));

        Dataset<Row> bookVertices = filteredRatings.select(col("ISBN").alias("id"))
            .distinct()
            .withColumn("type", lit("book"));

        Dataset<Row> vertices = userVertices.union(bookVertices);

        // Create edges
        Dataset<Row> edges = filteredRatings.select(
            col("UserID").alias("src"),
            col("ISBN").alias("dst"),
            col("BookRating").alias("weight")
        );

        // Build graph and run PageRank
        GraphFrame graph = GraphFrame.apply(vertices, edges);
        GraphFrame results = graph.pageRank()
            .resetProbability(0.15)
            .maxIter(5)
            .run();

        // Top 100 books
        Dataset<Row> bookRanks = results.vertices()
            .filter(col("type").equalTo("book"))
            .join(booksClean, col("id").equalTo(col("ISBN"))) // <-- use cleaned books
            .select(col("ISBN"), col("BookTitle"), col("BookAuthor"), col("pagerank"))
            .orderBy(desc("pagerank"))
            .limit(100);

        // Top 100 users
        Dataset<Row> userRanks = results.vertices()
            .filter(col("type").equalTo("user"))
            .select(col("id").alias("UserID"), col("pagerank"))
            .orderBy(desc("pagerank"))
            .limit(100);

        // Save results
        bookRanks.coalesce(1).write().mode("overwrite").option("header", "true")
            .csv("s3://khan-final-project/output/top_books_pagerank");

        userRanks.coalesce(1).write().mode("overwrite").option("header", "true")
            .csv("s3://khan-final-project/output/top_users_pagerank");

        System.out.println("PageRank analysis completed successfully!");
    }
    
}

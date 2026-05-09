import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public class DataFilter {

    // New method without sampling
    public Dataset<Row> filterRatings(Dataset<Row> ratingsDF, int minRatings) {
        // Rename columns to remove quotes and dashes
        Dataset<Row> ratingsClean = ratingsDF
            .withColumnRenamed("User-ID", "UserID")
            .withColumnRenamed("ISBN", "ISBN")
            .withColumnRenamed("Book-Rating", "BookRating");

        // Count ratings per book using cleaned column names
        Dataset<Row> bookRatingCounts = ratingsClean.groupBy("ISBN")
                .agg(count("BookRating").alias("rating_count"));

        Dataset<Row> popularBooks = bookRatingCounts.filter(col("rating_count").geq(minRatings));

        Dataset<Row> filteredRatings = ratingsClean.join(popularBooks, "ISBN");

        long filteredCount = filteredRatings.count();
        long uniqueBooks = popularBooks.count();
        
        System.out.println("Filtered ratings count: " + filteredCount);
        System.out.println("Number of books with at least " + minRatings + " ratings: " + uniqueBooks);
        
        return filteredRatings;
    }
    
    public Dataset<Row> filterAndSample(Dataset<Row> ratingsDF, int minRatings, double sampleFraction) {
        Dataset<Row> filtered = filterRatings(ratingsDF, minRatings);
        return filtered.sample(sampleFraction);
    }
}
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import static org.apache.spark.sql.functions.*;
import java.util.List;
import java.util.ArrayList;

public class BookAnalysis {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("BookAnalysis")
            // .master("local[4]")
            // .config("spark.driver.memory", "8g")
            // .config("spark.sql.shuffle.partitions", "200")
            .getOrCreate();

        // Load datasets
        Dataset<Row> booksDF = spark.read()
            .option("header", "true")
            .option("inferSchema", "true")
            .option("sep", ";")
            .csv("s3://khan-final-project/data/books.csv");

        Dataset<Row> ratingsDF = spark.read()
            .option("header", "true")
            .option("inferSchema", "true")
            .option("sep", ";")
            .csv("s3://khan-final-project/data/ratings.csv");

        // Print counts
        long bookCount = booksDF.count();
        long ratingCount = ratingsDF.count();
        System.out.println("=== Dataset Statistics ===");
        System.out.println("Total Books: " + bookCount);
        System.out.println("Total Ratings: " + ratingCount);

        // Step 1: Filter ratings
        Timer filterTimer = new Timer();
        filterTimer.start();
        
        DataFilter dataFilter = new DataFilter();
        Dataset<Row> filteredRatings = dataFilter.filterRatings(ratingsDF, 5);
        
        filterTimer.stop();
        filterTimer.writeElapsedTime("Data Filtering", "output/timing.txt");

        // Step 2: Run PageRank
        Timer pageRankTimer = new Timer();
        pageRankTimer.start();
        
        PageRankAnalysis pr = new PageRankAnalysis(spark);
        pr.runPageRank(booksDF, filteredRatings);
        
        pageRankTimer.stop();
        pageRankTimer.writeElapsedTime("PageRank Analysis", "s3://khan-final-project/output/timing.txt");

        // Step 3: Run KNN Recommendations
        Timer knnTimer = new Timer();
        knnTimer.start();
        
        System.out.println("\n=== KNN Recommendation System ===");
        
        KNNBookRecommendation knn = new KNNBookRecommendation(spark);
        
        // Use the batch method from KNNBookRecommendation
        Dataset<Row> combinedRecommendations = knn.generateBatchRecommendations(
            filteredRatings, 
            booksDF, 
            KNNBookRecommendation.TOP_INFLUENTIAL_USERS,
            20,  // kNeighbors
            15   // topNPerUser
        );
        
        long totalRecs = combinedRecommendations.count();
        
        if (totalRecs > 0) {
            System.out.println("\n=== Writing ALL recommendations to single file ===");
            
            // Save all at once using the new method
            knn.saveCombinedRecommendations(combinedRecommendations, "s3://khan-final-project/output/all_knn_recommendations");
            
            System.out.println("Total recommendations collected: " + totalRecs);
            System.out.println("✓ Saved ALL recommendations to 'output/all_knn_recommendations/'");
            
            // Show final combined result
            System.out.println("\n=== Combined Recommendations Sample ===");
            combinedRecommendations
                .orderBy(col("target_user_id"), desc("avg_rating"))
                .show(20, false);
        } else {
            System.out.println("\n✗ No recommendations generated for any user");
        }

        System.out.println("\n=== KNN Analysis Complete ===");
        System.out.println("Processed " + KNNBookRecommendation.TOP_INFLUENTIAL_USERS.length + " influential users");
        
        knnTimer.stop();
        knnTimer.writeElapsedTime("KNN Recommendations", "s3://khan-final-project/output/timing.txt");
        
        // Print summary timing
        System.out.println("\n=== Performance Summary ===");
        System.out.println("Data Filtering: " + filterTimer.getElapsedTimeSeconds() + " seconds");
        System.out.println("PageRank: " + pageRankTimer.getElapsedTimeSeconds() + " seconds");
        System.out.println("KNN Recommendations: " + knnTimer.getElapsedTimeSeconds() + " seconds");
        System.out.println("Total time: " + (filterTimer.getElapsedTimeSeconds() + 
                                            pageRankTimer.getElapsedTimeSeconds() + 
                                            knnTimer.getElapsedTimeSeconds()) + " seconds");

        spark.stop();
        System.out.println("\n=== Analysis Complete ===");
    }
}
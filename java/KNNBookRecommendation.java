import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;
import java.util.List;
import java.util.ArrayList;

public class KNNBookRecommendation {
    
    private SparkSession spark;
    
    // Top influential users constant
    public static final String[] TOP_INFLUENTIAL_USERS = {
        "15957", "268622", "213312", "186039", "180981",
        "132171", "271000", "234983", "23571", "178199"
    };
    
    public KNNBookRecommendation(SparkSession spark) {
        this.spark = spark;
    }
        
    /**
     * Generates recommendations for multiple users and combines into single dataset
     */
    public Dataset<Row> generateBatchRecommendations(
            Dataset<Row> filteredRatings,
            Dataset<Row> booksDF,
            String[] userIDs,
            int kNeighbors,
            int topNPerUser) {
        
        System.out.println("\n=== Batch KNN Recommendations ===");
        System.out.println("Processing " + userIDs.length + " users");
        
        List<Dataset<Row>> allRecommendations = new ArrayList<>();
        int usersWithRecs = 0;
        
        for (String userID : userIDs) {
            System.out.println("Processing User: " + userID);
            
            Dataset<Row> userRecs = generateSingleUserRecommendations(
                filteredRatings, booksDF, userID, kNeighbors, topNPerUser);
            
            if (userRecs != null && userRecs.count() > 0) {
                allRecommendations.add(userRecs);
                usersWithRecs++;
                System.out.println("✓ Found " + userRecs.count() + " recommendations");
            } else {
                System.out.println("✗ No recommendations found");
            }
        }
        
        System.out.println("\nBatch complete: " + usersWithRecs + "/" + 
                          userIDs.length + " users got recommendations");
        
        return combineRecommendations(allRecommendations);
    }
    
    /**
     * wrapper for generateRecommendation 
     */
    private Dataset<Row> generateSingleUserRecommendations(
            Dataset<Row> filteredRatings,
            Dataset<Row> booksDF,
            String userID,
            int kNeighbors,
            int topNPerUser) {
        
        Dataset<Row> recommendations = generateRecommendations(
            filteredRatings, booksDF, userID, kNeighbors, topNPerUser);
        
        long recCount = recommendations.count();
        if (recCount > 0) {
            // Add user_id column to identify which user this recommendation is for
            return recommendations
                .withColumn("target_user_id", lit(userID))
                .select("target_user_id", "isbn", "title", "author", "avg_rating", "rating_count");
        }
        return null;
    }
    
    /**
     * Combines all user recommendations into one dataset
     */
    private Dataset<Row> combineRecommendations(List<Dataset<Row>> allRecommendations) {
        if (allRecommendations.isEmpty()) {
            return spark.emptyDataFrame();
        }
        
        Dataset<Row> combined = allRecommendations.get(0);
        
        for (int i = 1; i < allRecommendations.size(); i++) {
            combined = combined.union(allRecommendations.get(i));
        }
        
        return combined;
    }
    

    public void saveCombinedRecommendations(Dataset<Row> combinedRecs, String outputPath) {
        combinedRecs.coalesce(1)
            .write()
            .mode("overwrite")
            .option("header", "true")
            .csv(outputPath);
    }
    

    public Dataset<Row> generateRecommendations(
            Dataset<Row> ratingsDF, 
            Dataset<Row> booksDF,
            String targetUserID,
            int kNeighbors,
            int topNRecommendations) {
        
        // Clean data
        Dataset<Row> ratingsClean = ratingsDF
            .withColumnRenamed("UserID", "user_id")
            .withColumnRenamed("ISBN", "isbn")
            .withColumnRenamed("BookRating", "rating");
        
        Dataset<Row> booksClean = booksDF
            .withColumnRenamed("ISBN", "isbn")
            .withColumnRenamed("Book-Title", "title")
            .withColumnRenamed("Book-Author", "author");
        
        // Get target user's books
        Dataset<Row> targetUserBooks = ratingsClean
            .filter(col("user_id").equalTo(targetUserID))
            .select("isbn");
        
        //  Find similar users
        Dataset<Row> similarUsers = findSimilarUsers(ratingsClean, targetUserID, kNeighbors);
        
        // Get recommendations from similar users
        List<Row> similarUserList = similarUsers.collectAsList();
        List<String> similarUserIDs = new ArrayList<>();
        
        for (Row row : similarUserList) {
            similarUserIDs.add(String.valueOf(row.getInt(0)));
        }
        
        if (similarUserIDs.isEmpty()) {
            return spark.emptyDataFrame();
        }
        
        Dataset<Row> similarUserRatings = ratingsClean
            .filter(col("user_id").isin(similarUserIDs.toArray()));
        
        Dataset<Row> recommendations = similarUserRatings
            .join(targetUserBooks, 
                similarUserRatings.col("isbn").equalTo(targetUserBooks.col("isbn")), 
                "left_anti")
            .groupBy("isbn")
            .agg(
                avg("rating").alias("avg_rating"),
                count("rating").alias("rating_count")
            )
            .filter(col("rating_count").geq(2))
            .join(booksClean, "isbn")
            .select("isbn", "title", "author", "avg_rating", "rating_count")
            .orderBy(desc("avg_rating"))
            .limit(topNRecommendations);
        
        return recommendations;
    }
    
    /**
     * Simplified similarity: Users who rated the same books
     */
    private Dataset<Row> findSimilarUsers(Dataset<Row> ratings, String targetUserID, int k) {
        Dataset<Row> targetBooks = ratings
            .filter(col("user_id").equalTo(targetUserID))
            .select("isbn");
        
        Dataset<Row> usersWithCommonBooks = ratings
            .alias("r1")
            .join(targetBooks.alias("tb"), 
                ratings.col("isbn").equalTo(targetBooks.col("isbn")))
            .filter(col("r1.user_id").notEqual(targetUserID))
            .select(col("r1.user_id").alias("similar_user"));
        
        Dataset<Row> similarUsers = usersWithCommonBooks
            .groupBy("similar_user")
            .agg(count("*").alias("common_books"))
            .orderBy(desc("common_books"))
            .limit(k);
        
        return similarUsers.select("similar_user");
    }
}
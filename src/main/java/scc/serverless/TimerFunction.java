package scc.serverless;

import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.TimerTrigger;
import scc.cache.RedisCache;
import scc.AuctionDAO;
import scc.BidDAO;
import scc.QuestionDAO;
import scc.CosmosDBLayer;
import redis.clients.jedis.Jedis;

import java.time.Instant;
import java.util.Date;
import java.util.List;

/**
 * Azure Functions with Timer Trigger.
 */
public class TimerFunction {

    private CosmosDBLayer getInstance(String container) {
        CosmosDBLayer db = CosmosDBLayer.getInstance();
        db.init(container);
        return db;
    }
	private CosmosDBLayer getC1() { return getInstance(CosmosDBLayer.AUCTION_CONTAINER); }
	private CosmosDBLayer getC2() { return getInstance(CosmosDBLayer.BID_CONTAINER); }
	private CosmosDBLayer getC3() { return getInstance(CosmosDBLayer.QUESTION_CONTAINER); }

    @FunctionName("periodic-compute")
    public void cosmosFunction(@TimerTrigger(name = "periodicSetTime", schedule = "30 */1 * * * *") String timerInfo, ExecutionContext context) {
        String open = "OPEN";
        List<AuctionDAO> res = getC1().getContainer().queryItems("SELECT * FROM auctions WHERE auctions.auctions=\"" + open + "\"", new CosmosQueryRequestOptions(), AuctionDAO.class).stream().toList();
        for (AuctionDAO a : res) {
            if (a.getEndTime().after(Date.from(Instant.now()))) {
                a.setStatus("CLOSED");
                PartitionKey key = new PartitionKey(a.getId());
                CosmosItemResponse<AuctionDAO> res1 = getC1().getContainer().replaceItem(a, a.getId(), key, new CosmosItemRequestOptions());
            }
        }
    }
	
	@FunctionName("garbage-collect")
    public void cosmosFunction2(@TimerTrigger(name = "cacheGarbageWipe", schedule = "30 */1 * * * *") String timerInfo, ExecutionContext context) {
        try (Jedis jedis = RedisCache.getCachePool().getResource()) {
			List<String> users = jedis.lrange("deleted", 0, -1);
			for(String userID : users) {
				List<BidDAO> bids = getC2().getContainer().queryItems("SELECT * FROM bids WHERE bids.user=\"" + userID + "\"", new CosmosQueryRequestOptions(), BidDAO.class).stream().toList();
				for(BidDAO b : bids)
					jedis.del("bid:" + b.getId());
				List<QuestionDAO> que = getC3().getContainer().queryItems("SELECT * FROM questions WHERE questions.user=\"" + userID + "\"", new CosmosQueryRequestOptions(), QuestionDAO.class).stream().toList();
				for(QuestionDAO q : que)
					jedis.del("question:" + q.getId());
				jedis.lpop("deleted");
			}
		}
    }
}

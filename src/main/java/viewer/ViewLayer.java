package viewer;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.Set;

public class ViewLayer {
    private final Jedis j;

    public ViewLayer() {
        //TODO: change to pool
        j = new Jedis("127.0.0.1", 6379);
    }

    public static void main(String args[]) throws InterruptedException {
        ViewLayer v = new ViewLayer();
        while (true) {
            Set<Tuple> tokens = v.j.zrevrangeByScoreWithScores("global-token-count", "+inf", "-inf", 10, 10);
            for (Tuple token : tokens) {
                System.out.println(token.getElement() + "," + token.getScore());
            }
            Thread.sleep(1000*60);
            //TODO: repeat the same for each movie
//            for (String s : j.smembers("movieslist")) {
//
//            }
        }
    }
}

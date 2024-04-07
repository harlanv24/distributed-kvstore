package dslabs.shardmaster;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

@ToString
@EqualsAndHashCode
public final class ShardMaster implements Application {
    public static final int INITIAL_CONFIG_NUM = 0;

    private final int numShards;

    // Your code here...
    private int configNum;
    private Map<Integer, ShardConfig> configHistory;
    //private PriorityQueue<Pair<Integer, Set<Integer>>> pq;
    //private PriorityQueue<Pair<Integer, Set<Integer>>> pq_inv;


    public ShardMaster(int numShards) {
        this.numShards = numShards;
        configHistory = new HashMap<>();
        //pq = new PriorityQueue<>(new GroupingComparatorInc());
        //pq_inv = new PriorityQueue<>(new GroupingComparatorDec());
        configNum = INITIAL_CONFIG_NUM - 1;
    }

    public interface ShardMasterCommand extends Command {
    }

    @Data
    public static final class Join implements ShardMasterCommand {
        private final int groupId;
        private final Set<Address> servers;
    }

    @Data
    public static final class Leave implements ShardMasterCommand {
        private final int groupId;
    }

    @Data
    public static final class Move implements ShardMasterCommand {
        private final int groupId;
        private final int shardNum;
    }

    @Data
    public static final class Query implements ShardMasterCommand {
        private final int configNum;

        @Override
        public boolean readOnly() {
            return true;
        }
    }

    public interface ShardMasterResult extends Result {
    }

    @Data
    public static final class Ok implements ShardMasterResult {
    }

    @Data
    public static final class Error implements ShardMasterResult {
    }

    @Data
    public static final class ShardConfig implements ShardMasterResult {
        private final int configNum;

        // groupId -> <group members, shard numbers>
        private final Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo;

    }

    // public static class GroupingComparatorInc implements Comparator<Pair<Integer, Set<Integer>>>{
    //     public int compare(Pair<Integer, Set<Integer>> p1, Pair<Integer, Set<Integer>> p2) {
    //         return p1.getValue().size() - p2.getValue().size();
    //     }
    // }

    // public static class GroupingComparatorDec implements Comparator<Pair<Integer, Set<Integer>>>{
    //     public int compare(Pair<Integer, Set<Integer>> p1, Pair<Integer, Set<Integer>> p2) {
    //         return p2.getValue().size() - p1.getValue().size();
    //     }
    // }

    @Override
    public Result execute(Command command) {
        if (command instanceof Join) {
            Join join = (Join) command;

            // Your code here...
            int groupId = join.groupId();
            Set<Address> servers = join.servers();

            if (configNum >= INITIAL_CONFIG_NUM && configHistory.get(configNum).groupInfo().containsKey(groupId)){
                return new Error();
            }

            if (configNum == INITIAL_CONFIG_NUM - 1){
                Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo = new HashMap<>();
                Set<Integer> shardsSet = new HashSet<>();
                for(int i = 1; i <= numShards; i++){
                    shardsSet.add(i);
                }
                Pair<Set<Address>, Set<Integer>> pair = Pair.of(servers, shardsSet);
                groupInfo.put(groupId, pair);
                ShardConfig firstConfig = new ShardConfig(configNum+1, groupInfo);
                configHistory.put(configNum+1, firstConfig);
            } else {
                Map<Integer, Pair<Set<Address>, Set<Integer>>> nextGroupInfo = new HashMap<>();
                for (int key : configHistory.get(configNum).groupInfo().keySet()){
                    Set<Address> servers_copy = new HashSet<>(configHistory.get(configNum).groupInfo().get(key).getKey());
                    Set<Integer> shards_copy = new HashSet<>(configHistory.get(configNum).groupInfo().get(key).getValue());
                    nextGroupInfo.put(key, Pair.of(servers_copy, shards_copy));
                }
                nextGroupInfo.put(groupId, Pair.of(new HashSet<>(servers), new HashSet<>()));
                //pq.add(Pair.of(groupId, new HashSet<>()));
                int[] stats = checkBalanced(nextGroupInfo);
                while(stats[0] >= 2){
                    // TODO
                    //Pair<Integer, Set<Integer>> removeFrom = pq_inv.poll();
                    //Pair<Integer, Set<Integer>> addTo = pq.poll();
                    int toMove = nextGroupInfo.get(stats[2]).getValue().iterator().next();
                    nextGroupInfo.get(stats[2]).getValue().remove(toMove);
                    nextGroupInfo.get(stats[1]).getValue().add(toMove);
                    // for (int first : removeFrom.getValue()){
                    //     removeFrom.getValue().remove(first);
                    //     addTo.getValue().add(first);
                    //     nextGroupInfo.get(removeFrom.getKey()).getValue().remove(first);
                    //     nextGroupInfo.get(addTo.getKey()).getValue().add(first);
                    //     break;
                    // }
                    // pq_inv.add(removeFrom);
                    // pq.add(addTo);
                    stats = checkBalanced(nextGroupInfo);
                }
                ShardConfig nextConfig = new ShardConfig(configNum+1, nextGroupInfo);
                configHistory.put(configNum+1, nextConfig);
            }
            configNum++;
            //addAllToPriorityQueues();
            return new Ok();
        }

        if (command instanceof Leave) {
            Leave leave = (Leave) command;

            // Your code here...
            int groupId = leave.groupId();

            if (configNum ==INITIAL_CONFIG_NUM-1 || !configHistory.get(configNum).groupInfo().containsKey(groupId)){
                return new Error();
            }

            Map<Integer, Pair<Set<Address>, Set<Integer>>> nextGroupInfo = new HashMap<>();
            for (int key : configHistory.get(configNum).groupInfo().keySet()){
                if (key != groupId) {
                    Set<Address> servers_copy = new HashSet<>(configHistory.get(configNum).groupInfo().get(key).getKey());
                    Set<Integer> shards_copy = new HashSet<>(configHistory.get(configNum).groupInfo().get(key).getValue());
                    nextGroupInfo.put(key, Pair.of(servers_copy, shards_copy));
                }
            }

            Set<Integer> shards = configHistory.get(configNum).groupInfo().get(groupId).getValue();
            int[] stats;
            for (int shard : shards) {
                stats = checkBalanced(nextGroupInfo);
                nextGroupInfo.get(stats[1]).getValue().add(shard);
            }

            stats = checkBalanced(nextGroupInfo);
            while(stats[0] >= 2){
                int toMove = nextGroupInfo.get(stats[2]).getValue().iterator().next();
                nextGroupInfo.get(stats[2]).getValue().remove(toMove);
                nextGroupInfo.get(stats[1]).getValue().add(toMove);
                stats = checkBalanced(nextGroupInfo);
            }

            // //better way to do this ??
            // Iterator iter = pq.iterator();
            // Pair<Integer, Set<Integer>> temp = null;
            // while(iter.hasNext()) {
            //     temp = (Pair<Integer, Set<Integer>>) iter.next();
            //     if(temp.getKey().equals(groupId)) {
            //         break;
            //         //pq.remove(temp);
            //     }
            // }
            // pq.remove(temp);

            // while(nextGroupInfo.get(groupId).getValue().size() != 0){
            //     Pair<Integer, Set<Integer>> addTo = pq.poll();
            //     for (int first : nextGroupInfo.get(groupId).getValue()){
            //         addTo.getValue().add(first);
            //         nextGroupInfo.get(addTo.getKey()).getValue().add(first);
            //         nextGroupInfo.get(groupId).getValue().remove(first);
            //         break;
            //     }
            //     pq.add(addTo);
            // }
            // nextGroupInfo.remove(groupId);
            // while(!checkBalanced(nextGroupInfo)) {
            //     Pair<Integer, Set<Integer>> removeFrom = pq_inv.poll();
            //     Pair<Integer, Set<Integer>> addTo = pq.poll();
            //     for (int first : removeFrom.getValue()){
            //         removeFrom.getValue().remove(first);
            //         addTo.getValue().add(first);
            //         nextGroupInfo.get(removeFrom.getKey()).getValue().remove(first);
            //         nextGroupInfo.get(addTo.getKey()).getValue().add(first);
            //         break;
            //     }
            //     pq_inv.add(removeFrom);
            //     pq.add(addTo);
            // }
            ShardConfig nextConfig = new ShardConfig(configNum+1, nextGroupInfo);
            configHistory.put(configNum+1, nextConfig);
            configNum++;
            //addAllToPriorityQueues();
            return new Ok();
        }

        if (command instanceof Move) {
            Move move = (Move) command;

            // Your code here...
            int groupId = move.groupId();
            int shardNum = move.shardNum();
            if(shardNum < 1 || shardNum > numShards || configHistory.get(configNum).groupInfo().get(groupId) == null || configHistory.get(configNum).groupInfo().get(groupId).getValue().contains(shardNum)) {
                return new Error();
            }
            Map<Integer, Pair<Set<Address>, Set<Integer>>> nextGroupInfo = new HashMap<>();
            for (int key : configHistory.get(configNum).groupInfo().keySet()){
                Set<Address> servers_copy = new HashSet<>(configHistory.get(configNum).groupInfo().get(key).getKey());
                Set<Integer> shards_copy = new HashSet<>(configHistory.get(configNum).groupInfo().get(key).getValue());
                nextGroupInfo.put(key, Pair.of(servers_copy, shards_copy));
            }

            for(int key : configHistory.get(configNum).groupInfo().keySet()) {
                if(configHistory.get(configNum).groupInfo().get(key).getValue().contains(shardNum)) {
                    nextGroupInfo.get(key).getValue().remove(shardNum);
                    nextGroupInfo.get(groupId).getValue().add(shardNum);
                    break;
                }
            }
            ShardConfig nextConfig = new ShardConfig(configNum+1, nextGroupInfo);
            configHistory.put(configNum+1, nextConfig);
            configNum++;
            //addAllToPriorityQueues();
            return new Ok();
        }

        if (command instanceof Query) {
            Query query = (Query) command;

            // Your code here...
            if (configNum == INITIAL_CONFIG_NUM - 1){
                return new Error();
            } else if (configHistory.containsKey(query.configNum())){
                return configHistory.get(query.configNum());
            } else {
                return configHistory.get(configNum);
            }
        }

        throw new IllegalArgumentException();
    }

    // public void addAllToPriorityQueues() {
    //     pq.clear();
    //     pq_inv.clear();
    //     for(int key : configHistory.get(configNum).groupInfo().keySet()) {
    //         pq.add(Pair.of(key, new HashSet<>(configHistory.get(configNum).groupInfo().get(key).getValue())));
    //         pq_inv.add(Pair.of(key, new HashSet<>(configHistory.get(configNum).groupInfo().get(key).getValue())));
    //     }
    // }

    public int[] checkBalanced(Map<Integer, Pair<Set<Address>, Set<Integer>>> groups) {
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        int min_key = -1;
        int max_key = -1;
        for(int key : groups.keySet()) {
            if(groups.get(key).getValue().size() < min) {
                min = groups.get(key).getValue().size();
                min_key = key;
            }
            if(groups.get(key).getValue().size() > max) {
                max = groups.get(key).getValue().size();
                max_key = key;
            }
        }
        int[] answers = new int[3];
        answers[0] = max - min;
        answers[1] = min_key;
        answers[2] = max_key;
        return answers;
    }
}
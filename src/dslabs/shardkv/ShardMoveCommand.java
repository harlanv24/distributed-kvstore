package dslabs.shardkv;

import lombok.Data;
import dslabs.framework.Command;
import dslabs.atmostonce.AMOApplication;
import dslabs.framework.Address;
import java.util.*;

@Data
public final class ShardMoveCommand implements Command {
    private final int configNum;
    private final int groupId;
    private final Address[] group;
    private final List<Integer> shards;
    private final List<AMOApplication> amoApps;
}
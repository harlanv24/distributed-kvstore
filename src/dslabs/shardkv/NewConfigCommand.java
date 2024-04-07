package dslabs.shardkv;

import lombok.Data;
import dslabs.framework.Command;
import dslabs.shardmaster.ShardMaster.ShardConfig;

@Data
public final class NewConfigCommand implements Command {
    private final ShardConfig shardConfig;
}
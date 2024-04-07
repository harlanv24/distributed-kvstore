package dslabs.shardkv;

import lombok.Data;
import dslabs.framework.Command;

@Data
public final class ShardMoveAckCommand implements Command {
    private final int configNum;
    private final int groupId;
}
package com.subaiqiao.databaseTableAlignment.strategy;

import com.subaiqiao.databaseTableAlignment.strategy.dm.DmDatabaseStrategy;
import com.subaiqiao.databaseTableAlignment.strategy.kingBase.KingBaseDatabaseStrategy;

/**
 * @author Caozhaoyu
 * @date 2025年10月11日 19:12
 */
public class DatabaseStrategyFactory {
    public static DatabaseStrategy getStrategy(DatabaseEnum databaseEnum) {
        return switch (databaseEnum) {
            case DM -> new DmDatabaseStrategy();
            case KING_BASE -> new KingBaseDatabaseStrategy();
            default -> throw new IllegalArgumentException("Unknown database type: " + databaseEnum.name());
        };
    }
}

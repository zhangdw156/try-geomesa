package com.demo;

import org.geotools.data.*;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("hbase.catalog", "geomesa");
        // 已经把 hbase-site.xml 放在 resources 下，所以可以不指定 zookeepers (注释掉是正确的做法)
        // params.put("hbase.zookeepers", "ds245:2181");

        System.out.println("尝试连接 GeoMesa HBase: " + params);
        DataStore datastore = DataStoreFinder.getDataStore(params);

        if (datastore == null) {
            throw new RuntimeException("DataStore is null. 检查连接参数和依赖。");
        }
        System.out.println("成功连接！");

        // ------------------- 步骤 1: 发现所有已存在的 Feature Type 名称 -------------------
        String[] typeNames = datastore.getTypeNames();
        if (typeNames.length == 0) {
            System.out.println("这个 catalog 中没有找到任何已注册的 Feature Type。");
            datastore.dispose();
            return;
        }

        System.out.println("\n在 'geomesa' catalog 中发现了以下 Feature Types:");
        for (String name : typeNames) {
            System.out.println(" - " + name);
        }

        // ------------------- 步骤 2: 选择一个名称并使用 ECQL 查询其数据 -------------------
        String targetTypeName = "beijing_subway_station"; // 确保这个名称在上面的列表中

        // ... (检查 typeExists 的代码保持不变) ...
        boolean typeExists = false;
        for (String name : typeNames) {
            if (name.equalsIgnoreCase(targetTypeName)) {
                typeExists = true;
                targetTypeName = name;
                break;
            }
        }
        if (!typeExists) {
            System.out.println("\n错误: 目标 Feature Type '" + targetTypeName + "' 不存在。");
            datastore.dispose();
            return;
        }

        // =================================================================================
        // ==                        在这里展示 ECQL 的强大功能                           ==
        // =================================================================================

        // --- 示例 1: 使用 IN 关键字查询多条线路的地铁站 ---
        // 这是标准 CQL 做不到的
        String ecqlFilter1 = "line IN ('地铁10号线', '地铁14号线')";
        System.out.println("\n[ECQL 查询 1]: " + ecqlFilter1);
        executeQuery(datastore, targetTypeName, ecqlFilter1);

        // --- 示例 2: 使用字符串函数查询 ---
        // 查询所有英文站名(stationNameEn)以 "zhuang" 结尾的地铁站 (不区分大小写)
        String ecqlFilter2 = "strEndsWith(stationNameEn, 'zhuang') = true";
        System.out.println("\n[ECQL 查询 2]: " + ecqlFilter2);
        executeQuery(datastore, targetTypeName, ecqlFilter2);

        // --- 示例 3: 结合空间函数和属性查询 ---
        // 查询天安门 (116.391, 39.905) 周边 0.1 个地理单位 (大约 10km) 内的所有地铁10号线车站
        String ecqlFilter3 = "DWITHIN(geom, POINT(116.391 39.905), 10, kilometers) AND line = '地铁10号线'";
        System.out.println("\n[ECQL 查询 3]: " + ecqlFilter3);
        executeQuery(datastore, targetTypeName, ecqlFilter3);


        // 清理连接
        datastore.dispose();
    }

    /**
     * 一个辅助方法，用于执行查询并打印结果
     */
    private static void executeQuery(DataStore dataStore, String typeName, String cqlFilter) {
        try {
            Query query = new Query(typeName, ECQL.toFilter(cqlFilter));
            query.setMaxFeatures(10); // 仍然限制最多10条

            System.out.println("  执行查询...");
            try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                         dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT)) {

                if (!reader.hasNext()) {
                    System.out.println("  -> 查询到 0 条记录。");
                    return;
                }

                int count = 0;
                while (reader.hasNext()) {
                    SimpleFeature feature = reader.next();
                    System.out.println("  -> " + (++count) + ": " + DataUtilities.encodeFeature(feature));
                }
            }
        } catch (Exception e) {
            System.err.println("  !! 执行查询时出错: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
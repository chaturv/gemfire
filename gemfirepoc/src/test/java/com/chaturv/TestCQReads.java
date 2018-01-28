package com.chaturv;

import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.query.*;
import org.apache.geode.cache.util.CqListenerAdapter;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestCQReads {

    @Test
    public void testWithoutTransaction() throws Exception {
        ClientCache cc = new ClientCacheFactory().
                set("cache-xml-file", "client-cache.xml")
                .create();

        QueryService queryService = cc.getQueryService();

        final Map<Object, Object> localMap = new HashMap<>();
        CqListener listener = new CqListenerAdapter() {
            @Override
            public void onEvent(CqEvent event) {
                System.out.println("Received event: " + event);
                localMap.put(event.getKey(), event.getNewValue());
            }

            @Override
            public void onError(CqEvent errEvent) {
                throw new RuntimeException(errEvent.getThrowable());
            }
        };

        CqAttributesFactory cqf = new CqAttributesFactory();
        cqf.addCqListener(listener);
        CqAttributes cqa = cqf.create();

        String cqName = "test-cq";
        String queryStr = "SELECT * FROM /test-region";
        CqQuery cqQuery = queryService.newCq(cqName, queryStr, cqa);
        cqQuery.execute();

        Region<Object, Object> region = cc.getRegion("test-region");
        region.put("key-1", "value-1");

        //wait
        Thread.sleep(5000);

        Assert.assertTrue(!localMap.isEmpty());
        Assert.assertTrue(localMap.containsKey("key-1"));
        Assert.assertEquals("value-1", localMap.get("key-1"));
    }

    @Test
    public void testWithSuspendedTransaction() throws Exception {
        ClientCache cc = new ClientCacheFactory().
                set("cache-xml-file", "client-cache.xml")
                .create();

        QueryService queryService = cc.getQueryService();

        final Map<Object, Object> localMap = new HashMap<>();
        CqListener listener = new CqListenerAdapter() {
            @Override
            public void onEvent(CqEvent event) {
                System.out.println("Received event: " + event);
                localMap.put(event.getKey(), event.getNewValue());
            }

            @Override
            public void onError(CqEvent errEvent) {
                throw new RuntimeException(errEvent.getThrowable());
            }
        };

        CqAttributesFactory cqf = new CqAttributesFactory();
        cqf.addCqListener(listener);
        CqAttributes cqa = cqf.create();

        String cqName = "test-cq";
        String queryStr = "SELECT * FROM /test-region";
        CqQuery cqQuery = queryService.newCq(cqName, queryStr, cqa);
        cqQuery.execute();

        //wait
        Thread.sleep(2000);
        System.out.println("Resuming after calling cqQuery.execute();");

        //put in a transaction
        CacheTransactionManager txnManager = cc.getCacheTransactionManager();
        //begin
        txnManager.begin();
        //put in cache
        Region<Object, Object> region = cc.getRegion("test-region");
        region.put("key-1-tran", "value-1-tran");
        //suspend
        TransactionId txnId = txnManager.suspend();

        //wait
        Thread.sleep(5000);
        System.out.println("Resuming after calling txnManager.suspend();");

        //assert
        Assert.assertTrue(localMap.isEmpty()); //should be empty

        //resume
        txnManager.resume(txnId);
        txnManager.commit();
        System.out.println("Called txnManager.commit();");

        Assert.assertTrue(!localMap.isEmpty());
        Assert.assertTrue(localMap.containsKey("key-1-tran"));
        Assert.assertEquals("value-1-tran", localMap.get("key-1-tran"));
    }

    @Test
    public void testWithExceptionInTransaction() throws Exception {
        ClientCache cc = new ClientCacheFactory().
                set("cache-xml-file", "client-cache.xml")
                .create();

        QueryService queryService = cc.getQueryService();

        final Map<Object, Object> localMap = new HashMap<>();
        CqListener listener = new CqListenerAdapter() {
            @Override
            public void onEvent(CqEvent event) {
                System.out.println("Received event: " + event);
                localMap.put(event.getKey(), event.getNewValue());
            }

            @Override
            public void onError(CqEvent errEvent) {
                throw new RuntimeException(errEvent.getThrowable());
            }
        };

        CqAttributesFactory cqf = new CqAttributesFactory();
        cqf.addCqListener(listener);
        CqAttributes cqa = cqf.create();

        String cqName = "test-cq";
        String queryStr = "SELECT * FROM /test-region";
        CqQuery cqQuery = queryService.newCq(cqName, queryStr, cqa);
        cqQuery.execute();

        //wait
        Thread.sleep(2000);
        System.out.println("Resuming after calling cqQuery.execute();");

        //put in a transaction
        CacheTransactionManager txnManager = cc.getCacheTransactionManager();
        //begin
        txnManager.begin();
        //put in cache
        try {
            Region<Object, Object> region = cc.getRegion("test-region");
            region.put("key-1-tran", "value-1-tran");

            Thread.sleep(3000);
            System.out.println("Resumed after calling region.put(\"key-1-tran\", \"value-1-tran\");");
            //assert
            Assert.assertTrue(localMap.isEmpty()); //should be empty

            System.out.println("Throwing exception now...");
            throw new RuntimeException();

        } catch (Exception e) {
            txnManager.rollback();
            System.out.println("Rolled back transaction!");
        }

        //wait
        Thread.sleep(3000);
        System.out.println("Resuming after waiting a while...");

        //assert
        Assert.assertTrue(localMap.isEmpty()); //should be empty
    }
}

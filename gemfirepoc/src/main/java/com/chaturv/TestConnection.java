package com.chaturv;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;

public class TestConnection {

    public static void main(String[] args) {
        new TestConnection().testConnection();
    }

    public void testConnection() {
        ClientCache cc = new ClientCacheFactory().
                set("cache-xml-file", "client-cache.xml")
                .create();
        Region<Object, Object> region = cc.getRegion("test-region");
        region.put("key-1", "value-1");

        assert region.get("key-1").equals("value-1");
    }

}

package com.zero.apache.kudu;

import com.zero.apache.kudu.common.excute.Example;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.Assert.assertTrue;

@SpringBootTest
class ZeroApacheKuduApplicationTests {

    private static final String KUDU_MASTERS = System.getProperty("kuduMasters", "localhost:7051");
    KuduClient client;

    @BeforeEach
    public void init(){
        client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();
    }

    @Test
    public void testCreateExampleTable() throws KuduException {
        String tableName = "test_create_example";
        Example.createExampleTable(client, tableName);
        assertTrue(client.tableExists(tableName));
    }

    @Test
    public void testInsertRows() throws KuduException {
        String tableName = "test_create_example";
        // Example.insertRows(client,tableName,100);
        System.out.println(client.getTableStatistics(tableName).getLiveRowCount());
    }

}

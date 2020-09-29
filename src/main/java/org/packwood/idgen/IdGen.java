package org.packwood.idgen;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;

import java.time.LocalTime;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.StreamSupport;

public class IdGen {

    private final DynamoDB dynamoDB;
    private final String idName;
    private final Table idTable;
    private final Table idHwmTable;

    private final AtomicLong idHwm;

    public IdGen(String idName) {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.EU_WEST_2)
                //.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2"))
                .build();

        dynamoDB = new DynamoDB(client);

        idHwmTable = setupIdHwmTable(idName);
        this.idName = idName;
        idTable = getOrCreateTable(idName, this::createIdTable);
        idHwm = new AtomicLong(0);

    }

    public static void main(String[] args) throws Exception {
        IdGen idGen = new IdGen("plop");

        for(int i=0; i<1000000; i++) {
            long id = idGen.getId(UUID.randomUUID().toString());
            if (id % 1000 == 0) {
                System.out.println(id);
                System.out.println(LocalTime.now());
            }
        }

    }

    public long getId(String hash) {
        GetItemSpec spec = new GetItemSpec().withPrimaryKey("hash", hash);
        Item item = idTable.getItem(spec);
        if (item == null) {
            long nextId = getNextId();
            item = new Item().withPrimaryKey("hash", hash).withLong("id", nextId);
            idTable.putItem(item);
        }
        return item.getLong("id");
    }

    private long getNextId() {
        UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("idName", idName)
                .withUpdateExpression("set max_id = max_id + :val")
                .withValueMap(new ValueMap().withNumber(":val", 1)).withReturnValues(ReturnValue.UPDATED_NEW);

        try {
//            System.out.println("Incrementing an atomic counter...");
            UpdateItemOutcome outcome = idHwmTable.updateItem(updateItemSpec);
//            System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());
            return outcome.getItem().getLong("max_id");
        }
        catch (Exception e) {
            System.err.println("Unable to update item: " + idName);
            System.err.println(e.getMessage());
        }

        return 0;

    }

    private Table getOrCreateTable(String tableName, Function<String,Table> tableFactory) {
        Page<Table, ListTablesResult> tables = dynamoDB.listTables().firstPage();

        return  StreamSupport
                .stream(tables.spliterator(), false)
                .filter((x)->x.getTableName().equals(tableName))
                .findAny()
                .orElseGet(()->tableFactory.apply(tableName));
    }

    private Table createIdTable(String idName) {
        System.out.println("Attempting to create table; please wait...");
        Table table = dynamoDB.createTable(idName,
                Arrays.asList(new KeySchemaElement("hash", KeyType.HASH)),
                Arrays.asList(new AttributeDefinition("hash", ScalarAttributeType.S)),
                new ProvisionedThroughput(10L, 10L));
        try {
            table.waitForActive();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }



        System.out.println("Success.  Table status: " + table.getDescription().getTableStatus());
        return table;
    }

    private Table createIdHwmTable(String idHwmTableName) {
        System.out.println("Attempting to create table; please wait...");
        Table table = dynamoDB.createTable(idHwmTableName,
                Arrays.asList(new KeySchemaElement("idName", KeyType.HASH)),
                Arrays.asList(new AttributeDefinition("idName", ScalarAttributeType.S)),
                new ProvisionedThroughput(10L, 10L));
        try {
            table.waitForActive();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Success.  Table status: " + table.getDescription().getTableStatus());
        return table;
    }

    private Table setupIdHwmTable(String idName) {
        Table idHwmTable = getOrCreateTable("idHwm", this::createIdHwmTable);

        GetItemSpec spec = new GetItemSpec().withPrimaryKey("idName", idName);
        Item item = idHwmTable.getItem(spec);
        if (item == null) {
            item = new Item().withPrimaryKey("idName", idName).withLong("max_id", 0);
            idHwmTable.putItem(item);
        }
        return idHwmTable;
    }

}
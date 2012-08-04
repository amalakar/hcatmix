package org.apache.hcatalog.hcatmix;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hcatalog.hcatmix.conf.HiveTableSchemas;
import org.apache.thrift.TException;

import java.util.HashMap;

/**
 * Author: malakar
 */
public class HiveTableCreator {

    HiveMetaStoreClient hiveClient;



    public HiveTableCreator() throws MetaException {
        HiveConf hiveConf = new HiveConf(HiveTableCreator.class);
        //hiveClient = new HiveMetaStoreClient(hiveConf);

        Table tbl = new Table();
        //tbl.setParameters();
        //hiveClient.createTable();
    }

    public void createTables(HiveTableSchemas tableSchemas) {


        for (HiveTableSchemas.HiveTableSchema hiveTableSchema : tableSchemas) {
            Table table = new Table();
            table.setDbName(tableSchemas.getDatabaseName());
            table.setTableName(hiveTableSchema.getName());
            StorageDescriptor sd = new StorageDescriptor();
            sd.setCols(hiveTableSchema.getColumns());
            table.setSd(sd);
            sd.setParameters(new HashMap<String, String>());
            sd.setSerdeInfo(new SerDeInfo());
            sd.getSerdeInfo().setName(table.getTableName());
            sd.getSerdeInfo().setParameters(new HashMap<String, String>());

            sd.setInputFormat(org.apache.hadoop.hive.ql.io.RCFileInputFormat.class.getName());
            sd.setOutputFormat(org.apache.hadoop.hive.ql.io.RCFileOutputFormat.class.getName());
            sd.getSerdeInfo().getParameters().put(
                    org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, "1");
            sd.getSerdeInfo().setSerializationLib(
                    org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe.class.getName());
            table.setPartitionKeys(hiveTableSchema.getPartitions());

            try {
                System.out.println(table.getTableName());
//                hiveClient.createTable(table);
//            } catch (AlreadyExistsException e) {
//                e.printStackTrace();
//            } catch (InvalidObjectException e) {
//                e.printStackTrace();
//            } catch (MetaException e) {
//                e.printStackTrace();
//            } catch (NoSuchObjectException e) {
//                e.printStackTrace();
//            } catch (TException e) {
//                e.printStackTrace();
            } finally {

            }

        }
    }



}

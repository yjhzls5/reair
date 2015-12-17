package com.airbnb.di.hive.batchreplication.udf;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 *
 */
public class UDFRemoveFile extends GenericUDF {
    private static final Log LOG = LogFactory.getLog(UDFRemoveFile.class);
    private static final ImmutableMap<String, String> NAMENODE_MAP =
            ImmutableMap.of("pinky", "nn2.pinky.musta.ch", "brain", "nn1.brain.musta.ch", "silver", "airfs-silver");

    private transient ObjectInspectorConverters.Converter converterArg0;
    private transient ObjectInspectorConverters.Converter converterArg1;
    private transient ObjectInspectorConverters.Converter converterArg2;
    private transient MapredContext context;

    @Override
    public void configure(MapredContext context) {
        this.context = context;
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if(objectInspectors.length != 3) {
            throw new UDFArgumentException("Invalid number of arguments.");
        } else {
            this.converterArg0 = ObjectInspectorConverters
                    .getConverter(objectInspectors[0], PrimitiveObjectInspectorFactory.writableStringObjectInspector);
            this.converterArg1 = ObjectInspectorConverters.
                    getConverter(objectInspectors[1], PrimitiveObjectInspectorFactory.writableStringObjectInspector);
            this.converterArg2 = ObjectInspectorConverters.
                    getConverter(objectInspectors[2], PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
            return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        }
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        String rootPath = this.converterArg0.convert(deferredObjects[0].get()).toString();
        String flag = this.converterArg1.convert(deferredObjects[1].get()).toString();
        Boolean dryRun = ((BooleanWritable)this.converterArg2.convert(deferredObjects[2].get())).get();
        LOG.info(rootPath + ":" + dryRun.toString());

        Path silverPath = new Path("hdfs://" + NAMENODE_MAP.get("silver") + rootPath);
        Path pinkyPath = new Path("hdfs://" + NAMENODE_MAP.get("pinky") + rootPath);
        Path brainPath = new Path("hdfs://" + NAMENODE_MAP.get("brain") + rootPath);
        String result = "";
        try {
            FileSystem silverFs = silverPath.getFileSystem(context.getJobConf());
            FileSystem pinkyFs = pinkyPath.getFileSystem(context.getJobConf());
            FileSystem brainFs = brainPath.getFileSystem(context.getJobConf());

            if (silverFs.exists(silverPath) && !pinkyFs.exists(pinkyPath) && !brainFs.exists(brainPath)) {
                result += "qualify for remove";
                if (!dryRun) {
                    silverFs.delete(silverPath, true);
                    result += "removed";
                }
            } else {
                result += "not for removal";
            }

        } catch (IOException e) {
            LOG.error(e.getMessage());
        }

        return new Text(result);
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "removefile()";
    }
}

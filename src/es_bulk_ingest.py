from pyspark import SparkContext, SparkConf
import json

conf = SparkConf().setAppName("ES Ingest")
sc = SparkContext(conf=conf)

es_write_conf = {
    "es.nodes" : "localhost",
    "es.port" : "9200",
    "es.resource" : "my-index/doc",
    "es.nodes.client.only" : "true",
    "es.input.json" : "yes"
}

hdfs_path = "/my/json/data/"

def f(line):
    try:
        data = json.loads(line)
        doc = {}
        doc['data'] = data
        return [json.dumps(doc)]
    except:
        return []

#d = sc.textFile(fp)

d = sc.textFile(hdfs_path).flatMap(f).map(lambda x : ('key', x))

d.saveAsNewAPIHadoopFile(
    path='-', 
    outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
    keyClass="org.apache.hadoop.io.NullWritable",
    valueClass="org.apache.hadoop.io.Text",
    #valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
    conf=es_write_conf)


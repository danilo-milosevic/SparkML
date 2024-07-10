from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, StringType
import sys
from pyspark.sql.functions import col, unix_timestamp
from abc import abstractmethod
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.regression import GBTRegressor, GBTRegressionModel
import os
import time

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

class Predictor:
    @abstractmethod
    def fit(ABC, X):
        pass
    @abstractmethod
    def predict(ABC, X):
        pass

class RegressionPredictor(Predictor):
    def __init__(self, target_label:str, features_label:str='features'):
        self.gbt = GBTRegressor(featuresCol=features_label, labelCol=target_label, seed = 1)
        self.model:GBTRegressionModel = None

    def fit(self, X):
        self.model = self.gbt.fit(X)

    def predict(self, X):
        return self.model.transform(X)
    
    def save(self, path):
        print("Saving to: ", path)
        self.model.write().overwrite().save(path)

    def load(self, path:str):
        print("Loading from: ", path)
        self.model = GBTRegressionModel()
        self.model.load(path)
    
    def name(self):
        return "GBT"

class ClusteringPredictor(Predictor):
    def __init__(self, k=2):
        self.kmeans = KMeans().setK(k).setSeed(1)
        self.model:KMeansModel = None

    def fit(self, X):
        self.model = self.kmeans.fit(X)

    def predict(self, X):
        return self.model.transform(X)
    
    def save(self, path):
        print("Saving to: ", path)
        self.model.write().overwrite().save(path)

    def load(self, path:str):
        print("Loading from: ", path)
        self.model = KMeansModel()
        self.model.load(path)

    def name(self):
        return "KM"
    
class ConsumerSpark:
    minY = -1.68
    minX = -74.273621
    maxX = 25447.74
    maxY = 36412.67

    xZones = 4  
    yZones = 4
    blockSizeX = None
    blockSizeY = None
    producer = None

    @staticmethod
    def select_emissions(stream:SparkSession): # type: ignore
        return stream.selectExpr("split(value, ';') as parsed").selectExpr( # type: ignore
                "parsed[0] AS timestamp",
                "parsed[1] AS CO",
                "parsed[2] AS CO2",
                "parsed[3] AS HC",
                "parsed[4] AS NOx",
                "parsed[5] AS PMx",
                "parsed[8] AS electricity",
                "parsed[9] AS fuel",
                "parsed[10] AS id",
                "parsed[12] AS noise",
                "parsed[15] AS speed",
                "parsed[16] AS type",
                "parsed[18] AS x",
                "parsed[19] AS y"
            )
    
    @staticmethod
    def process_region(stream:SparkSession, window_seconds = 15*60):
        parsed = stream.select( # type: ignore
            col("timestamp").cast("timestamp"),
            col("x").cast(DoubleType()),
            col("y").cast(DoubleType()),
        )
        return parsed

    @staticmethod
    def process_emissions(stream:SparkSession):
        parsed = stream.select( # type: ignore
            col("timestamp").cast("timestamp"),
            col("CO").cast(DoubleType()),
            col("CO2").cast(DoubleType()),
            col("HC").cast(DoubleType()),
            col("NOx").cast(DoubleType()),
            col("PMx").cast(DoubleType()),
            col("electricity").cast(DoubleType()),
            col("fuel").cast(DoubleType()),
            col("id").cast(DoubleType()),
            col("noise").cast(DoubleType()),
            col("speed").cast(DoubleType()),
            col("type")
        )
        return parsed
    
    def train_model(self, pred: Predictor, X, col_list, output):
        if isinstance(pred, RegressionPredictor):
            if output not in ['CO','CO2','HC','NOx',"PMx"]:
                print("Output can only be one of: CO, CO2, HC, NOx or PMx")

        train_data = X.withColumn("timestamp_ms", unix_timestamp(col("timestamp")) * 1000)
        assembler = VectorAssembler(
            inputCols=col_list,
            outputCol="features",
            handleInvalid='skip'
        )
        assembled_data = assembler.transform(train_data)

        if isinstance(pred, RegressionPredictor):
            assembled_data = assembled_data.withColumnRenamed(output, "label")

        pred.fit(assembled_data)
        pred.save("./models/"+pred.name())
        
    def predict(self, pred: Predictor, X, col_list, output):        
        if pred.model is None:
            pred.load('./models/'+pred.name())    
        if isinstance(pred, RegressionPredictor):
            if output not in ['CO','CO2','HC','NOx',"PMx"]:
                print("Output can only be one of: CO, CO2, HC, NOx or PMx")
        
        predict_data = X.withColumn("timestamp_ms", unix_timestamp(col("timestamp")) * 1000)

        try:
            assembler = VectorAssembler(
                inputCols=col_list,
                outputCol="features",
                handleInvalid='skip'
            )
            assembled_data = assembler.transform(predict_data)
        except Exception as e:
            print("Failed to assemble: ", e)
            exit(1)

        if isinstance(pred, RegressionPredictor):
            predictions = pred.predict(assembled_data).select(col("features").cast(StringType()), col("prediction"), col(output))
        else:
            predictions = pred.predict(assembled_data).select(col("features").cast(StringType()), col("prediction"))
        return predictions.withColumnRenamed("prediction", output+"_predicted")
       
    def consume(self, config):
        start = time.time()
        #Create spark app and datastream
        try:
            spark = SparkSession.builder.appName("EMS").master(f"local[{config['num_proc']}]").getOrCreate() # type: ignore
            kafkaDF = spark.readStream.format("kafka")\
                .option("kafka.bootstrap.servers", "kafka:9092")\
                .option("subscribe", config['train_topic'])\
                .option("startingOffsets", "earliest")\
                .load()
            dataStream_train = kafkaDF.selectExpr("CAST(value AS STRING)").alias("value")
            kafkaDF_pred = spark.readStream.format("kafka")\
                .option("kafka.bootstrap.servers", "kafka:9092")\
                .option("subscribe", config['predict_topic'])\
                .option("startingOffsets", "earliest")\
                .load()
            dataStream_pred = kafkaDF_pred.selectExpr("CAST(value AS STRING)").alias("value")
        except Exception as e:
            print("Failed to create data stream: ", e)
            exit(1)

        #Create a predictor
        try:
            predict_ems: Predictor = RegressionPredictor('label','features')
            predict_cluster: Predictor = ClusteringPredictor(5)
        except Exception as e:
            print("Failed to create a predictor: ",e)
            exit(1)

        #Parse the data
        try:
            train_data = ConsumerSpark.select_emissions(dataStream_train)
            train_data = train_data.filter(col(config['label']) > 0)

            pred_data = ConsumerSpark.select_emissions(dataStream_pred)
        except Exception as e:
            print("Failed to parse data: ",e)
            exit(1)

        #Preprocess the data
        try:
            train_ems = ConsumerSpark.process_emissions(train_data)
            pred_ems = ConsumerSpark.process_emissions(pred_data)

            train_cluster = ConsumerSpark.process_region(train_data)
            pred_cluster = ConsumerSpark.process_region(pred_data)
        except Exception as e:
            print("Failed to process data: ", e)
            exit(1)
        
        train_ems = train_ems.writeStream.format("memory").queryName("train_ems").start().awaitTermination(10)
        train_ems = spark.sql("select * from train_ems")

        train_cluster = train_cluster.writeStream.format("memory").queryName("train_cluster").start().awaitTermination(10)
        train_cluster = spark.sql("select * from train_cluster")
        
        #Train
        print("Train...")
        try:
            self.train_model(predict_ems, train_ems, config['train_inputs_ems'], config['label'])
            self.train_model(predict_cluster, train_cluster, config['train_inputs_clusters'], config['label'])
        except Exception as e:
            print("Failed to train the model: ", e)

        print("Predict...")
        try:
            predicted_ems = self.predict(predict_ems, pred_ems, config['train_inputs_ems'], config['label'])
            predicted_cong = self.predict(predict_cluster, pred_cluster, config['train_inputs_clusters'], '')

            query_ems = predicted_ems \
                    .writeStream \
                    .outputMode("append") \
                    .format("csv") \
                    .option("path", f"./{config['label']}_reggresion") \
                    .option("checkpointLocation", "./checkpoint_regression") \
                    .start()  
            query_cong = predicted_cong \
                    .writeStream \
                    .outputMode("append") \
                    .format("csv") \
                    .option("path", f"./clusterization") \
                    .option("checkpointLocation", "./checkpoint_clusters") \
                    .start() 
            query_ems.awaitTermination()
            query_cong.awaitTermination()
        except Exception as e:
            end = time.time()
            print("Time in s: ",(end-start))
            print("Failed to predict using the model: ", e)

    @staticmethod
    def get_app_config(args):
        config = {
            'train_topic':'ems_train_topic',
            'predict_topic':'ems_pred_topic',
            'group':'emission-group',
            'to_print':False,
            'error':False,
            'train_inputs_ems':['timestamp_ms','fuel','electricity','noise','speed'],
            'train_inputs_clusters':['timestamp_ms','x','y'],
            'label':'CO',
            'num_proc':'*'
        }
        i=1
        try:
            if args[i] == '--label':
                i+=1
                config['label'] = str(args[i])
                i+=1
            
            if (args[i]=="--print"):
                config['to_print'] = True
                i+=1

            if(args[i]=='--n'):
                i+=1
                try:
                    config['num_proc'] = str(int(args[i]))
                except:
                    config['num_proc'] = '*'
                i+=1
        except Exception as e:
            pass
        return config

if __name__ == "__main__":
    c = ConsumerSpark.get_app_config(sys.argv)
    emsConsumer = ConsumerSpark()

    try:
        emsConsumer.consume(c)
    except Exception as e:
        print(e)

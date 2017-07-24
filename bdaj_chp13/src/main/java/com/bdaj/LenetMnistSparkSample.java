package com.bdaj;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.deeplearning4j.datasets.iterator.impl.MnistDataSetIterator;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.LearningRatePolicy;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.Updater;
import org.deeplearning4j.nn.conf.inputs.InputType;
import org.deeplearning4j.nn.conf.layers.ConvolutionLayer;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.conf.layers.SubsamplingLayer;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.spark.api.TrainingMaster;
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer;
import org.deeplearning4j.spark.impl.paramavg.ParameterAveragingTrainingMaster;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Train a simple/small MLP on MNIST data using Spark, then evaluate it on the test set in a distributed manner
 * <p>
 * Note that the network being trained here is too small to make proper use of Spark - but it shows the configuration
 * and evaluation used for Spark training.
 * <p>
 * <p>
 * To run the example locally: Run the example as-is. The example is set up to use Spark local by default.
 * NOTE: Spark local should only be used for development/testing. For data parallel training on a single machine
 * (for example, multi-GPU systems) instead use ParallelWrapper (which is faster than using Spark for training on a single machine).
 * See for example MultiGpuLenetMnistExample in dl4j-cuda-specific-examples
 * <p>
 * To run the example using Spark submit (for example on a cluster): pass "-useSparkLocal false" as the application argument,
 * OR first modify the example by setting the field "useSparkLocal = false"
 *
 * @author Alex Black
 */
public class LenetMnistSparkSample {
    private static final Logger log = LoggerFactory.getLogger(LenetMnistSparkSample.class);

    @Parameter(names = "-useSparkLocal", description = "Use spark local (helper for testing/running without spark submit)", arity = 1)
    private boolean useSparkLocal = true;

    @Parameter(names = "-batchSizePerWorker", description = "Number of examples to fit each worker with")
    private int batchSizePerWorker = 16;

    @Parameter(names = "-numEpochs", description = "Number of epochs for training")
    private int numEpochs = 15;

    public static void main(String[] args) throws Exception {
        new LenetMnistSparkSample().entryPoint(args);
    }

    protected void entryPoint(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf();
        if (useSparkLocal) {
            sparkConf.setMaster("local[*]");
        }
        sparkConf.setAppName("DL4J Spark MLP Example");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        //Load the data into memory then parallelize
        //This isn't a good approach in general - but is simple to use for this example
        DataSetIterator iterTrain = new MnistDataSetIterator(batchSizePerWorker, true, 12345);
        DataSetIterator iterTest = new MnistDataSetIterator(batchSizePerWorker, true, 12345);
        List<DataSet> trainDataList = new ArrayList<DataSet>();
        List<DataSet> testDataList = new ArrayList<DataSet>();
        while (iterTrain.hasNext()) {
            trainDataList.add(iterTrain.next());
        }
        while (iterTest.hasNext()) {
            testDataList.add(iterTest.next());
        }

        JavaRDD<DataSet> trainData = sc.parallelize(trainDataList);
        JavaRDD<DataSet> testData = sc.parallelize(testDataList);


        //----------------------------------
        //Create network configuration and conduct network training
        MultiLayerConfiguration conf = getLenetCnnConfig();

        //Configuration for Spark training: see http://deeplearning4j.org/spark for explanation of these configuration options
        TrainingMaster tm = new ParameterAveragingTrainingMaster.Builder(batchSizePerWorker)    //Each DataSet object: contains (by default) 32 examples
                .averagingFrequency(5)
                .workerPrefetchNumBatches(2)            //Async prefetching: 2 examples per worker
                .batchSizePerWorker(batchSizePerWorker)
                .build();

        //Create the Spark network
        SparkDl4jMultiLayer sparkNet = new SparkDl4jMultiLayer(sc, conf, tm);

        //Execute training:
        for (int i = 0; i < numEpochs; i++) {
            sparkNet.fit(trainData);
            System.out.println("Completed Epoch {}" +  i);
        }

        //Perform evaluation (distributed)
        Evaluation evaluation = sparkNet.evaluate(testData);
        System.out.println("***** Evaluation *****");
        System.out.println(evaluation.stats());

        //Delete the temp training files, now that we are done with them
        tm.deleteTempFiles(sc);

        System.out.println("***** Example Complete *****");
    }

    public static MultiLayerConfiguration getLenetCnnConfig() {
        int nChannels = 1; // Number of input channels
        int outputNum = 10; // The number of possible outcomes
        int batchSize = 64; // Test batch size
        int nEpochs = 1; // Number of training epochs
        int iterations = 1; // Number of training iterations
        int seed = 123; //


        System.out.println("Build model....");

        // learning rate schedule in the form of <Iteration #, Learning Rate>
        Map<Integer, Double> lrSchedule = new HashMap<Integer, Double>();
        lrSchedule.put(0, 0.01);
        lrSchedule.put(1000, 0.005);
        lrSchedule.put(3000, 0.001);

        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .seed(seed)
                .iterations(iterations) // Training iterations as above
                .regularization(true).l2(0.0005)
                .learningRate(.01)//.biasLearningRate(0.02)
                .learningRateDecayPolicy(LearningRatePolicy.Schedule)
                .learningRateSchedule(lrSchedule)
                .weightInit(WeightInit.XAVIER)
                .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
                .updater(Updater.NESTEROVS).momentum(0.9)
                .list()
                .layer(0, new ConvolutionLayer.Builder(5, 5)
                        //nIn and nOut specify depth. nIn here is the nChannels and nOut is the number of filters to be applied
                        .nIn(nChannels)
                        .stride(1, 1)
                        .nOut(20)
                        .activation(Activation.IDENTITY)
                        .build())
                .layer(1, new SubsamplingLayer.Builder(SubsamplingLayer.PoolingType.MAX)
                        .kernelSize(2,2)
                        .stride(2,2)
                        .build())
                .layer(2, new ConvolutionLayer.Builder(5, 5)
                        //Note that nIn need not be specified in later layers
                        .stride(1, 1)
                        .nOut(50)
                        .activation(Activation.IDENTITY)
                        .build())
                .layer(3, new SubsamplingLayer.Builder(SubsamplingLayer.PoolingType.MAX)
                        .kernelSize(2,2)
                        .stride(2,2)
                        .build())
                .layer(4, new DenseLayer.Builder().activation(Activation.RELU)
                        .nOut(500).build())
                .layer(5, new OutputLayer.Builder(LossFunctions.LossFunction.NEGATIVELOGLIKELIHOOD)
                        .nOut(outputNum)
                        .activation(Activation.SOFTMAX)
                        .build())
                .setInputType(InputType.convolutionalFlat(28,28,1)) //See note below
                .backprop(true).pretrain(false).build();
        return conf;
    }
}


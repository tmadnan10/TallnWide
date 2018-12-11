./deleteXtX.sh
echo > dw
echo 1 > ID
$SPARK_HOME/bin/spark-submit --class org.qcri.sparkpca.TallnWide --master spark://ip-172-31-59-114.ec2.internal:7077 --conf spark.driver.maxResultSize=0 --conf spark.network.timeout=4000s --driver-memory 800m --executor-memory 800m --driver-java-options "-Di=../amazon.txt1-10000.seq -Do=. -DdataSet=amazon.txt1-10000.seq -Drows=10000 -Dcols=1000 -DavgCols=4 -Dpcs=10 -DmaxMemory=$1 -Dq=10 -DsubSample=2 -DsubSampleNorm=10 -DcalculateError=1 -DkSingularValue=1472.1220 -Dtolerance=0.05 -DmaxIter=$2" PPCA-Tall-and-Wide-0.0.1-SNAPSHOT.jar

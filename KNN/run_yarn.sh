spark-submit --class KNN \
--num-executors 32 \
--deploy-mode client \
target/scala-2.10/knn-application_2.10-1.0.jar knn_test/input 100 6

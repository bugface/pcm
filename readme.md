cut more than 10000 positive training pairs and more than 10000 negative training pairs into 10 separated files because of training time consume

python script mimics the csv sample provided by dedupe.io

processed_dataset.csv is a light modified data set of original data set (change all female male to F, M, U)

the pcm_settings file was created after the first time run, if the file exist in the current directory, the python script will skip the
training and directly use the settings from this file

the output_v1_verybad.csv is the result from the first test
the output.csv is the second run with previous training and extra 200 trainings from console label by myself
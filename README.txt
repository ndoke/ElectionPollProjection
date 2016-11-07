Cloud Computing for data analytics project

Nachiket Doke
Paravasthu Siddhanthi Navya Keerthi

The commands that should be run in order to execute the program.

hadoop fs -mkdir input
hadoop fs -put Input.csv input
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* CloudProject.java -d build -Xlint
jar -cvf cloudproject.jar -C build/ .
hadoop fs -rm -r output*
hadoop jar cloudproject.jar org.myorg.CloudProject input output


export CLASSPATH=`echo ./lib/*.jar | tr ' ' :`

SCALA_HOME = /mnt/c/Program\ Files\ \(x86\)/scala
SPARK_HOME = /mnt/e/installers/spark-2.2.0-bin-hadoop2.7/bin

#Data Subset
INPUT1 = input/song_info.csv
#INPUT1 = input/MillionSongSubset/song_info_sample.csv
INPUT2 = input/similar_artists.csv
INPUT3 = input/artist_terms.csv
INPUT4 = small

#gunzip:
#	gunzip ./input/MillionSongSubset/*
#	gunzip ./input/MillionSongDataset/*

#gzip:
#	-gzip ./input/MillionSongSubset/*;
#	 gzip ./input/MillionSongDataset/*

all: clean build run report


clean:
	pwd
	-rm -rf out1


run:
	$(SPARK_HOME)/spark-submit --class Main --master local  out1/Cl.jar  $(INPUT1) $(INPUT3) $(INPUT4)

compile:
	mkdir out1
	$(SCALA_HOME)/bin/scalac -classpath $(CLASSPATH) -d out1/Cl.jar src/main/scala/*.scala

build: compile


#report:
	#Rscript -e "rmarkdown::render('./a8_report.Rmd')"

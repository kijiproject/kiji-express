KijiChopsticks
==============

KijiChopsticks provides a simple, data analysis language using
[KijiSchema](https://github.com/kijiproject/kiji-schema/) and
[Scalding](https://github.com/twitter/scalding/).

Compilation
-----------

KijiChopsticks requires [Apache Maven 3](http://maven.apache.org/download.html)
to build. It may built by running the command

    mvn clean package

from the root of the KijiChopsticks repository. This will create a release in
the target directory.

Running the NewsgroupWordCounts example
---------------------------------------

The following instructions assume that a functional
[KijiBento](https://github.com/kijiproject/kiji-bento/) minicluster has been
setup and is running.

First, create and populate the 'words' table:

    kiji-schema-shell --file=words.ddl
    kiji jar target/kiji-chopsticks-0.1.0-SNAPSHOT.jar com.wibidata.lang.NewsgroupLoader \
        kiji://.env/default/words <path/to/newsgroups/root/>

Run the word count outputting to hdfs:

    kiji jar target/kiji-chopsticks-0.1.0-SNAPSHOT.jar \
        com.twitter.scalding.Tool com.wibidata.lang.NewsgroupWordCount \
        --input kiji://.env/default/words --output ./wordcount.tsv --hdfs

Check the results of the job:

    hadoop fs -cat ./wordcounts.tsv


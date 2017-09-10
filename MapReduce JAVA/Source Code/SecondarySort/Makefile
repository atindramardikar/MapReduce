
JAR=$(shell basename `pwd`)

$(JAR).jar: *.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/$(JAR)-all.jar $(JAR).jar

run: $(JAR).jar
	rm -rf output
	hadoop jar $(JAR).jar 1991.csv output

clean:
	rm -rf build $(JAR).jar output .gradle
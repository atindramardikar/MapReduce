
DIR := $(shell basename `pwd`)

$(DIR).jar: mapredpagerank/*.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/$(DIR).jar $(DIR).jar

run: $(DIR).jar clean-output
	hadoop jar $(DIR).jar wikipedia-simple-html.bz2 /Users/atindramardikar/Desktop/MR-HW05/sourceCode/RowvsCol/output

clean-output: 
	rm -rf data? data?? output
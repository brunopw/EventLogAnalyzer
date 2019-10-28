start cmd.exe /k java -cp src/main/resources/hsqldb.jar org.hsqldb.server.Server --database.0 file:eventsdb --dbname.0 edb
TIMEOUT /T 3
start cmd.exe /k java -jar EventLogAnalyzer.jar logfile.txt
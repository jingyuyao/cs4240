# Prerequisite 
Install the `gcloud` commandline tool.

# How to run on Google Cloud DataProc
`./submit.sh --region {cluster-region} --cluster {cluster-name} --properties={spark-properties}`

E.g. `./submit.sh --region northamerica-northeast1 --cluster final-project --properties=spark.driver.memory=10g,spark.executor.memory=10g`

See `gcloud dataproc jobs submit spark --help` for more options.

# How to access Spark UI

```
./socks.sh {master-node-zone} {master-node-name}
./socks_chrome.sh {path-to-a-chrome-executable} {master-node-name}
```
Then navigate to `http://{master-node-name}:8088` in the Chrome browser

E.g.

```
./socks.sh northamerica-northeast1-b final-project-m
./socks_chrome.sh google-chrome-stable final-project-m
```
Open `http://final-project-m:8088`

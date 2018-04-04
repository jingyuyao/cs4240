# Prerequisite 
Install the `gcloud` commandline tool.

# How to run on Google Cloud DataProc
```
sbt assembly
./submit.sh --region {cluster-region} --cluster {cluster-name} -- {import/analyze} {table-name} ...
```

E.g. 
```
sbt assembly
./submit.sh --region northamerica-northeast1 --cluster final-project -- import fh-bigquery:reddit_comments.2009 fh-bigquery:reddit_comments.2010
```

See `gcloud dataproc jobs submit spark --help` for more options. `sbt assembly` may be skipped if the code was not changed.

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

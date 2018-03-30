# How to run on Google Cloud Dataproc
Generate a jar using `sbt assembly`. The jar will be generated in `target/scala-2.11/cs4240.jar`.
You can then upload this jar to Google cloud console and run it by setting the main class to
`cs4240.Main`. Or you can run it using the `gcloud` command tool:
```
gcloud dataproc jobs submit spark --cluster final-project --class cs4240.Main --jars target/scala-2.11/cs4240.jar
```

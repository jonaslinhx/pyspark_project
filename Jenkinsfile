pipeline {
    agent any

    stages {
        stage("Build") {
            steps {
                sh 'pipenv --python python3 sync'
            }
        }
        stsge("Test") {
            steps {
                sh 'pipenv run pytest'
            }
        }
        stage("Package") {
            when {
                anyOf(branch "master" ; branch "release")
            }
            steps {
                sh "zip -r pyspark_project.zip lib"
            }
        }
        stage("Release") {
            when {
                branch "release"
            }
            steps {
                sh "scp -i /home/jonas/cred/edge-node_key.pem -o 'StrictHostKeyChecking no' -r pyspark_project.zip log4j.properties main.py spark-submit.sh conf jonas@40.117.123.12"
            }
        }
        stage("Deploy") {
            when {
                branch "master"
            }
            steps {
                sh "scp -i /home/jonas/cred/edge-node_key.pem -o 'StrictHostKeyChecking no' -r pyspark_project.zip log4j.properties main.py spark-submit.sh conf jonas@40.117.123.12"
            }
        }
    }
}
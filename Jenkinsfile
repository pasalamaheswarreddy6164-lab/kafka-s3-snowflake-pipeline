pipeline {
    agent any

    environment {
        AWS_ACCESS_KEY_ID = credentials('aws-access-key-id')
        AWS_SECRET_ACCESS_KEY = credentials('aws-secret-access-key')
    }

    stages {

        stage('Checkout') {
             steps {
                git branch: 'main', url: 'https://github.com/pasalamaheswarreddy6164-lab/kafka-s3-snowflake-pipeline.git'
            }
        }

        stage('Build Docker Image') {
            steps {
                sh 'docker build -t spark-pipeline .'
            }
        }

        stage('Run Spark Pipeline') {
            steps {
                sh '''
                docker run --rm \
                --network kafka-network \
                -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
                -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
                spark-pipeline
                '''
            }
        }
    }
}
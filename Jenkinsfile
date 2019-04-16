pipeline {
    agent any
    
    environment {
      PROJECT = 'mergin/py-client'
    }
    
    stages {
        stage('Checkout') {
            steps {
              script {
                GIT_VARS = lutraGitLabCheckout("${PROJECT}")
              }
            }
        }

        stage ('Build and publish Wheel') {
            steps {
                script {
                    GIT_VERSION = GIT_VARS.describe
                }

                sh "./update_version.sh ${GIT_VERSION}"
                script {
                    lutraBuildAndUploadToInternalPyPi(GIT_VARS)
                }
            }
        }
    }
}

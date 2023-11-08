@Library(value='ibp-libraries', changelog=false) _
@Library(value = 'msaas-shared-lib', changelog = false) l2

def config = [:]

// Commit hash
def commitId = ""
// Defining version for artifacts when it is a snapshot being built or release
def revisionNo = ""
// Converting Slashes to hyphens from branch name
def branchNameConverted = env.BRANCH_NAME.replace('/','-')
// Adding version suffix to a non-release build.
def versionSuffix = ('master' != env.BRANCH_NAME) ? "-${branchNameConverted}-SNAPSHOT" : ''
// Slack channel. For snapshots has a -snapshot appended
def slackChannel = "build-simplan"
def slackCrChannel = "${slackChannel}-cr-notifications"
slackChannel = ('master' == env.BRANCH_NAME) ? slackChannel : "${slackChannel}-snapshot"
// Slack messages for snapshot and release, please look below set in INIT DEFINITIONS to modify RELEASE MESSAGE value.
def slackRepoName = "*SIMPLAN SPARK*\n"
def slackSnapshotMessage  = "#<${env.RUN_DISPLAY_URL}|${env.BUILD_NUMBER}> for branch ${env.BRANCH_NAME}.\n"
def slackReleaseMessage  = ''
def slackMessage = slackRepoName + "#<${env.RUN_DISPLAY_URL}|${env.BUILD_NUMBER}>. "
// FatJar Name to be found in s3 bucket (it will have a .jar extension)
def artifactName = "simplan-spark-launcher"
// Dependencies properties names
def depPropertiesNames = [
  simplanFrameworkVersion : 'simplan-framework.version'
]


pipeline {
  agent {
    kubernetes {
      defaultContainer "maven"
      yaml """
        apiVersion: v1
        kind: Pod
        metadata:
          annotations:
            iam.amazonaws.com/role: arn:aws:iam::733536204770:role/tech-ea-prd-jenkins
        spec:
          containers:
          - name: maven
            image: 'docker.artifactory.a.intuit.com/maven:3.5.3-jdk-8'
            tty: true
            command:
            - cat
            volumeMounts:
              - name: shared-build-output
                mountPath: /var/run/outputs
          - name: toolbox
            image: docker.intuit.com/data/kgpt/curation/service/jenkins-toolbox:latest
            tty: true
            command:
            - cat
            volumeMounts:
              - name: shared-build-output
                mountPath: /var/run/outputs
          - name: mkdocs
            image: docker.intuit.com/dev-test/oid-mkdocs-builder/service/oid-mkdocs-builder:latest
            tty: true
            command:
            - cat
            volumeMounts:
              - name: shared-build-output
                mountPath: /var/run/outputs
          - name: servicenow
            image: docker.intuit.com/coe/servicenow-cr-agent/service/servicenow-cr-agent:latest
            tty: true
            command: ["cat"]
            imagePullPolicy: Always
            volumeMounts:
              - name: shared-build-output
                mountPath: /var/run/outputs
          volumes:
            - name: shared-build-output
              emptyDir: {}
      """
    }
  }

  options {
    timestamps()
    ansiColor('xterm')
    /*
      daysToKeepStr: history is only kept up to this days.
      numToKeepStr: only this number of build logs are kept.
      artifactDaysToKeepStr: artifacts are only kept up to this days.
      artifactNumToKeepStr: only this number of builds have their artifacts kept.
    */
    buildDiscarder(logRotator(daysToKeepStr:'', numToKeepStr: numbersAccordingBranch(), artifactDaysToKeepStr: '', artifactNumToKeepStr: ''))
  }

  parameters {
    choice(
      name: 'promote',
      choices: ['E2E', 'BETA', 'PROD'],
      description: 'Choose the environment to deploy'
    )
    string(
      name: 'artifactVersion',
      defaultValue: 'e.g. 1.0.0.100',
      description: 'Input the version to release'
    )
  }

  environment {
    IBP_MAVEN_SETTINGS_FILE = credentials("ibp-maven-settings-file")
    MAVEN_ARTIFACTORY_CREDENTIALS = credentials("artifactory-Simplan-Spark")
    MAVEN_ARTIFACTORY_USERID = "${env.MAVEN_ARTIFACTORY_CREDENTIALS_USR}"
    MAVEN_ARTIFACTORY_TOKEN = "${env.MAVEN_ARTIFACTORY_CREDENTIALS_PSW}"
    GIT_BRANCH = "${env.BRANCH_NAME}"
    GIT_URL = "github.intuit.com/Simplan/simplan-spark.git"
    ARTIFACT_VERSION = removeSpacesEnv(params.artifactVersion)
    S3_BUCKET_ROLE = 'arn:aws:iam::733536204770:role/tech-ea-prd-jenkins'
  }

  stages {
    stage ('INIT DEFINITIONS'){
      steps {
        script {
          config = readConfigYAML()
          commitId = sh(script: 'git rev-parse HEAD',returnStdout: true)
          revisionNo = ('master' == env.BRANCH_NAME) ? config.artifactVersion+'.'+env.BUILD_NUMBER : "1.0.0${versionSuffix}"
          def numberToSendInSlack = (env.ARTIFACT_VERSION == "e.g.1.0.0.100" && params.promote == "E2E") ? revisionNo : env.ARTIFACT_VERSION
          slackReleaseMessage = "*${params.promote}*. *RELEASE* v<${env.RUN_DISPLAY_URL}|${numberToSendInSlack}>\n"
          slackMessage = slackRepoName + (('master' == env.BRANCH_NAME) ? slackReleaseMessage : slackSnapshotMessage)
          adjustPOMValues(config, depPropertiesNames)
        }
      }
    }

    stage('BUILD CHECK. PRs & GENERAL BRANCHES') {
      when {
        anyOf {
          changeRequest ()
          allOf{
            not { branch 'master' }
            not { branch 'develop' }
          }
        }
      }
      steps {
        mavenBuildPR("-U -B -s settings.xml")
      }
      post {
        success {
          PRPostSuccess(config)
        }
        always {
          PRPostAlways(config)
        }
      }
    }

    stage('BUILDING STAGE') {
      when {
        allOf {
          not { changeRequest() }
          anyOf {
            branch 'master'
            branch 'develop'
          }
          expression { env.ARTIFACT_VERSION == "e.g.1.0.0.100" && params.promote == "E2E" }
        }
      }

      stages {
        stage('BUILD') {
		      steps {
		        script {
		          sh """
		            echo 'Creating file with versions used.'
		            printf 'simplan.system.ci.spark.version=${revisionNo}\nsimplan.system.ci.spark.framework=${config.simplanFrameworkVersion}\nsimplan.system.ci.spark.commitHash=${commitId}' > spark-core/src/main/resources/simplan-spark-manifest.conf
		          """
		          mavenBuildCI("-P upload-artifact -Drevision=${revisionNo} -U -B -s settings.xml")
		          // Tagging only when it is a realease version (master branch), artifact version is built from config.artifactVersion and buildNo
		          if ( env.BRANCH_NAME == 'master' ) {
		            gitTag(revisionNo, env.GIT_URL)
		          }
		        }
		      }
		      post {
            success {
              CIPostSuccess(config)
            }
            always {
              CIPostAlways(config)
            }
          }
        }

        stage('DOCUMENTATION DEPLOYMENT') {
          when {
            branch 'master'
          }
          steps{
            container('mkdocs'){
              mkdocsFunction()
            }
          }
        }
      }
    }

    stage('LOW ENVIRONMENTS DEPLOYMENT STAGE'){
      when {
        allOf {
          anyOf {
            branch 'develop'
            branch 'master'
          }
          not { changeRequest() }
          expression { 'E2E' == params.promote }
        }
      }
      environment {
        S3_BUCKET_ROLE      = 'arn:aws:iam::673736607118:role/DLInfrastructureRole'
        S3_BUCKET_NAME_DEV  = 's3://databricks-e2-673736607118-superglue-e2e/oregon-prod/3109720659393320/simplan/qal/spark/bin'
        S3_BUCKET_NAME_E2E  = 's3://databricks-e2-673736607118-superglue-e2e/oregon-prod/3109720659393320/simplan/e2e/spark/bin'
      }
      stages {
        // Stages for automatic deployment. This get triggered when building develop or master branch.
        stage('DEV'){
          when {
            branch 'develop'
          }
          steps {
            script {
              container('toolbox') {
                sh (script: 'echo "DEV Deploy"')
                preparePackageToUploadDEVS(artifactName)
                uploadZipToBuckets([env.S3_BUCKET_NAME_DEV], artifactName, env.S3_BUCKET_ROLE)
              }
            }
          }
        }

        stage('E2E'){
          when {
            branch 'master'
          }
          steps {
            script {
              container('toolbox') {
                sh (script: 'echo "E2E Deploy"')
                preparePackageToUploadDEVS(artifactName)
                uploadZipToBuckets([env.S3_BUCKET_NAME_E2E], artifactName, env.S3_BUCKET_ROLE)
              }
            }
          }
        }
      }
    }

    stage('HIGHER ENVIRONMENTS DEPLOYMENT STAGE'){
      when {
        allOf {
          branch 'master'
          not { changeRequest() }
          expression { 'E2E' != params.promote }
          expression { env.ARTIFACT_VERSION != "e.g.1.0.0.100" && env.ARTIFACT_VERSION ==~ /\d+\.\d+\.\d+\.\d+/ }
        }
      }

      environment {
        S3_BUCKET_ROLE           = 'arn:aws:iam::570264151593:role/DLInfrastructureRole'
        S3_BUCKET_NAME_BETA_ONE  = 's3://databricks-e2-570264151593-superglue-prd/oregon-prod/946581739522633/simplan/beta/spark/bin'
        S3_BUCKET_NAME_BETA_TWO  = 's3://databricks-e2-570264151593-superglue2-prd/oregon-prod/2048614189517991/simplan/beta/spark/bin'
        S3_BUCKET_NAME_PROD_ONE  = 's3://databricks-e2-570264151593-superglue-prd/oregon-prod/946581739522633/simplan/prd/spark/bin'
        S3_BUCKET_NAME_PROD_TWO  = 's3://databricks-e2-570264151593-superglue2-prd/oregon-prod/2048614189517991/simplan/prd/spark/bin'
      }

      stages {

        stage('BETA'){
          when {
            expression { 'BETA' == params.promote }
          }
          steps {
            script {
              container('toolbox') {
                sh (script: 'echo "BETA Deploy"')
                preparePackageToUploadPRODS(artifactName)
                uploadZipToBuckets([env.S3_BUCKET_NAME_BETA_ONE, env.S3_BUCKET_NAME_BETA_TWO], artifactName, env.S3_BUCKET_ROLE)
              }
            }
          }
        }

        stage('PROD'){
          when {
            expression { 'PROD' == params.promote }
          }
          stages{
            stage('Create CR') {
              steps {
								slackCRNotification(slackRepoName, slackCrChannel)
                scorecardProdReadiness(config, 'prd')
                container('servicenow') {
                  sh label: 'Opens CR', script: 'echo "Opens CR"'
                  openSnowCR(config, 'prd', config.artifactId)
                }
              }
            }

						stage('PRD DEPLOYMENT'){
							steps {
                script {
                  container('toolbox') {
                    sh (script: 'echo "PROD Deploy"')
                    preparePackageToUploadPRODS(artifactName)
                    uploadZipToBuckets([env.S3_BUCKET_NAME_PROD_ONE, env.S3_BUCKET_NAME_PROD_TWO], artifactName, env.S3_BUCKET_ROLE)
                  }
                }
              }
              post {
                always {
                  script {
                    stage('Close CR') {
                      container('servicenow') {
                        sh label: 'Close CR', script: 'echo "Close CR"'
                        closeSnowCR(config, 'prd')
                      }
                    }
                  }
                }
              }
						}
          }
        }
      }
    }


  }

  post {
    success {
      slackSend(channel: slackChannel, tokenCredentialId: 'slack-token-curation-tf', message: ":white_check_mark: " + slackMessage + "CICD was SUCCESSFUL.")
    }
    failure {
      slackSend(channel: slackChannel, tokenCredentialId: 'slack-token-curation-tf', message: ":x: " + slackMessage + "CICD FAILED.")
    }
    aborted {
      slackSend(channel: slackChannel, tokenCredentialId: 'slack-token-curation-tf', message: ":white_circle: " + slackMessage + "CICD was ABORTED.")
    }
    always {
      customReleaseMetrics(config)
    }
  }
}

def readConfigYAML(){
  def values = readYaml file: "library-config.yaml"
  return values
}

def removeSpacesEnv(String word){
  word = (word?.trim()) ? word.replace(' ', '') : ""
  return word
}

def gitTag(String tagValue, String GIT_URL){
  withCredentials([usernamePassword(credentialsId: 'github-svc-sbseg-ci', passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME')]) {
    sh """
      set +x
      git config user.name svc-sbseg-ci
      git config user.email SBSEGCI-Admins@intuit.com
      git tag ${tagValue}
      git push https://${GIT_USERNAME}:${GIT_PASSWORD}@${GIT_URL} --tags
      set -x
    """
  }
}

def preparePackageToUploadDEVS(artifactName){
  def findJar = sh (script: "find spark-launcher/target -type f -maxdepth 1 -name ${artifactName}-*-jar-with-dependencies.jar",returnStdout: true).trim()
  sh """
    cp ${findJar} ${artifactName}.jar
  """
}

def preparePackageToUploadPRODS(artifactName){
  sh """
    wget https://artifactory.a.intuit.com/artifactory/maven.Simplan.simplan-releases/com/intuit/data/simplan/spark/${artifactName}/${env.ARTIFACT_VERSION}/${artifactName}-${env.ARTIFACT_VERSION}-jar-with-dependencies.jar -O ${artifactName}.jar
  """
}

def uploadZipToBuckets(bucket_names,artifactName,bucket_role){
  sh """
    aws sts assume-role --role-arn ${bucket_role} --role-session-name TempSession > tempCreds.log
    aws configure set profile.temporary.region us-west-2
    aws configure set profile.temporary.aws_access_key_id `jq '.Credentials.AccessKeyId' tempCreds.log -r`
    aws configure set profile.temporary.aws_secret_access_key `jq '.Credentials.SecretAccessKey' tempCreds.log -r`
    aws configure set profile.temporary.aws_session_token `jq '.Credentials.SessionToken' tempCreds.log -r`
  """

  bucket_names.each { val ->
    sh "aws s3 cp ${artifactName}.jar ${val}/${artifactName}.jar --profile temporary"
  }
}

def adjustPOMValues(config, depPropertiesNames){
  depPropertiesNames.each { properties ->
    sh """
      echo 'NAME OF DEPENDENCY: \${${properties.value}}'
      echo 'VERSION OF DEPENDENCY: ${config[properties.key]}'
      find . -name pom.xml -type f -exec sed -i s+\$\\{${properties.value}\\}+${config[properties.key]}+g {} +
    """
  }
}

def numbersAccordingBranch(){
  if (env.BRANCH_NAME == 'master' || env.BRANCH_NAME == 'develop') return '20'
  else return '5'
}

def mkdocsFunction(){
  withCredentials([usernamePassword(credentialsId: 'github-svc-sbseg-ci', passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME')]) {
    sh """
      git config user.name svc-sbseg-ci
      git config user.email SBSEGCI-Admins@intuit.com
      git config --global url."https://${GIT_USERNAME}:${GIT_PASSWORD}@github.intuit.com".insteadOf https://github.intuit.com
      git config remote.origin.fetch "+refs/heads/*:refs/remotes/origin/*"
      bash gh-deploy.sh
    """
  }
}

def slackCRNotification(slackRepoName, slackCrChannel){
	def message = "<!here>: ${slackRepoName} SNOW CR approval needed: <${env.RUN_DISPLAY_URL}|CLICK HERE>"
	slackSend(channel: slackCrChannel, tokenCredentialId: 'slack-token-curation-tf', message: message)
}
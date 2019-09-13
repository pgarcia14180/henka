properties([[$class: 'GitLabConnectionProperty', gitLabConnection: 'gitlab']])

def app_name = "henka"
def img = "cencoreg:5000/euserx/${app_name}"
def img_sed = 'cencoreg:5000\\/pypi-internal\\/henka'
def gitrepo = 'ssh://git@gitlab.cencosud.corp:29418/pypi-internal/henka.git'


switch(env.BRANCH_NAME) {
    case 'master':
        nspace = 'euserx'
        k_cloud = 'k8scl01'
        k_folder = 'prod'
        iversion = "${env.BUILD_ID}"
        break
    case 'develop':
        nspace = 'euserx'
        k_cloud = 'k8sqacl01'
        k_folder = 'qa'
        iversion = "dev-${env.BUILD_ID}"
        break
    default:
        exit
    }

def image = "${img}:${iversion}"
def image_latest = "${img}:latest"

podTemplate(cloud: k_cloud, label: "${app_name}",
    containers: [
        containerTemplate(name: 'docker', image: 'docker:git', ttyEnabled: true, command: 'cat'),
        containerTemplate(name: 'kubectl', image: 'amaceog/kubectl', ttyEnabled: true, command: 'cat')
    ],
    volumes: [
        hostPathVolume(hostPath: '/var/run/docker.sock', mountPath: '/var/run/docker.sock'),
    ]) {
        node("${app_name}") {
            gitlabCommitStatus(name: 'jenkins') {
                stage('clone repo') {
                    git branch: "${env.BRANCH_NAME}", credentialsId: 'euserx', url: "${gitrepo}"
                    try {
                        tagversion = sh(returnStdout: true, script: "git describe --tags --abbrev=0 --match='v*'").trim()
                    } catch (err) {
                        echo "No git tag version..setting as v0.0.0"
                        tagversion = 'v0.0.0'
                    }
                    commitId = sh(returnStdout: true, script: 'git rev-parse HEAD').trim()
                    image = "${img}:${commitId}"
                    image_tagversion = "${img}:${tagversion}"
                }
                stage('Build Python Package') {
                    container('docker') {
                        //sh "docker build --no-cache -t ${image} ."
                    }
                }
                stage('Push to repository') {	    
                    withCredentials([usernamePassword(credentialsId: 'nexus-registry', passwordVariable: 'DOCKER_PASSWD', usernameVariable: 'DOCKER_USER')]) {
                        container('docker') {
                            //sh "echo ${DOCKER_PASSWD} |docker login -u ${DOCKER_USER} --password-stdin cencoreg:5000"
                        }
                    }
                }
            }
        }
}

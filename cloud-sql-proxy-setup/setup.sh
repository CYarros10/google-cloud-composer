GOOGLE_SA=""
K8_SA=""
GKE_CLUSTER=""
REGION=""
PROJECT_ID=""
K8_SECRET=""
CLOUDSQL_DB=""
CLOUDSQL_USER=""
CLOUDSQL_PASS=""

# CREATE GOOGLE SERVICE ACCOUNT
gcloud iam service-accounts create ${GOOGLE_SA} --display-name ${GOOGLE_SA}

# CREATE IAM POLICY BINDING
gcloud projects add-iam-policy-binding ${PROJECT_ID} --member serviceAccount:${GOOGLE_SA}@${PROJECT_ID}.iam.gserviceaccount.com --role roles/cloudsql.client --condition None

# AUTHENTICATE WITH COMPOSER GKE CLUSTER
gcloud container clusters get-credentials ${GKE_CLUSTER} --region ${REGION}

# CREATE KUBERNETES SERVICE ACCOUNT
kubectl apply -f configs/service_account.yaml

# CREATE KUBERNETES SECRET 
kubectl create secret generic ${K8_SECRET} --from-literal username=${CLOUDSQL_USER} --from-literal password=${CLOUDSQL_PASS} --from-literal database=${CLOUDSQL_DB}

# ENABLE WORKLOAD IDENTITY IN GKE CLUSTER
gcloud container clusters update --region ${REGION} ${GKE_CLUSTER} --workload-pool ${PROJECT_ID}.svc.id.goog

# CREATE GSA-KSA BINDING
gcloud iam service-accounts add-iam-policy-binding --role roles/iam.workloadIdentityUser --member serviceAccount:${PROJECT_ID}.svc.id.goog[default/test-ksa] ${GOOGLE_SA}@${PROJECT_ID}.iam.gserviceaccount.com

# CREATE GKE ANNOTATION FOR KSA
kubectl annotate serviceaccount test-ksa iam.gke.io/gcp-service-account=${GOOGLE_SA}@${PROJECT_ID}.iam.gserviceaccount.com

# APPLY SIDECAR DEPLOYMENT 
kubectl apply -f configs/sidecar_proxy_with_workload_identity.yaml

# APPLY SIDECAR SERVICE
kubectl apply -f configs/sidecar_service.yaml
export BUCKET=<your bucket>

gsutil rm -r "$BUCKET"/dags/demo
gsutil cp -r dags/ "$BUCKET"/dags/demo

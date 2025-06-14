#!/bin/bash

# Set your project ID
PROJECT_ID="your-project-id"
INSTANCE_NAME="news-scraper"
ZONE="us-central1-a"
MACHINE_TYPE="e2-medium"

# Create the instance
gcloud compute instances create $INSTANCE_NAME \
    --project=$PROJECT_ID \
    --zone=$ZONE \
    --machine-type=$MACHINE_TYPE \
    --image-family=ubuntu-2004-lts \
    --image-project=ubuntu-os-cloud \
    --metadata-from-file=startup-script=startup.sh \
    --tags=http-server,https-server

# Create firewall rule to allow HTTP traffic
gcloud compute firewall-rules create allow-http \
    --project=$PROJECT_ID \
    --allow tcp:8080 \
    --target-tags=http-server \
    --description="Allow HTTP traffic"

# Get the external IP
EXTERNAL_IP=$(gcloud compute instances describe $INSTANCE_NAME \
    --project=$PROJECT_ID \
    --zone=$ZONE \
    --format='get(networkInterfaces[0].accessConfigs[0].natIP)')

echo "Instance created with external IP: $EXTERNAL_IP"
echo "Your application will be available at: http://$EXTERNAL_IP:8080" 
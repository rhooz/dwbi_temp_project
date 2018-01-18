#!/usr/bin/env bash
# create the email secret
#kubectl create secret generic i2ap-email --from-literal=email-id=tectonic.api.email@gmail.com --from-literal=email-name="Tectonic LLC" --from-literal=password=t3ct0n1c --namespace=i2ap
kubectl create secret generic i2ap-email --from-literal=email-id=ali.m.kassoff@gettectonic.com --from-literal=email-name="Ali Kassoff" --from-literal=password=T3ct0n1c! --namespace=i2ap
# expose the css file
kubectl create configmap email-css --namespace=i2ap \
   --from-file=email.css="/Users/tyler/Dropbox/Code/Git/analytics/content/email.css"
# and the email template
kubectl create configmap email-html --namespace=i2ap \
   --from-file=email.html="/Users/tyler/Dropbox/Code/Git/analytics/content/email-100001.html"
# and the text
kubectl create configmap email-text --namespace=i2ap \
   --from-file=email.txt="/Users/tyler/Dropbox/Code/Git/analytics/content/email-100001.txt"
# create the mart database secret
kubectl create secret generic i2ap-mart-access --from-literal=database-user=postgres --from-literal=database-password=T3ct0n1c --namespace=i2ap
# create the secret for salesforce access
kubectl create secret generic i2ap-salesforce-access \
--from-literal=salesforce-user=tyler.allbritton@gettectonic.com \
--from-literal=salesforce-password=R0ck1nData \
--from-literal=salesforce-token=birbjs0yOdbRLNpiBmoWz9Kd4 \
--from-literal=salesforce-sandbox=False --namespace=i2ap

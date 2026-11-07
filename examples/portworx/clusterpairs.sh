#!/bin/sh
# One-time: a Stork ClusterPair per direction, in the VM namespace, through an S3 bucket both sites reach.
# Destination tokens: `pxctl cluster token show` on each cluster. See ../../docs/portworx.md
set -e
S3="--provider s3 --s3-endpoint s3.example.com --s3-access-key $S3_KEY --s3-secret-key $S3_SECRET --s3-region us-east-1 --bucket harvey-px-dr"
storkctl create clusterpair harvey-to-site-b -n production --src-kube-file site-a.yaml --dest-kube-file site-b.yaml \
  --dest-ep <site-b-node>:9001 --dest-token "$SITE_B_PX_TOKEN" $S3 --encryption-key "$PX_ENC_KEY" --unidirectional
storkctl create clusterpair harvey-to-site-a -n production --src-kube-file site-b.yaml --dest-kube-file site-a.yaml \
  --dest-ep <site-a-node>:9001 --dest-token "$SITE_A_PX_TOKEN" $S3 --encryption-key "$PX_ENC_KEY" --unidirectional
kubectl --kubeconfig site-a.yaml -n production get clusterpair harvey-to-site-b   # both statuses Ready

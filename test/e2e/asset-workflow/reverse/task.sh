#!/bin/sh
CONTENT=$(cat /data/output.txt)
REVERSED=$(echo "$CONTENT" | rev)
echo "$REVERSED" > /data/output.txt
echo "Reversed content:"
cat /data/output.txt

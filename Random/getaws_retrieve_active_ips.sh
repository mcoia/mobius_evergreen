#!/bin/bash

BEANSTALKTAG="nccard-AppEb-env"
CSSHNAME="nccard"

IPS=`aws ec2 describe-instances --filters "Name=tag:elasticbeanstalk:environment-name,Values=$BEANSTALKTAG"|grep '"PublicIp":'|awk '{print $2}'|sed -e 's/"//' -e 's/",//'|uniq|sed -e ':a;N;$!ba;s/\n/ /g'`

START="$CSSHNAME "
LINE=$START$IPS

echo $LINE
sed -i "/$CSSHNAME/d" .clusterssh/clusters

echo $LINE >> .clusterssh/clusters

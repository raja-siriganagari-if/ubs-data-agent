#!/bin/bash

docker build -t insightfinderinc/ubs-data-agent .

rm ubs-data-agent.tar 2>/dev/null

docker save insightfinderinc/ubs-data-agent -o ubs-data-agent.tar

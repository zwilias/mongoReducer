#!/bin/bash
ssh -f -L 29018:localhost:27017 ilias@facebook.storedesk.com sleep 10;
mongo localhost:29018/tomato $1


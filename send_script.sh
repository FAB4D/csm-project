#!/bin/bash

m[0]='humaniac@humanitas1.cloudapp.net'
m[1]='josephboyd@humanitas2.cloudapp.net'
m[2]='humanitas3@humanitas3.cloudapp.net'
m[3]='humaniac@humanitas4.cloudapp.net'
m[4]='h5@humanitas5.cloudapp.net'
m[5]='humaniac@humanitas6.cloudapp.net'
m[6]='humaniac@humanitas7.cloudapp.net'
#m[7]='fabbrix@humanitas8.cloudapp.net'
loc=':nairobi_tweets'

for REMOTE in "${m[@]}"; do scp get_tweets.py $REMOTE$loc; done

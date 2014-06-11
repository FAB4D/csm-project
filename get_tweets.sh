m[0]='humaniac@humanitas1.cloudapp.net'
m[1]='josephboyd@humanitas2.cloudapp.net'
m[2]='humanitas3@humanitas3.cloudapp.net'
m[3]='humaniac@humanitas4.cloudapp.net'
m[4]='h5@humanitas5.cloudapp.net'
m[5]='humaniac@humanitas6.cloudapp.net'
m[6]='humaniac@humanitas7.cloudapp.net'
m[7]='fabbrix@humanitas8.cloudapp.net'

k[0]='yWCcLQkxhdGCdSl9TCDsTaI64'
k[1]='pdSvpPsuBfT9R21ii6KPqw'
k[2]='vfSVb5hOwkugWkYH8YVWbOZI9'
k[3]='Dwf6lsbI30iWzbSzm'
k[4]='BNyaxYjjvOKJ0jRd6BcHDg9cp'
k[5]='7UgebY9RGJ8JHkHJKPXtjjWzj'

s[0]='VVMLWoJzTDghbLBgCNszQrwKY8Wmt8OwNNjYcC6TFeYEXfxmQ7'
s[1]='X1nyXfoLsYWeMhM01rRgwlskfmFPEo60mq5QUszBwo8'
s[2]='KkHUPK8TH6vacFcXqegbprEulDXmnOJkCOD0ZUrwoATYarRP0I'
s[3]='ZsxZ8Ei441sWV4Yt31so3FOmS647DobrwYZagO2hokUEujzoaO'
s[4]='ysM3ZtXNNt4LBZmMUbya32LQPhSG549aXboIvloGVS4HrfWS0I'
s[5]='wO70AYZMdDikRm2BQ5dwSFERg8Bl1uNBaQhr8dAR7AErslWver'

for (( i=0; i<=5; i++ ))
do
    python="python ~/nairobi_tweets/get_tweets.py ${k[i]} ${s[i]} tweets"
    "ssh" ${m[i]} "nohup" $python "&> errorlog.txt &"
done

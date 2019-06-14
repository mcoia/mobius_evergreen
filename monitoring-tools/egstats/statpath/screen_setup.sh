# SET THIS FOR YOUR SYSTEM
statpath=/mnt/evergreen/statpath

# restore top layout
cp ${statpath}/.toprc_stats ~/.toprc

# start detached screen session which can remember its layout
screen -S node -c ${statpath}/.screenrc_stats

# push interactive commands to top
screen -S node -p 0 -X stuff "o^M"
sleep 1
screen -S node -p 0 -X stuff "%CPU>10.0^M"
sleep 1

# restore screen
screen -r node

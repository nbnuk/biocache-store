#
# script to manage biocache process-local-node
#
# 1. checks if there are checkpoint files and gives option to abort if files are found
# 2. generate log filename from local time
# 3. will run biocache process-local-node and check exit code
# 4. if exit code not 0 then will re run with new log filename
# 5. for use with version of biocache store that exits with 1 when detecting a slow down

_biocache_cmd=biocache
# line below might be handy when running development builds
#_biocache_cmd=target/appassembler/bin/biocache

# see if we have a checkpoint file
# warn user and give them the option of exiting or continuing

if sudo ls /data/tmp/p* 1> /dev/null 2>&1; then
    echo "******** warning ********"
    echo "checkpoint files found at /data/tmp/p*"
    read -p "Are you sure you wish to continue from where processing last left off? (y/n)" -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]
    then
        echo "confirmed processing continuation"
    else
        echo "confirmed, processing abort"
        exit
    fi
else
    echo "no checkpoint files found, starting a new processing run"
fi

# run processing, re running the command until it exits cleanly

for (( ; ; ))
do
_now=`date "+%Y%m%d%H%M%S"`
_log_filename="process_$_now.log"

echo Processing: $_log_filename $$
sudo $_biocache_cmd process-local-node -t 4 > $_log_filename

if [ $? -eq 0 ]
then
    break
fi

done

# give user some info re timings

times


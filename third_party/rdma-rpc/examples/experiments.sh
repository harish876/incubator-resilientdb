#!/bin/bash

define(){ IFS='\n' read -r -d '' ${1} || true; }
redirection=( "> out" "2> err" "< /dev/null" )


WORKDIR="$(cd "$(dirname "$0")" && pwd)"
LOCALWORKDIR="$WORKDIR"
__VERBOSE=2

function log () {
    if [[ $__VERBOSE -ge 1 ]]; then
        echo -e "$@"
    fi
}

function debug () {
    if [[ $__VERBOSE -ge 2 ]]; then
        echo -e "$@"
    fi
}
 
scpFileTo(){
    local server="$1"
    local filename="$2"
    local cmd=( "scp" "$2" "$USER@$server:${WORKDIR}/" )
    debug "\t\tExecuting: ${cmd[@]}"
    $("${cmd[@]}")
}

scpFileFrom(){
    local server="$1"
    local filename="$2"
    local cmd=("scp" "$USER@$server:${WORKDIR}/$2" ./)
    debug "\t\tExecuting: ${cmd[@]}"
    $("${cmd[@]}")
}

sshCommandAsync() {
    local server=$1
    local command=$2
    local valredirect="${redirection[@]}"
    if ! [[ -z $3 ]]
    then
        valredirect="> "$3" 2>/dev/null"
    fi
    local cmd=( "ssh" "-oStrictHostKeyChecking=no" "$USER@$server" "nohup" "$command" "$valredirect" "&" "echo \$!" )
    local pid=$("${cmd[@]}")
    echo "$pid"
}
sshCommandSync() {
    local server="$1"
    local command="$2"
    local valredirect="${redirection[@]}"
    if ! [[ -z $3 ]]
    then
        valredirect="> "$3" 2>/dev/null"
    fi
    local cmd=( "ssh" "-oStrictHostKeyChecking=no" "$USER@$server" "$command" "$valredirect" )
    debug "\t\tExecuting: ${cmd[@]}"
    $("${cmd[@]}")
}

sshKillCommand() {
    local server=$1
    local pid=$2
    cmd=( "ssh" "$USER@$server" "kill -9" "${pid}" )
    debug "\t\tExecuting: ${cmd[@]}"
    $("${cmd[@]}")
}

sshStopCommand() {
    local server=$1
    local pid=$2
    cmd=( "ssh" "$USER@$server" "kill -2" "${pid}" )
    debug "\t\tExecuting: ${cmd[@]}"
    $("${cmd[@]}")
}


remoteip="127.0.0.1"  
localip="127.0.0.1"  



echo "Start first test with ${remoteip} and  ${localip}"
echo "Start latency and BW experiments"


memsize=(1048576 16777216 134217728) # ring buffer size
outstanding=(1 4 8 16 32)
sizes=(4 16 64 256 1024 4096 8192 16384)
exps=(0 1 2 3 4 5 6 11)

for mem in ${memsize[@]}; do
for ot in ${outstanding[@]}; do
for size in ${sizes[@]}; do
for exp in ${exps[@]}; do
continue

if [ "$exp" == "5" ] && [ "$ot" == "32" ] ; then
continue
fi

if [ "$exp" == "6" ] && [ "$ot" == "32" ] ; then
continue
fi

echo "-e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem}"
sleep 1
pid=$(sshCommandAsync ${remoteip} "${WORKDIR}/server -a ${remoteip} -e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem}")
sleep 2
timeout -s 9 60s ${WORKDIR}/client -a ${remoteip} -e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem}
sshKillCommand  ${remoteip} ${pid}

done
done
done
done



# test 8 uses prefetch that is configured with --rsize

exps=(7 8 9 10) # here latency is measured by the server

for mem in ${memsize[@]}; do
for ot in ${outstanding[@]}; do
for size in ${sizes[@]}; do
for exp in ${exps[@]}; do
continue

if [ "$exp" == "8" ] && [ "$ot" == "32" ] ; then
continue
fi

echo "-e ${exp} -s ${size} --rsize=8 -o ${ot} -m ${mem}"

sleep 1

timeout -s 9 60s ${WORKDIR}/server -a ${localip} -e ${exp} -s ${size} --rsize=8 -o ${ot} -m ${mem} &
lpid=$!
sleep 1
rpid=$(sshCommandAsync ${remoteip} "${WORKDIR}/client -a ${localip} -e ${exp} -s ${size} --rsize=8 -o ${ot} -m ${mem}")
wait ${lpid}
sshKillCommand  ${remoteip} ${rpid}

done
done
done
done



echo "DOne with basic experiments"

outstanding=(8 16 32)
echo "Start SGE experiments"
#1 CircularConnectionMailBox 2
#2 CircularConnectionMagicbyte 3
#4 CircularConnectionReverse 3
#5 SharedCircularConnectionMailbox 3

exps=(1) 
sge=2

for mem in ${memsize[@]}; do
for ot in ${outstanding[@]}; do
for size in ${sizes[@]}; do
for exp in ${exps[@]}; do
continue

if [ "$exp" == "5" ] && [ "$ot" == "32" ] ; then
continue
fi

if [ "$exp" == "6" ] && [ "$ot" == "32" ] ; then
continue
fi
sleep 1
echo "-e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem} --send-sges=${sge}"

pid=$(sshCommandAsync ${remoteip} "${WORKDIR}/server -a ${remoteip} -e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem}")
sleep 2
timeout -s 9 60s ${WORKDIR}/client -a ${remoteip} -e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem}
sshKillCommand  ${remoteip} ${pid}

done
#exit 1
done
done
done

sge=3
exps=(2 4 5) 
for mem in ${memsize[@]}; do
for ot in ${outstanding[@]}; do
for size in ${sizes[@]}; do
for exp in ${exps[@]}; do
continue

if [ "$exp" == "5" ] && [ "$ot" == "32" ] ; then
continue
fi

if [ "$exp" == "6" ] && [ "$ot" == "32" ] ; then
continue
fi
sleep 1
echo "-e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem} --send-sges=${sge}"

pid=$(sshCommandAsync ${remoteip} "${WORKDIR}/server -a ${remoteip} -e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem}")
sleep 2
timeout -s 9 60s ${WORKDIR}/client -a ${remoteip} -e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem}
sshKillCommand  ${remoteip} ${pid}

done
#exit 1
done
done
done
echo "done  SGE experiments"



echo "Start prefetch test for exp 8"

# test 8 uses prefetch that is configured with --rsize

exp=8 # here latency is measured by the server
prefs=(16 32 128 512 1024 4096 8192 16384)
sizes=(8 24 120 504 1016 4088 8184 16376)

for mem in ${memsize[@]}; do
for ot in ${outstanding[@]}; do
for size in ${sizes[@]}; do
for pref in ${prefs[@]}; do
continue

if [ "$exp" == "8" ] && [ "$ot" == "32" ] ; then
continue
fi

echo "-e ${exp} -s ${size} --rsize=${pref} -o ${ot} -m ${mem}"

sleep 1

timeout -s 9 60s ${WORKDIR}/server -a ${localip} -e ${exp} -s ${size} --rsize=${pref} -o ${ot} -m ${mem} &
lpid=$!
sleep 1
rpid=$(sshCommandAsync ${remoteip} "${WORKDIR}/client -a ${localip} -e ${exp} -s ${size} --rsize=${pref} -o ${ot} -m ${mem}")
wait ${lpid}
sshKillCommand  ${remoteip} ${rpid}

done
#exit 1
done
done
done

echo "Done prefetch experiment"



echo "Start device memory tests"
# for 5 6 8 9 10

memsize=(1048576 16777216 134217728) # ring buffer size
outstanding=(8 16 32)
sizes=(4 16 64 256 1024 4096 8192 16384)
exps=(5 6)

for mem in ${memsize[@]}; do
for ot in ${outstanding[@]}; do
for size in ${sizes[@]}; do
for exp in ${exps[@]}; do
continue

if [ "$exp" == "5" ] && [ "$ot" == "32" ] ; then
continue
fi

if [ "$exp" == "6" ] && [ "$ot" == "32" ] ; then
continue
fi
sleep 1
echo "-e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem} --with-dm-atomics"

pid=$(sshCommandAsync ${remoteip} "${WORKDIR}/server -a ${remoteip} -e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem} --with-dm-atomics")
sleep 2
timeout -s 9 60s ${WORKDIR}/client -a ${remoteip} -e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem}
sshKillCommand  ${remoteip} ${pid}

sleep 1
echo "-e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem} --with-dm-reads"

pid=$(sshCommandAsync ${remoteip} "${WORKDIR}/server -a ${remoteip} -e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem} --with-dm-reads")
sleep 2
timeout -s 9 60s ${WORKDIR}/client -a ${remoteip} -e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem}
sshKillCommand  ${remoteip} ${pid}

sleep 1
echo "-e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem} --with-dm-atomics --with-dm-reads"

pid=$(sshCommandAsync ${remoteip} "${WORKDIR}/server -a ${remoteip} -e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem} --with-dm-atomics --with-dm-reads")
sleep 2
timeout -s 9 60s ${WORKDIR}/client -a ${remoteip} -e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem}
sshKillCommand  ${remoteip} ${pid}

#exit 1

done
done
done
done


 
exps=(8 9 10) # here latency is measured by the server

for mem in ${memsize[@]}; do
for ot in ${outstanding[@]}; do
for size in ${sizes[@]}; do
for exp in ${exps[@]}; do
continue
if [ "$exp" == "8" ] && [ "$ot" == "32" ] ; then
continue
fi

echo "-e ${exp} -s ${size} --rsize=8 -o ${ot} -m ${mem} --with-dm-reads"
sleep 1
timeout -s 9 60s ${WORKDIR}/server -a ${localip} -e ${exp} -s ${size} --rsize=8 -o ${ot} -m ${mem} --with-dm-reads &
lpid=$!
sleep 1
rpid=$(sshCommandAsync ${remoteip} "${WORKDIR}/client -a ${localip} -e ${exp} -s ${size} --rsize=8 -o ${ot} -m ${mem}")
wait ${lpid}
sshKillCommand  ${remoteip} ${rpid}

#exit 1
done
done
done
done



echo "Start prefetch test for exp 8 and  --with-dm-reads"

# test 8 uses prefetch that is configured with --rsize

exp=8 # here latency is measured by the server
prefs=(16 32 128 512 1024 4096 8192 16384)
sizes=(8 24 120 504 1016 4088 8184 16376)
outstanding=(8 16 32)

for mem in ${memsize[@]}; do
for ot in ${outstanding[@]}; do
for size in ${sizes[@]}; do
for pref in ${prefs[@]}; do
continue

if [ "$exp" == "8" ] && [ "$ot" == "32" ] ; then
continue
fi

echo "-e ${exp} -s ${size} --rsize=${pref} -o ${ot} -m ${mem} --with-dm-reads"

sleep 1

timeout -s 9 60s ${WORKDIR}/server -a ${localip} -e ${exp} -s ${size} --rsize=${pref} -o ${ot} -m ${mem} --with-dm-reads &
lpid=$!
sleep 1
rpid=$(sshCommandAsync ${remoteip} "${WORKDIR}/client -a ${localip} -e ${exp} -s ${size} --rsize=${pref} -o ${ot} -m ${mem}")
wait ${lpid}
sshKillCommand  ${remoteip} ${rpid}

done
done
done
done

echo "done all DM tests"



echo "start with copy requirement"
memsize=(1048576 16777216 134217728) # ring buffer size
outstanding=(8 16 32)
sizes=(4 16 64 256 1024 4096 8192 16384)
exps=(0 1 2 3 4 5 6 11)

for mem in ${memsize[@]}; do
for ot in ${outstanding[@]}; do
for size in ${sizes[@]}; do
for exp in ${exps[@]}; do
continue

if [ "$exp" == "5" ] && [ "$ot" == "32" ] ; then
continue
fi

if [ "$exp" == "6" ] && [ "$ot" == "32" ] ; then
continue
fi

sleep 1
echo "-e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem} --with-copy=1262485504"

pid=$(sshCommandAsync ${remoteip} "${WORKDIR}/server -a ${remoteip} -e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem} --with-copy=1262485504")
sleep 2
timeout -s 9 60s ${WORKDIR}/client -a ${remoteip} -e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem} --with-copy=1262485504
sshKillCommand  ${remoteip} ${pid}


done
done
done
done

echo "done with copy"


echo "start shared tests N:1"
# exp 5 6 11 for N:1


nums=(2 4 8 16)
memsize=(1048576 16777216 134217728) # ring buffer size
outstanding=(1 4 16)
sizes=(4 16 64 256 1024 4096 8192 16384)
exps=(5 6)

for num in ${nums[@]}; do
for mem in ${memsize[@]}; do
for ot in ${outstanding[@]}; do
for size in ${sizes[@]}; do
for exp in ${exps[@]}; do
continue 

sleep 1
echo "-e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem} --clients=${num}"
pid=$(sshCommandAsync ${remoteip} "${WORKDIR}/server -a ${remoteip} -e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem} --clients=${num}")
sleep 2


for i in $(seq 1 $num)
do
sleep 0.3
timeout -s 9 60s ${WORKDIR}/client -a ${remoteip} -e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem} &
done
wait $!

sshKillCommand  ${remoteip} ${pid}

################

sleep 1
echo "-e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem} --with-dm-atomics --clients=${num}"

pid=$(sshCommandAsync ${remoteip} "${WORKDIR}/server -a ${remoteip} -e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem} --with-dm-atomics --clients=${num}")
sleep 2

for i in $(seq 1 $num)
do
sleep 0.3
timeout -s 9 60s ${WORKDIR}/client -a ${remoteip} -e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem} &
done
wait $!

sshKillCommand  ${remoteip} ${pid}

#####################

sleep 1
echo "-e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem} --with-dm-reads --clients=${num}"

pid=$(sshCommandAsync ${remoteip} "${WORKDIR}/server -a ${remoteip} -e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem} --with-dm-reads --clients=${num}")
sleep 2

for i in $(seq 1 $num)
do
sleep 0.3
timeout -s 9 60s ${WORKDIR}/client -a ${remoteip} -e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem} &
done
wait $!

sshKillCommand  ${remoteip} ${pid}

#####################

sleep 1
echo "-e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem} --with-dm-atomics --with-dm-reads --clients=${num}"

pid=$(sshCommandAsync ${remoteip} "${WORKDIR}/server -a ${remoteip} -e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem} --with-dm-atomics --with-dm-reads --clients=${num}")
sleep 2

for i in $(seq 1 $num)
do
sleep 0.3
timeout -s 9 60s ${WORKDIR}/client -a ${remoteip} -e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem} &
done
wait $!

sshKillCommand  ${remoteip} ${pid}

done
done
done
done
done


nums=(1 2 4 8 16)
exps=(11)
for num in ${nums[@]}; do
for mem in ${memsize[@]}; do
for ot in ${outstanding[@]}; do
for size in ${sizes[@]}; do
for exp in ${exps[@]}; do
continue 

sleep 1
echo "-e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem} --clients=${num} --with-srq"
pid=$(sshCommandAsync ${remoteip} "${WORKDIR}/server -a ${remoteip} -e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem} --with-srq --clients=${num}")
sleep 2


for i in $(seq 1 $num)
do
sleep 0.3
timeout -s 9 60s ${WORKDIR}/client -a ${remoteip} -e ${exp} -s ${size} --rsize=${size} -o ${ot} -m ${mem} &
done
wait $!

sshKillCommand  ${remoteip} ${pid}

done
done
done
done
done



echo "start shared tests 1:N"
# exp 7 8 9 10 for 1:N

# test 8 uses prefetch that is configured with --rsize


 
nums=(2 4 8 16)
exps=(7 8 9 10) # here latency is measured by the server

for num in ${nums[@]}; do
for mem in ${memsize[@]}; do
for ot in ${outstanding[@]}; do
for size in ${sizes[@]}; do
for exp in ${exps[@]}; do
continue

if [ "$exp" == "8" ] && [ "$ot" == "32" ] ; then
continue
fi

echo "-e ${exp} -s ${size} --rsize=8 -o ${ot} -m ${mem} --clients=${num}"

sleep 1

timeout -s 9 60s ${WORKDIR}/server -a ${localip} -e ${exp} -s ${size} --rsize=8 -o ${ot} -m ${mem} --clients=${num} &
lpid=$!
 
pids=()
for i in $(seq 1 $num)
do
sleep 0.3
rpid=$(sshCommandAsync ${remoteip} "${WORKDIR}/client -a ${localip} -e ${exp} -s ${size} --rsize=8 -o ${ot} -m ${mem}")
pids+=($rpid)
done

wait ${lpid}

for rpid in "${pids[@]}"
do
sshKillCommand ${remoteip} ${rpid}
done


exit 1

done
done
done
done
done



exps=(8 9 10)
for num in ${nums[@]}; do
for mem in ${memsize[@]}; do
for ot in ${outstanding[@]}; do
for size in ${sizes[@]}; do
for exp in ${exps[@]}; do
continue 

if [ "$exp" == "8" ] && [ "$ot" == "32" ] ; then
continue
fi

echo "-e ${exp} -s ${size} --rsize=8 -o ${ot} -m ${mem} --clients=${num}  --with-dm-reads"

sleep 1

timeout -s 9 60s ${WORKDIR}/server -a ${localip} -e ${exp} -s ${size} --rsize=8 -o ${ot} -m ${mem} --clients=${num}  --with-dm-reads &
lpid=$!
 
pids=()
for i in $(seq 1 $num)
do
sleep 0.3
rpid=$(sshCommandAsync ${remoteip} "${WORKDIR}/client -a ${localip} -e ${exp} -s ${size} --rsize=8 -o ${ot} -m ${mem}")
pids+=($rpid)
done

wait ${lpid}

for rpid in "${pids[@]}"
do
sshKillCommand ${remoteip} ${rpid}
done
 


done
done
done
done
done

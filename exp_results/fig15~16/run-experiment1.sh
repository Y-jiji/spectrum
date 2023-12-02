# Set some variables
step=10
start=80
limit=140
logf=experiment-log
logv=verbose-log
logb=build-log
if [ ! -n "$1" ]; then keys=1000000; else keys=$1; fi
if [ ! -n "$2" ]; then threads=36; else threads=$2; fi
if [ ! -n "$3" ]; then contract_type=11; else contract_type=$3; fi
if [ ! -n "$4" ]; then synthetic=false; else synthetic=$4; fi
if [ ! -n "$5" ]; then two_partitions=true; else two_partitions=$5; fi
if [ ! -n "$6" ]; then cold_record_ratio=0; else cold_record_ratio=$6; fi
if [ ! -n "$7" ]; then cold_record_time=0; else cold_record_time=$7; fi
if [ ! -n "$8" ]; then sparkle_lock_div=3; else sparkle_lock_div=$8; fi
time_to_init=30
time_to_run=5
shrink_window_size=40
initial_window_size=40
batch_size=100
two_partitions="false"
execute='../spectrum/build/dcc_bench'
timestamp=$(date +"%Y-%m-%d-%H-%M")
current=$(pwd)

# Set log file names
logf="$logf-$timestamp"
logv="$logv-$timestamp"
echo "" >> $logb

for l in $logf $logv
do
	echo "" > $l
	echo "zipf is changing" >> $l
	echo "keys=$keys" >> $l
	echo "threads=$threads" >> $l
	echo "synthetic=$synthetic" >> $l
	echo "two_partitions=$two_partitions" >> $l
	echo "contract=$contract_type" >> $l
done

# Switch to the spectrum directory and compile dcc_bench
cd ../spectrum
git checkout -f with-partial-sched
cd third_party/evmone
git checkout -f with-partial
cd ../..
rm -rf build-with-partial-sched
mkdir build-with-partial-sched      
echo "cmake build compile"
cmake -S . -B build-with-partial-sched >& $logb
cmake --build build-with-partial-sched -- -j16 >& $logb
cd $current

# Set the path of dcc_bench
execute='../spectrum/build-with-partial-sched/dcc_bench'

# Run the experiment of Spectrum protocol (without pre-scheduling)
i=$start
while [ $i -lt $limit ]
do
	zipf=$(python3 -c "print($i / 100 + 0.00001)")
	echo "@ spectrum-p; zipf=$zipf" >> $logf
	echo "@ spectrum-p; zipf=$zipf" >> $logv
	echo "@ spectrum-p; zipf=$zipf"
	$execute \
                --contract_type=$contract_type \
                --protocol=Sparkle \
                --threads=$threads \
                --keys=$keys \
		--zipf=$zipf \
		--batch_size=$batch_size \
		--synthetic=$synthetic \
		--two_partitions=$two_partitions \
		--cold_record_ratio=$cold_record_ratio \
		--time_to_run=$time_to_run >& tmp &
	sleep $(python3 -c "print($time_to_run + $time_to_init)")
	kill -9 $(pgrep dcc_bench)
	cat tmp >> $logv
	cat tmp | grep -o "average commit.*" >> $logf
	cat tmp | grep -o "\] commit: .*" >> $logf
	cat tmp | grep -o "average commit.*"
	cat tmp | grep -o "\] commit: .*"
	i=$(($i+$step))
done

# Switch to the spectrum directory and compile dcc_bench
cd ../spectrum
git checkout -f with-partial-unblocked-pre-sched-v2-resched
cd third_party/evmone
git checkout -f with-partial
cd ../..
rm -rf build-with-partial-unblocked-pre-sched-v2-resched
mkdir build-with-partial-unblocked-pre-sched-v2-resched    
echo "cmake build compile"
cmake -S . -B build-with-partial-unblocked-pre-sched-v2-resched >& $logb
cmake --build build-with-partial-unblocked-pre-sched-v2-resched -- -j16 >& $logb
cd $current

# Set the path of dcc_bench
execute='../spectrum/build-with-partial-unblocked-pre-sched-v2-resched/dcc_bench'

# Run the experiment of Spectrum protocol (with pre-scheduling)
i=$start
while [ $i -lt $limit ]
do
	lock_manager=$(python3 -c "print($threads // $sparkle_lock_div)")
	subkeys=$(python3 -c "print(int($keys / $lock_manager))")

	zipf=$(python3 -c "print($i / 100 + 0.00001)")
	echo "@ spectrum-pp; zipf=$zipf" >> $logf
	echo "@ spectrum-pp; zipf=$zipf" >> $logv
	echo "@ spectrum-pp; zipf=$zipf"
	$execute \
                --contract_type=$contract_type \
                --protocol=Sparkle \
                --threads=$(python3 -c "print($threads + $lock_manager)") \
                --keys=$subkeys \
		--zipf=$zipf \
		--batch_size=$batch_size \
		--synthetic=$synthetic \
		--sparkle_lock_manager=$lock_manager \
		--partition_num=$lock_manager \
		--global_key_space=false \
		--sche_only_hotspots=true \
		--pre_sched_num=10 \
		--re_sched_num=20 \
		--time_to_run=$time_to_run >& tmp &
	sleep $(python3 -c "print($time_to_run + $time_to_init)")
	kill -9 $(pgrep dcc_bench)
	cat tmp >> $logv
	cat tmp | grep -o "average commit.*" >> $logf
	cat tmp | grep -o "\] commit: .*" >> $logf
	cat tmp | grep -o "average commit.*"
	cat tmp | grep -o "\] commit: .*"
	i=$(($i+$step))
done

# Switch to the no-partial-revised branch and compile dcc_bench
cd ../spectrum
git checkout -f no-partial-revised
cd third_party/evmone
git checkout -f no-partial
cd ../..
rm -rf build-no-partial-revised
mkdir build-no-partial-revised
echo "cmake build compile"
cmake -S . -B build-no-partial-revised >& $logb
cmake --build build-no-partial-revised -- -j16 >& $logb
cd $current

# Set the path of dcc_bench
execute='../spectrum/build-no-partial-revised/dcc_bench'

# Run the experiment of AriaFB protocol
i=$start
while [ $i -lt $limit ]
do
	lock_manager=$(python3 -c "print(2)")
	subkeys=$(python3 -c "print(int($keys / $lock_manager))")
	
	zipf=$(python3 -c "print($i / 100 + 0.00001)")
	echo "@ aria fb; zipf=$zipf" >> $logf
	echo "@ aria fb; zipf=$zipf" >> $logv
	echo "@ aria fb; zipf=$zipf"
	$execute \
                --contract_type=$contract_type \
                --protocol=AriaFB \
                --threads=$threads \
                --keys=$subkeys \
		--zipf=$zipf \
		--batch_size=$batch_size \
		--synthetic=$synthetic \
		--two_partitions=$two_partitions \
		--ariaFB_lock_manager=$lock_manager \
		--partition_num=$lock_manager \
		--global_key_space=false \
		--time_to_run=$time_to_run >& tmp &
	sleep $(python3 -c "print($time_to_run + $time_to_init)")
	kill -9 $(pgrep dcc_bench)
	cat tmp >> $logv
	cat tmp | grep -o "average commit.*" >> $logf
	cat tmp | grep -o "\] commit: .*" >> $logf
	cat tmp | grep -o "average commit.*"
	cat tmp | grep -o "\] commit: .*"
	i=$(($i+$step))
done

# Run the experiment of the original Sparkle protocol
i=$start
while [ $i -lt $limit ]
do
	zipf=$(python3 -c "print($i / 100 + 0.00001)")
	echo "@ sparkle; zipf=$zipf" >> $logf
	echo "@ sparkle; zipf=$zipf" >> $logv
	echo "@ sparkle; zipf=$zipf"
	$execute \
                --contract_type=$contract_type \
                --protocol=Sparkle \
                --threads=$threads \
                --keys=$keys \
		--zipf=$zipf \
		--batch_size=$batch_size \
		--synthetic=$synthetic \
		--two_partitions=$two_partitions \
		--time_to_run=$time_to_run >& tmp &
	sleep $(python3 -c "print($time_to_run + $time_to_init)")
	kill -9 $(pgrep dcc_bench)
	cat tmp >> $logv
	cat tmp | grep -o "average commit.*" >> $logf
	cat tmp | grep -o "\] commit: .*" >> $logf
	cat tmp | grep -o "average commit.*"
	cat tmp | grep -o "\] commit: .*"
	i=$(($i+$step))
done
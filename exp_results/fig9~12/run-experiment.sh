# Set some variables
step=6
start=6
limit=37
logf=experiment-log
logv=verbose-log
logb=build-log
if [ ! -n "$1" ]; then keys=1000000; else keys=$1; fi
if [ ! -n "$2" ]; then zipf=1.1; else zipf=$2; fi
if [ ! -n "$3" ]; then contract_type=11; else contract_type=$3; fi
if [ ! -n "$4" ]; then synthetic=false; else synthetic=$4; fi
if [ ! -n "$5" ]; then two_partitions=true; else two_partitions=$5; fi
if [ ! -n "$6" ]; then cold_record_ratio=0; else cold_record_ratio=$6; fi
if [ ! -n "$7" ]; then cold_record_time=0; else cold_record_time=$7; fi
time_to_init=30
time_to_run=5
initial_window_size=40
shrink_window_size=8
execute='../spectrum/build/dcc_bench'
timestamp=$(date +"%Y-%m-%d-%H-%M")
current=$(pwd)
batch_size=100

# Set the log file names
logf="$logf-$timestamp"
logv="$logv-$timestamp"
echo "" >> $logb

# Create log files and write some information
for l in $logf $logv
do
	echo "" > $l
	echo "zipf=$zipf" >> $l
	echo "keys=$keys" >> $l
	echo "synthetic=$synthetic" >> $l
	echo "two_partitions=$two_partitions" >> $l
	echo "contract=$contract_type" >> $l
	echo "cold_record_ratio=$cold_record_ratio" >> $l
	echo "cold_record_time=$cold_record_time" >> $l
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

# Run the experiment with Serial protocol
echo "@ serial; threads=1" >> $logf
echo "@ serial; threads=1" >> $logv
echo "@ serial; threads=1"
	$execute \
			--contract_type=$contract_type \
			--protocol=Serial \
			--keys=$keys \
		--zipf=$zipf \
		--synthetic=$synthetic \
		--two_partitions=$two_partitions \
		--cold_record_ratio=$cold_record_ratio \
		--cold_record_time=$cold_record_time \
		--time_to_run=$time_to_run >& tmp &
sleep $(python3 -c "print($time_to_run + $time_to_init)")
kill -9 $(pgrep dcc_bench)
cat tmp >> $logv
cat tmp | grep -o "average commit.*" >> $logf
cat tmp | grep -o "average commit.*"


# Run the experiment with Spectrum protocol
i=$start
while [ $i -lt $limit ]
do
	echo "@ spectrum; threads=$i" >> $logf
	echo "@ spectrum; threads=$i" >> $logv
	echo "@ spectrum; threads=$i"
	$execute \
                --contract_type=$contract_type \
                --protocol=Spectrum \
                --threads=$i \
                --keys=$keys \
		--zipf=$zipf \
		--batch_size=$batch_size \
		--synthetic=$synthetic \
		--two_partitions=$two_partitions \
		--cold_record_ratio=$cold_record_ratio \
		--cold_record_time=$cold_record_time \
		--time_to_run=$time_to_run >& tmp &
	sleep $(python3 -c "print($time_to_run + $time_to_init)")
	kill -9 $(pgrep dcc_bench)
	cat tmp >> $logv
	cat tmp | grep -o "average commit.*" >> $logf
	cat tmp | grep -o "average commit.*"
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

# Run the experiment with AriaFB protocol
i=$start
while [ $i -lt $limit ]
do
	lock_manager=$(python3 -c "print(2)")
	subkeys=$(python3 -c "print(int($keys / $lock_manager))")
	
	echo "@ aria fb; threads=$i" >> $logf
	echo "@ aria fb; threads=$i" >> $logv
	echo "@ aria fb; threads=$i"
	$execute \
                --contract_type=$contract_type \
                --protocol=AriaFB \
                --threads=$i \
                --keys=$subkeys \
		--zipf=$zipf \
		--batch_size=$batch_size \
		--synthetic=$synthetic \
		--two_partitions=$two_partitions \
		--cold_record_ratio=$cold_record_ratio \
		--cold_record_time=$cold_record_time \
		--ariaFB_lock_manager=$lock_manager \
		--partition_num=$lock_manager \
		--global_key_space=false \
		--time_to_run=$time_to_run >& tmp &
	sleep $(python3 -c "print($time_to_run + $time_to_init)")
	kill -9 $(pgrep dcc_bench)
	cat tmp >> $logv
	cat tmp | grep -o "average commit.*" >> $logf
	cat tmp | grep -o "average commit.*"
	i=$(($i+$step))
done

# Run the experiment with original Sparkle protocol
i=$start
while [ $i -lt $limit ]
do
	echo "@ sparkle; threads=$i" >> $logf
	echo "@ sparkle; threads=$i" >> $logv
	echo "@ sparkle; threads=$i"
	$execute \
                --contract_type=$contract_type \
                --protocol=Sparkle \
                --threads=$i \
                --keys=$keys \
		--zipf=$zipf \
		--batch_size=$batch_size \
		--synthetic=$synthetic \
		--time_to_run=$time_to_run >& tmp &
	sleep $(python3 -c "print($time_to_run + $time_to_init)")
	kill -9 $(pgrep dcc_bench)
	cat tmp >> $logv
	cat tmp | grep -o "average commit.*" >> $logf
	cat tmp | grep -o "average commit.*"
	i=$(($i+$step))
done

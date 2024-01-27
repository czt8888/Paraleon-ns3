# Paraleon NS-3 simulator
This is an NS-3 simulator based on [HPCC: High Precision Congestion Control (SIGCOMM' 2019)](https://github.com/alibaba-edu/High-Precision-Congestion-Control).

It is based on NS-3 version 3.17.

## Quick Start

### Build
`./waf configure`

Please note if gcc version > 5, compilation will fail due to some ns3 code style.  If this what you encounter, please use:

`CC='gcc-5' CXX='g++-5' ./waf configure`

### Experiment config
Please see `mix/config.txt` for example. 

`mix/config_doc.txt` is a explanation of the example (texts in {..} are explanations).

### Run
The direct command to run is:
`./waf --run 'scratch/third mix/config.txt'`

To start tuning, run:
`python tuning.py`
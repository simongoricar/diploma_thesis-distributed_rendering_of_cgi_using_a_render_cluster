#!/bin/bash
set -e
squeue --me -o "%.18i %.9P | %.40j %.8u %.2t | %.11M %.11l %.20S | %.4C %.9f %.23E %R" --sort=S

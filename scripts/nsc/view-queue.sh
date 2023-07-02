#!/bin/bash
squeue --me -o "%.18i %.9P %.56j %.8u %.2t %.10M %24S %44Y %R" --sort=S

#!/usr/bin/env bash

echo "Pulling linuxserver/blender:3.6.0 from Docker Hub."
singularity pull blender-3.6.0.sif docker://linuxserver/blender:3.6.0
